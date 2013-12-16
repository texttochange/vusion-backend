import re
import uuid
import json

from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred
from smpp.pdu_builder import (BindTransceiver,
                                BindTransmitter,
                                BindReceiver,
                                DeliverSMResp,
                                SubmitSM,
                                SubmitMulti,
                                EnquireLink,
                                EnquireLinkResp,
                                QuerySM,
                                )
from smpp.pdu_inspector import (MultipartMessage,
                                detect_multipart,
                                multipart_key,
                                )
from vumi.message import TransportUserMessage
from vumi.transports.smpp import SmppTransport
from vumi.transports.smpp.clientserver.client import EsmeTransceiver, EsmeTransceiverFactory

from middlewares.custom_middleware_stack import CustomMiddlewareStack, useCustomMiddleware


@useCustomMiddleware
class PushTzSmppTransport(SmppTransport):
    
    regex_plus = re.compile("^\+")
    
    DELIVERY_REPORT_STATUS_MAPPING = {
        # Output values should map to themselves:
        'delivered': 'delivered',
        'failed': 'failed',
        'pending': 'pending',
        # SMPP `message_state` values:
        'ENROUTE': 'pending',
        'DELIVERED': 'delivered',
        'EXPIRED': 'failed',
        'DELETED': 'failed',
        'UNDELIVERABLE': 'failed',
        'ACCEPTED': 'delivered',
        'UNKNOWN': 'pending',
        'REJECTED': 'failed',
        # From the most common regex-extracted format:
        'DELIVRD': 'delivered',
        'REJECTD': 'failed',
        'UNDELIV': 'failed'}    

    def delivery_status(self, stat):
        return self.DELIVERY_REPORT_STATUS_MAPPING.get(stat, 'pending')
    
    def make_factory(self):
        return PushTzEsmeTransceiverFactory(self.client_config,
                                            self.r_server,
                                            self.esme_callbacks)    
    
    def handle_outbound_message(self, message):
        message['from_addr'] = message['from_addr'] if 'default_origin' not in self.config else self.config['default_origin']
        super(PushTzSmppTransport, self).handle_outbound_message(message)

    def deliver_sm(self, *args, **kwargs):
        if (not re.match(self.regex_plus, kwargs.get('source_addr'))):
            kwargs.update({'source_addr': ('+%s' % kwargs.get('source_addr'))})        
        super(PushTzSmppTransport, self).deliver_sm(*args, **kwargs)


class PushTzEsmeTransceiverFactory(EsmeTransceiverFactory):
    
    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = PushTzEsmeTransceiver(self.config, self.kvs, self.esme_callbacks)
        self.resetDelay()
        return self.esme


class PushTzEsmeTransceiver(EsmeTransceiver):
    
    def detect_error_delivery(self, pdu):
        if ('optional_parameters' in pdu['body']):
            for optional_parameter in pdu['body']['optional_parameters']:
                if optional_parameter['tag'] == 'network_error_code':
                    log.msg('NETWORK ERROR CODE %s' % optional_parameter['value'])
                    return True
        return False
    
    def handle_deliver_sm(self, pdu):
        #if ('optional_parameters' in pdu['body']):
            #for optional_parameter in pdu['body']['optional_parameters']:
                #if optional_parameter['tag'] == 'network_error_code':
                    #log.msg('NETWORK ERROR CODE %s' % optional_parameter['value'])
                    ##return
        #EsmeTransceiver.handle_deliver_sm(self, pdu)
        if self.state not in ['BOUND_RX', 'BOUND_TRX']:
            log.err('WARNING: Received deliver_sm in wrong state: %s' % (
                self.state))

        if pdu['header']['command_status'] == 'ESME_ROK':
            sequence_number = pdu['header']['sequence_number']
            message_id = str(uuid.uuid4())
            pdu_resp = DeliverSMResp(sequence_number,
                    **self.defaults)
            self.send_pdu(pdu_resp)
            pdu_params = pdu['body']['mandatory_parameters']
            delivery_report = self.config.delivery_report_re.search(
                    pdu_params['short_message'] or ''
                    )
            if delivery_report:
                self.esme_callbacks.delivery_report(
                        destination_addr=pdu_params['destination_addr'],
                        source_addr=pdu_params['source_addr'],
                        delivery_report=delivery_report.groupdict(),
                        )
            elif detect_multipart(pdu):
                redis_key = "%s#multi_%s" % (
                        self.r_prefix, multipart_key(detect_multipart(pdu)))
                log.msg("Redis multipart key: %s" % (redis_key))
                value = json.loads(self.r_server.get(redis_key) or 'null')
                log.msg("Retrieved value: %s" % (repr(value)))
                multi = MultipartMessage(value)
                multi.add_pdu(pdu)
                completed = multi.get_completed()
                if completed:
                    self.r_server.delete(redis_key)
                    log.msg("Reassembled Message: %s" % (completed['message']))
                    # and we can finally pass the whole message on
                    self.esme_callbacks.deliver_sm(
                            destination_addr=completed['to_msisdn'],
                            source_addr=completed['from_msisdn'],
                            short_message=completed['message'],
                            message_id=message_id,
                            )
                else:
                    self.r_server.set(redis_key, json.dumps(multi.get_array()))
            elif self.detect_error_delivery(pdu):
                return
            else:
                decoded_msg = self._decode_message(pdu_params['short_message'],
                                                   pdu_params['data_coding'])
                self.esme_callbacks.deliver_sm(
                        destination_addr=pdu_params['destination_addr'],
                        source_addr=pdu_params['source_addr'],
                        short_message=decoded_msg,
                        message_id=message_id,
                        )
    
    def submit_sm(self, **kwargs):
        if self.state not in ['BOUND_TX', 'BOUND_TRX']:
            log.err(('WARNING: submit_sm in wrong state: %s, '
                            'dropping message: %s' % (self.state, kwargs)))
            return 0
        else:
            sequence_number = self.get_seq()
            pdu = SubmitSM(sequence_number, **dict(self.defaults, **kwargs))
            self.get_next_seq()

            message = kwargs['short_message']
            if len(message) > 254:
                pdu.add_message_payload(''.join('%02x' % ord(c) for c in message))
    
            self.send_pdu(pdu)
            self.push_unacked(sequence_number)
            return sequence_number
