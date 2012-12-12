import re
import uuid
import json

from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
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

from vumi.transports.smpp import SmppTransport
from vumi.transports.smpp.clientserver.client import EsmeTransceiver, EsmeTransceiverFactory
from vumi.middleware import MiddlewareStack, setup_middlewares_from_config

class PushTzSmppTransport(SmppTransport):
    
    regex_plus = re.compile("^\+")
    
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

    @inlineCallbacks    
    def setup_middleware(self):
        middlewares = yield setup_middlewares_from_config(self, self.config)
        self._middlewares = CustomMiddlewareStack(middlewares)


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


class CustomMiddlewareStack(MiddlewareStack):

    @inlineCallbacks
    def _handle(self, middlewares, handler_name, message, endpoint):
        method_name = 'handle_%s' % (handler_name,)
        for middleware in middlewares:
            handler = getattr(middleware, method_name)
            message = yield handler(message, endpoint)
            if message is None:
                continue
        returnValue(message)    