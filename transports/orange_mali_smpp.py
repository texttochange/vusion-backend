import re
import uuid
import json

from smpp.pdu_builder import DeliverSMResp
from smpp.pdu_inspector import (MultipartMessage, detect_multipart,
                                multipart_key)

from vumi.log import log
from vumi.transports.smpp import SmppTransport
from vumi.transports.smpp.clientserver.client import EsmeTransceiverFactory

from transports.enhanced_smpp.enhanced_client import EnhancedEsmeTransceiver

from middlewares.custom_middleware_stack import useCustomMiddleware


@useCustomMiddleware
class OrangeMaliSmppTransport(SmppTransport):
    
    def make_factory(self):
        return OrangeMaliEsmeTransceiverFactory(
            self.client_config,
            self.r_server,
            self.esme_callbacks)


class OrangeMaliEsmeTransceiverFactory(EsmeTransceiverFactory):
    
    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = OrangeMaliEsmeTransceiver(self.config, self.kvs, self.esme_callbacks)
        self.resetDelay()
        return self.esme


class OrangeMaliEsmeTransceiver(EnhancedEsmeTransceiver):
    
    def detect_error_delivery(self, pdu):
        if ('optional_parameters' in pdu['body']):
            for optional_parameter in pdu['body']['optional_parameters']:
                if optional_parameter['tag'] == 'network_error_code':
                    log.msg('NETWORK ERROR CODE %s' % optional_parameter['value'])
                    return True
        return False    

    def get_optional_parameter(self, pdu, tag):
        return (option for option in pdu['body']['optional_parameters'] if option['tag'] == tag).next()

    def handle_deliver_sm(self, pdu):
        if self.state not in ['BOUND_RX', 'BOUND_TRX']:
            log.err('WARNING: Received deliver_sm in wrong state: %s' % (
                self.state))

        if pdu['header']['command_status'] == 'ESME_ROK':
            sequence_number = pdu['header']['sequence_number']
            #user_message_reference = self.get_optional_parameter(pdu, 'user_message_reference')
            pdu_resp = DeliverSMResp(
                sequence_number,
                **self.defaults)
            #pdu_resp.obj['body']['mandatory_parameters']['message_id'] = (user_message_reference['value'] or '')
            message_id = str(uuid.uuid4())
            #pdu_resp.obj['body']['mandatory_parameters']['message_id'] = message_id
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
