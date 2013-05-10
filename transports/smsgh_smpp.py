from twisted.internet.defer import inlineCallbacks

from vumi.transports.smpp import SmppTransport
from vumi.log import log
from vumi.utils import get_operator_number
from vumi.middleware import setup_middlewares_from_config

from middlewares.custom_middleware_stack import CustomMiddlewareStack


class SmsghSmppTransport(SmppTransport):
    
    @inlineCallbacks    
    def setup_middleware(self):
        middlewares = yield setup_middlewares_from_config(self, self.config)
        self._middlewares = CustomMiddlewareStack(middlewares)

    # Overwriting the methode for middleware hook
    def _process_message(self, message):
           def _send_failure(f):
               self.send_failure(message, f.value, f.getTraceback())
               log.err(f)
               if self.SUPPRESS_FAILURE_EXCEPTIONS:
                   return None
               return f
           
           d = self._middlewares.apply_consume("outbound", message,
                                               self.transport_name)
           d.addCallback(self.handle_outbound_message)
           d.addErrback(self._middlewares.process_control_flag)        
           d.addErrback(_send_failure)
           return d

    def send_smpp(self, message):
        log.msg("Sending SMPP message: %s" % (message))
        # first do a lookup in our YAML to see if we've got a source_addr
        # defined for the given MT number, if not, trust the from_addr
        # in the message
        to_addr = message['to_addr']
        from_addr = message['from_addr']
        text = message['content']
        route = get_operator_number(to_addr,
                self.config.get('COUNTRY_CODE', ''),
                self.config.get('OPERATOR_PREFIX', {}),
                self.config.get('OPERATOR_NUMBER', {})) or from_addr
        if 'customized_id' in message['transport_metadata']:
            sequence_number = self.esme_client.submit_sm(
                short_message=text.encode('utf-8'),
                destination_addr=str(to_addr),
                source_addr=message['transport_metadata']['customized_id'],
                source_addr_ton=2,
                source_addr_npi=0)
        else:
            sequence_number = self.esme_client.submit_sm(
                short_message=text.encode('utf-8'),
                destination_addr=str(to_addr),
                source_addr=route)
        return sequence_number
