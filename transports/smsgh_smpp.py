from vumi.transports.smpp import SmppTransport
from vumi.log import log
from vumi.utils import get_operator_number

from push_tz_smpp import PushTzSmppTransport


class SmsghSmppTransport(SmppTransport):
    
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
