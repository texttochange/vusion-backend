from smpp.pdu_builder import SubmitSM

from vumi.log import log
from vumi.utils import get_operator_number
from vumi.transports.smpp import SmppTransport
from vumi.transports.smpp.clientserver.client import (
    EsmeTransceiver, EsmeTransceiverFactory)


class EnhancedSmppTransport(SmppTransport):
    
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
    
    def setup_transport(self):
        self.submit_sm_encoding = self.config.get("submit_sm_encoding", 'utf-8')
        self.submit_sm_data_encoding = self.config.get("submit_sm_data_encoding", 0)
        super(EnhancedSmppTransport, self).setup_transport()

    def make_factory(self):
        return EnhancedEsmeTransceiverFactory(
            self.client_config,
            self.r_server,
            self.esme_callbacks)
    
    def delivery_status(self, stat):
        return self.DELIVERY_REPORT_STATUS_MAPPING.get(stat, 'pending')
    
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
        sequence_number = self.esme_client.submit_sm(
                short_message=text.encode(self.submit_sm_encoding),
                data_coding=self.submit_sm_data_encoding,
                destination_addr=str(to_addr),
                source_addr=route)
        return sequence_number


class EnhancedEsmeTransceiverFactory(EsmeTransceiverFactory):
    
    def buildProtocol(self, addr):
        log.msg('Connected')
        self.esme = EnhancedEsmeTransceiver(self.config, self.kvs, self.esme_callbacks)
        self.resetDelay()
        return self.esme


class EnhancedEsmeTransceiver(EsmeTransceiver):
    
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
