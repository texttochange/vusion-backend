from smpp.pdu_builder import SubmitSM

from vumi.log import log
from vumi.transports.smpp import SmppTransport
from vumi.transports.smpp.clientserver.client import (
    EsmeTransceiver, EsmeTransceiverFactory)


class EnhancedSmppTransport(SmppTransport):
    
    def make_factory(self):
        return EnhancedEsmeTransceiverFactory(
            self.client_config,
            self.r_server,
            self.esme_callbacks)


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
