from smpp.pdu_builder import SubmitSM
from vumi.transports.smpp.clientserver.client import EsmeTransceiver


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
