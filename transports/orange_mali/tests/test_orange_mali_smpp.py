# encoding: utf-8
from twisted.internet.defer import inlineCallbacks

from vumi.transports.smpp.tests.test_smpp_transport import SmppTransceiverTransportTestCase
from vumi.transports.smpp.pdu_utils import seq_no, pdu_ok
from transports import OrangeMaliSmppTransport


class OrangeMaliSmppTransportTestCase(SmppTransceiverTransportTestCase):

    transport_class = OrangeMaliSmppTransport

    @inlineCallbacks
    def test_mo_sms(self):
        self.default_config['deliver_short_message_processor_config'] = {
            'data_coding_overrides': {
                1: 'utf-8',
            }
        }           
        smpp_helper = yield self.get_smpp_helper()
        smpp_helper.send_mo(
            sequence_number=1, short_message='foo \xc3\xab', source_addr='123',
            destination_addr='456')
        [deliver_sm_resp] = yield smpp_helper.wait_for_pdus(1)
        self.assertTrue(pdu_ok(deliver_sm_resp))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'foo ë')
        self.assertEqual(msg['from_addr'], '123')
        self.assertEqual(msg['to_addr'], '456')
        self.assertEqual(msg['transport_type'], 'sms')

    @inlineCallbacks
    def test_mo_sms_multipart_udh_accent(self):
        self.default_config['deliver_short_message_processor_config'] = {
            'data_coding_overrides': {
                1: 'utf-8',
            }
        }          
        smpp_helper = yield self.get_smpp_helper()
        deliver_sm_resps = []
        smpp_helper.send_mo(sequence_number=1,
                            short_message="\x05\x00\x03\xff\x03\x01back \xc3\xab")
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])
        smpp_helper.send_mo(sequence_number=2,
                            short_message="\x05\x00\x03\xff\x03\x02 at")
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])
        smpp_helper.send_mo(sequence_number=3,
                            short_message="\x05\x00\x03\xff\x03\x03 you")
        deliver_sm_resps.append((yield smpp_helper.wait_for_pdus(1))[0])
        self.assertEqual([1, 2, 3], map(seq_no, deliver_sm_resps))
        self.assertTrue(all(map(pdu_ok, deliver_sm_resps)))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'back ë at you')        
