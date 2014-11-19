from twisted.internet.defer import inlineCallbacks

from vumi.transports.smpp.pdu_utils import command_id, short_message
from vumi.transports.smpp.tests.test_smpp_transport import SmppTransceiverTransportTestCase

from transports import SmsghSmppTransport


class SmsghSmppTransportTestCase(SmppTransceiverTransportTestCase):
    
    transport_class = SmsghSmppTransport

    def setUp(self):
        super(SmppTransceiverTransportTestCase, self).setUp()
        self.default_config['source_addr_ton'] = 2   #required by smsgh

    @inlineCallbacks
    def test_mt_sms_customized_id(self):
        smpp_helper = yield self.get_smpp_helper()
        msg = self.tx_helper.make_outbound(
            'hello world', transport_metadata={'customized_id': 'myid'})
        yield self.tx_helper.dispatch_outbound(msg)
        [pdu] = yield smpp_helper.wait_for_pdus(1)
        self.assertEqual(command_id(pdu), 'submit_sm')
        self.assertEqual(pdu['body']['mandatory_parameters']['source_addr'], 'myid')
        self.assertEqual(pdu['body']['mandatory_parameters']['source_addr_npi'], 'unknown')
        self.assertEqual(pdu['body']['mandatory_parameters']['source_addr_ton'], 'national')
