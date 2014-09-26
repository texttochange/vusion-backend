from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks

from smpp.pdu_builder import DeliverSM

from vumi.transports.smpp.service import SmppService
from vumi.transports.smpp.clientserver.tests.utils import SmscTestServer
from vumi.transports.smpp.tests.test_smpp import EsmeToSmscTestCase
from vumi.transports.tests.test_base import TransportTestCase
from vumi.tests.utils import FakeRedis

from transports.orange_mali_smpp import OrangeMaliSmppTransport


class MockSmppTransport(OrangeMaliSmppTransport):
    
    def _setup_message_consumer(self):
        super(MockSmppTransport, self)._setup_message_consumer()
        self._block_till_bind.callback(None)


def mk_expected_pdu(direction, sequence_number, command_id, **extras):
    headers = {
        'command_status': 'ESME_ROK',
        'sequence_number': sequence_number,
        'command_id': command_id,
        }
    headers.update(extras)
    return {"direction": direction, "pdu": {"header": headers}}


class EsmeToOrangeMaliSmscTestCase(EsmeToSmscTestCase):
    
    transport_name = "esme_orange_mali_testing_transport"
    transport_class = MockSmppTransport

    @inlineCallbacks
    def test_return_message_id_in_deliver_sm_resp(self):
              
        ## Startup
        yield self.startTransport()
        yield self.transport._block_till_bind
        
        # First we make sure the Client binds to the Server
        # and enquire_link pdu's are exchanged as expected
        pdu_queue = self.service.factory.smsc.pdu_queue
        
        for i in range(1, 5):
            yield pdu_queue.get()

        pdu = DeliverSM(555,
                        short_message="SMS from server",
                        destination_addr="2772222222",
                        source_addr="2772000000")
        ## Specific Orange Malie the user_message_reference need to be returned
        #pdu.obj['body']['optional_parameters'] = []        
        #pdu.obj['body']['optional_parameters'].append({
        #           'tag':'user_message_reference',
        #           'length':0,
        #           'value':'123456'})
        self.service.factory.smsc.send_pdu(pdu)

        deliver_sm = yield pdu_queue.get()
        deliver_sm_resp = yield pdu_queue.get()
        self.assert_server_pdu(
            mk_expected_pdu("inbound", 555, "deliver_sm_resp"),
            deliver_sm_resp)
        ## Assert the user_message_reference is returned in message_id
        self.assertEqual(
            '',
            deliver_sm_resp['pdu']['body']['mandatory_parameters']['message_id'])
        
        dispatched_messages = self.get_dispatched_messages()
        msg = dispatched_messages[0].payload

        self.assertEqual(msg['content'], "SMS from server")
        self.assertEqual(msg['from_addr'], "2772000000")

    @inlineCallbacks
    def test_receive_sms_with_accent(self):

        ## Startup
        yield self.startTransport()
        yield self.transport._block_till_bind

        # First we make sure the Client binds to the Server
        # and enquire_link pdu's are exchanged as expected
        pdu_queue = self.service.factory.smsc.pdu_queue

        for i in range(1, 5):
            yield pdu_queue.get()

        pdu = DeliverSM(555,
                        short_message="SMS from server \xe0",
                        destination_addr="2772222222",
                        source_addr="2772000000")
        ## Specific Orange Malie the user_message_reference need to be returned
        self.service.factory.smsc.send_pdu(pdu)

        deliver_sm = yield pdu_queue.get()
        deliver_sm_resp = yield pdu_queue.get()
        self.assert_server_pdu(
            mk_expected_pdu("inbound", 555, "deliver_sm_resp"),
            deliver_sm_resp)
        ## Assert the user_message_reference is returned in message_id
        self.assertEqual(
            '',
            deliver_sm_resp['pdu']['body']['mandatory_parameters']['message_id'])

        dispatched_messages = self.get_dispatched_messages()
        msg = dispatched_messages[0].payload

        self.assertEqual(msg['content'], u"SMS from server \xe0")
        self.assertEqual(msg['from_addr'], "2772000000")
