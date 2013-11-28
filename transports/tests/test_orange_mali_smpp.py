from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks

from smpp.pdu_builder import DeliverSM

from vumi.transports.smpp.service import SmppService
from vumi.transports.smpp.clientserver.tests.utils import SmscTestServer
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


class EsmeToOrangeMaliSmscTestCase(TransportTestCase):
    
    transport_name = "esme_orange_mali_testing_transport"
    transport_class = MockSmppTransport

    def assert_pdu_header(self, expected, actual, field):
        self.assertEqual(expected['pdu']['header'][field],
                         actual['pdu']['header'][field])

    def assert_server_pdu(self, expected, actual):
        self.assertEqual(expected['direction'], actual['direction'])
        self.assert_pdu_header(expected, actual, 'sequence_number')
        self.assert_pdu_header(expected, actual, 'command_status')
        self.assert_pdu_header(expected, actual, 'command_id')

    @inlineCallbacks
    def setUp(self):
        yield super(EsmeToOrangeMaliSmscTestCase, self).setUp()
        self.config = {
            "system_id": "VumiTestSMSC",
            "password": "password",
            "host": "localhost",
            "port": 0,
            "redis": {},
            "transport_name": self.transport_name,
            "transport_type": "smpp"}
        self.service = SmppService(None, config=self.config)
        yield self.service.startWorker()
        self.service.factory.protocol = SmscTestServer
        self.config['port'] = self.service.listening.getHost().port
        self.transport = yield self.get_transport(self.config, start=False)
        self.transport.r_server = FakeRedis()
        self.expected_delivery_status = 'delivered'

    @inlineCallbacks
    def startTransport(self):
        self.transport._block_till_bind = defer.Deferred()
        yield self.transport.startWorker()

    @inlineCallbacks
    def tearDown(self):
        yield super(EsmeToOrangeMaliSmscTestCase, self).tearDown()
        self.transport.r_server.teardown()
        self.transport.factory.stopTrying()
        self.transport.factory.esme.transport.loseConnection()
        yield self.service.listening.stopListening()
        yield self.service.listening.loseConnection()

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
