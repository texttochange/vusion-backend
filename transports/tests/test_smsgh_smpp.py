from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks

from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.smpp.service import SmppService
from vumi.transports.smpp.clientserver.tests.utils import SmscTestServer
from vumi.message import TransportUserMessage

from test_push_tz_smpp import FakeRedis, mk_expected_pdu

from transports.smsgh_smpp import SmsghSmppTransport


class MockSmppTransport(SmsghSmppTransport):
    def _setup_message_consumer(self):
        super(MockSmppTransport, self)._setup_message_consumer()
        self._block_till_bind.callback(None)


class EsmeToSmscTestCaseCustomizedId(TransportTestCase):

    transport_name = "esme_testing_transport"
    transport_class = MockSmppTransport

    def assert_pdu_header(self, expected, actual, field):
        self.assertEqual(expected['pdu']['header'][field],
                         actual['pdu']['header'][field])

    def assert_server_pdu(self, expected_body_parameters, actual):
        self.assertEqual(
            expected_body_parameters['source_addr'], 
            actual['pdu']['body']['mandatory_parameters']['source_addr'])
        self.assertEqual(
            expected_body_parameters['source_addr_ton'], 
            actual['pdu']['body']['mandatory_parameters']['source_addr_ton'])
        self.assertEqual(
            expected_body_parameters['source_addr_npi'], 
            actual['pdu']['body']['mandatory_parameters']['source_addr_npi'])

    @inlineCallbacks
    def setUp(self):
        yield super(EsmeToSmscTestCaseCustomizedId, self).setUp()
        self.config = {
            "system_id": "VumiTestSMSC",
            "password": "password",
            "host": "localhost",
            "port": 0,
            "redis": {},
            "transport_name": self.transport_name,
            "transport_type": "smpp",
        }
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
        yield super(EsmeToSmscTestCaseCustomizedId, self).tearDown()
        self.transport.r_server.teardown()
        self.transport.factory.stopTrying()
        self.transport.factory.esme.transport.loseConnection()
        yield self.service.listening.stopListening()
        yield self.service.listening.loseConnection()
    
    @inlineCallbacks
    def test_submit_customized_id(self):
        
        expected_body_parameters = {
            "source_addr": "ttc", 
            "source_addr_ton": "national", 
            "source_addr_npi": "unknown"}
      
        ## Startup
        yield self.startTransport()
        yield self.transport._block_till_bind

        # First we make sure the Client binds to the Server
        # and enquire_link pdu's are exchanged as expected
        pdu_queue = self.service.factory.smsc.pdu_queue

        # Next the Client submits a SMS to the Server
        # and recieves an ack and a delivery_report

        msg = TransportUserMessage(
                to_addr="2772222222",
                from_addr="2772000000",
                content='hello world',
                transport_name=self.transport_name,
                transport_type='smpp',
                transport_metadata={
                    'customized-id': 'ttc'},
                rkey='%s.outbound' % self.transport_name,
                timestamp='0',
                )
        yield self.dispatch(msg)

        #dump the connection setup
        for i in range(1, 5):
            yield pdu_queue.get()

        actual_message = yield pdu_queue.get()
        self.assert_server_pdu(expected_body_parameters, actual_message)
