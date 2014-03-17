from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks

from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.smpp.service import SmppService
from vumi.transports.smpp.clientserver.tests.utils import SmscTestServer
from vumi.message import TransportUserMessage
from vumi.tests.utils import FakeRedis

from tests.utils import ObjectMaker

from transports import EnhancedSmppTransport


class MockSmppTransport(EnhancedSmppTransport):
    def _setup_message_consumer(self):
        super(MockSmppTransport, self)._setup_message_consumer()
        self._block_till_bind.callback(None)


class LongMessageSmscTestServer(SmscTestServer):
    
    def command_status(self, pdu):
        if (pdu['body']['mandatory_parameters']['short_message'] is not None
            and pdu['body']['mandatory_parameters']['short_message'][:5] == "ESME_"):
            return pdu['body']['mandatory_parameters']['short_message'].split(
                ' ')[0]
        else:
            return 'ESME_ROK'


class EnhancedSmppTransportTestCase(TransportTestCase, ObjectMaker):

    transport_name = "esme_testing_transport"
    transport_class = MockSmppTransport

    @inlineCallbacks
    def setUp(self):
        yield super(EnhancedSmppTransportTestCase, self).setUp()
        self.config = {
            "system_id": "VumiTestSMSC",
            "password": "password",
            "host": "localhost",
            "port": 0,
            "redis": {},
            "transport_name": self.transport_name,
            "transport_type": "smpp",
            "submit_sm_encoding": "latin1",
            "submit_sm_data_encoding": 3,
            "service_type": "OPSOD"
        }
        self.service = SmppService(None, config=self.config)
        yield self.service.startWorker()
        self.service.factory.protocol = LongMessageSmscTestServer
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
        yield super(EnhancedSmppTransportTestCase, self).tearDown()
        self.transport.r_server.teardown()
        self.transport.factory.stopTrying()
        self.transport.factory.esme.transport.loseConnection()
        yield self.service.listening.stopListening()
        yield self.service.listening.loseConnection()
    
    @inlineCallbacks
    def test_submit_sm_date_encoding(self):
        
        expected_body_parameters = {
            "data_coding": 3,
            "short_message": "hello @ world"
        }
      
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
                content='hello @ world',
                transport_name=self.transport_name,
                transport_type='smpp',
                rkey='%s.outbound' % self.transport_name,
                timestamp='0',
                )
        yield self.dispatch(msg)

        #dump the connection setup
        for i in range(1, 5):
            yield pdu_queue.get()

        actual_message = yield pdu_queue.get()
        self.assertEqual(
            expected_body_parameters['data_coding'], 
            actual_message['pdu']['body']['mandatory_parameters']['data_coding']) 
        self.assertEqual(
            expected_body_parameters['short_message'], 
            actual_message['pdu']['body']['mandatory_parameters']['short_message'])        

    @inlineCallbacks
    def test_submit_long_message(self):

        self._block_till_bind = defer.Deferred()

        # Startup
        yield self.startTransport()
        yield self.transport._block_till_bind  
        
        pdu_queue = self.service.factory.smsc.pdu_queue        
        
        for i in range(1, 5):
            yield pdu_queue.get()

        msg = TransportUserMessage(
                to_addr="2772222222",
                from_addr="2772000000",
                content=self.mk_content(260),
                transport_name=self.transport_name,
                transport_type='smpp',
                transport_metadata={},
                rkey='%s.outbound' % self.transport_name,
                timestamp='0',
                )
        yield self.dispatch(msg)

        long_message = yield pdu_queue.get()

        self.assertEqual(
            0,
            long_message['pdu']['body']['mandatory_parameters']['sm_length'])
        self.assertEqual(
            None,
            long_message['pdu']['body']['mandatory_parameters']['short_message'])
        self.assertTrue(
            'value' in long_message['pdu']['body']['optional_parameters'][0])

    @inlineCallbacks
    def test_submit_sm_service_type(self):
        
        expected_body_parameters = {
            "service_type": "OPSOD",
        }
      
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
                content='hello @ world',
                transport_name=self.transport_name,
                transport_type='smpp',
                rkey='%s.outbound' % self.transport_name,
                timestamp='0',
                )
        yield self.dispatch(msg)

        #dump the connection setup
        for i in range(1, 5):
            yield pdu_queue.get()

        actual_message = yield pdu_queue.get()
        self.assertEqual(
            expected_body_parameters['service_type'], 
            actual_message['pdu']['body']['mandatory_parameters']['service_type'])
