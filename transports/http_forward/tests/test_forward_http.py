
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import VumiTestCase, MockHttpServer

from transports import ForwardHttp


class ForwardHttpTransportTestCase(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.fh_1_calls = DeferredQueue()
        self.mock_fh_1 = MockHttpServer(self.handle_request_1)
        self.mock_fh_1_response = ''
        self.mock_fh_1_response_code = http.OK
        yield self.mock_fh_1.start()

        self.fh_2_calls = DeferredQueue()
        self.mock_fh_2 = MockHttpServer(self.handle_request_2)
        self.mock_fh_2_response = ''
        self.mock_fh_2_response_code = http.OK
        yield self.mock_fh_2.start()

        self.config = {
            'message_replacement':{
                'content': '\[MESSAGE\]',
                'from_addr': '\[PROGRAM\]'},
            'message_metadata_replacement': {
                'participant_phone': '\[FROM\]',
                'program_shortcode': '\[TO\]'}
        }
        self.tx_helper = self.add_helper(TransportHelper(ForwardHttp))
        self.transport = yield self.tx_helper.get_transport(self.config)

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_fh_1.stop()
        yield self.mock_fh_2.stop()
        yield super(ForwardHttpTransportTestCase, self).tearDown()

    def handle_request_1(self, request):
        self.fh_1_calls.put(request)
        request.setResponseCode(self.mock_fh_1_response_code)
        return self.mock_fh_1_response

    def handle_request_2(self, request):
        self.fh_2_calls.put(request)
        request.setResponseCode(self.mock_fh_2_response_code)
        return self.mock_fh_2_response

    @inlineCallbacks
    def test_outbound_ok_url_with_arguments(self):
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%ssendsms1?message=[MESSAGE]&from=[FROM]&to=[TO]&program=[PROGRAM]" % self.mock_fh_1.url,
            from_addr="myprogram",
            content="hello world",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        req_1 = yield self.fh_1_calls.get()
        self.assertEqual(
            req_1.args, 
            {'to':['256-8181'],
             'message': ['hello world'],
             'program': ['myprogram'],
             'from': ['+6']});
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_ok_same_message_multiple_forward(self):
        yield self.tx_helper.make_dispatch_outbound(
            "hello world 1",
            to_addr="%ssendsms1?message=[MESSAGE]&from=[FROM]&to=[TO]&program=[PROGRAM]" % self.mock_fh_1.url,
            from_addr="myprogram",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        yield self.tx_helper.make_dispatch_outbound(
            "hello world 2",
            to_addr="%ssendsms2" % self.mock_fh_2.url,
            from_addr="myprogram",
            message_id='2',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})

        req_1 = yield self.fh_1_calls.get()
        self.assertEqual(
            req_1.args,
            {'to':['256-8181'],
             'message': ['hello world 1'],
             'program': ['myprogram'],
             'from': ['+6']});
        req_2 = yield self.fh_2_calls.get()
        self.assertEqual(req_2.args, {}); 

        [ack1, ack2] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(ack1['event_type'], 'ack')
        self.assertEqual(ack1['user_message_id'], '1')
        self.assertEqual(ack1['transport_metadata'], {'transport_type':'http_api'})
        
        self.assertEqual(ack2['event_type'], 'ack')
        self.assertEqual(ack2['user_message_id'], '2')
        self.assertEqual(ack2['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_fail_service(self):
        self.mock_fh_1_response = "SOME INTERNAL STUFF HAPPEN"
        self.mock_fh_1_response_code = http.INTERNAL_SERVER_ERROR
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%ssendsms1" % self.mock_fh_1.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        req = yield self.fh_1_calls.get()

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], 'HTTP ERROR 500 - SOME INTERNAL STUFF HAPPEN')
        self.assertEqual(event['transport_metadata'], {'transport_type': 'http_api'})

    @inlineCallbacks
    def test_outbound_fail_transport(self):
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="http://localhost:9997/sendsms2",
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], 'TRANSPORT ERROR Connection refused')
        self.assertEqual(event['transport_metadata'], {'transport_type': 'http_api'})

    @inlineCallbacks
    def test_outbound_fail_wrong_url_format(self):
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="htp://localhost:9997/sendsms",
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], 'TRANSPORT ERROR Unsupported scheme: \'htp\'')
        self.assertEqual(event['transport_metadata'], {'transport_type': 'http_api'})
