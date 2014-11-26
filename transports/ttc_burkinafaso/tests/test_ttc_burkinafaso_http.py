# encoding: utf-8
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import VumiTestCase, MockHttpServer
from vumi.utils import http_request_full

from transports import TtcBurkinafasoHttpTransport


class TtcBurkinafasoHttpTransportTestCase(VumiTestCase):
    
    transport_type = 'sms'

    @inlineCallbacks
    def setUp(self):
        self.ttc_calls = DeferredQueue()
        self.mock_ttc = MockHttpServer(self.handle_request)
        self.mock_server_response = ''
        self.mock_server_response_code = http.OK
        yield self.mock_ttc.start()
        
        self.receive_path = 'ttc_bf'
        self.receive_port = 9998
        self.config = {
            'send_url': self.mock_ttc.url,
            'receive_path': self.receive_path,
            'receive_port': self.receive_port,
            'default_shortcode': '3400'}
        self.tx_helper = self.add_helper(TransportHelper(TtcBurkinafasoHttpTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)    

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_ttc.stop()
        yield super(TtcBurkinafasoHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.ttc_calls.put(request)
        request.setResponseCode(self.mock_server_response_code)
        return self.mock_server_response

    @inlineCallbacks
    def test_outbound_ok(self):
        self.mock_server_response = ""
        self.mock_server_response_code = http.OK
        yield self.tx_helper.make_dispatch_outbound(
            u"ça éprôùvè", message_id='1')

        req = yield self.ttc_calls.get()
        self.assertEqual("+41791234567", req.args['to_addr'][0])
        self.assertEqual("3400", req.args["from_addr"][0])
        self.assertEqual("ça éprôùvè", req.args["message"][0])
        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_nack_service(self):
        self.mock_server_response = "an error occured"
        self.mock_server_response_code = http.INTERNAL_SERVER_ERROR
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')
        req = yield self.ttc_calls.get()
        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "SERVICE ERROR 500 - an error occured")

    @inlineCallbacks
    def test_outbound_nack_http(self):
        self.mock_server_response = ""
        self.mock_server_response_code = http.INTERNAL_SERVER_ERROR
        
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')
        req = yield self.ttc_calls.get()
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "HTTP ERROR 500 - ")

    @inlineCallbacks
    def test_outbound_nack_transport(self):
        yield self.mock_ttc.stop()
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "TRANSPORT ERROR Connection refused")

    @inlineCallbacks
    def test_inbound(self):
        url_template = "http://localhost:%s/%s?%s"
        params = {'from_addr': '26477',
                  'to_addr': '4444',
                  'message': "ça éprôùvè"}
        url = url_template % (
            self.receive_port, self.receive_path, urlencode(params))
       
        response = yield http_request_full(
            url,
            method='GET')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, '')
        
        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual(u"ça éprôùvè", user_msg['content'])
        self.assertEqual("26477", user_msg['from_addr'])
        self.assertEqual("3400", user_msg['to_addr'])
