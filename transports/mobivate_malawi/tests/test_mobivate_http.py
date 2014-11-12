import re

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import (
    VumiTestCase, MockHttpServer, RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full

from transports import MobivateHttpTransport


class MobivateHttpTransportTestCase(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.mobivate_calls = DeferredQueue()
        self.mock_mobivate = MockHttpServer(self.handle_request)
        self.mock_server_response = ''
        self.mock_server_response_code = http.OK
        yield self.mock_mobivate.start()
        self.config ={
            'url': self.mock_mobivate.url,
            'user_name': 'username',
            'password': 'password',
            'default_origin': '55882',
            'receive_path': '/mobivate',
            'receive_port': 9998}
        self.tx_helper = self.add_helper(TransportHelper(MobivateHttpTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_mobivate.stop()
        yield super(MobivateHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.mobivate_calls.put(request)
        request.setResponseCode(self.mock_server_response_code)
        return self.mock_server_response
    
    @inlineCallbacks
    def test_outbound_ok(self):
        self.mock_server_response = "0"
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')
        req = yield self.mobivate_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'GET')
        self.assertEqual(req.args, {
            'MESSAGE_TEXT': ['hello world'],
            'ORIGINATOR': ['9292'],
            'PASSWORD': ['password'],
            'RECIPIENT': ['+41791234567'],
            'REFERENCE': ['1'],
            'USER_NAME': ['username']})
        
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_fail(self):
        self.mock_server_response = "500\nSome internal issue"
        yield self.tx_helper.make_dispatch_outbound(
                    "hello world", message_id='1')
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "SERVICE ERROR 500 - Some internal issue")

    @inlineCallbacks
    def test_inbound(self):
        params = ("ORIGINATOR=61412345678&RECIPIENT=1987654&PROVIDER=telstra"
                  "&MESSAGE_TEXT=Hello%20There!")
        url = ("http://localhost:%s%s/SMSfromMobiles?%s" %
               (self.config['receive_port'], self.config['receive_path'], params))

        response = yield http_request_full(url, method='GET')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, '0')        

        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual("Hello There!", user_msg['content'])
        self.assertEqual("61412345678", user_msg['from_addr'])
        self.assertEqual("1987654", user_msg['to_addr'])

    @inlineCallbacks
    def test_delivery_report(self):
        params = ("ORIGINATOR=61412345678&RECIPIENT=1987654&PROVIDER=telstra"
                  "&MESSAGE_TEXT=Hello%20There!&ID=939ec52e333fbf124a87845d3a5d72e1"
                  "&REFERENCE=ABC123&RESULT=1")
        url = ("http://localhost:%s%s/DeliveryReciept?%s" %
               (self.config['receive_port'], self.config['receive_path'], params))

        response = yield http_request_full(url, method='GET')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, '0')        
        
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['user_message_id'], 'ABC123')
        self.assertEqual(event['delivery_status'], 'delivered')
