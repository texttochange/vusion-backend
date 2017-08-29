# encoding: utf-8
import re
from string import Template

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import (
    VumiTestCase, MockHttpServer, RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full

from transports import MobifoneHttpTransport

class MobifoneHttpTransportTestCase(VumiTestCase):

    transport_type = 'sms'

    @inlineCallbacks
    def setUp(self):
        self.mobifone_calls = DeferredQueue()
        self.mock_mobifone = MockHttpServer(self.handle_request)
        self.mock_server_response = ''
        self.mock_server_response_code = http.OK
        yield self.mock_mobifone.start()
        
        self.config = {
            'outbound_url': self.mock_mobifone.url,
            'mobiaccount': 'accountL',
            'mobipassword': 'passwordL',
            'default_origin': '8282',
            'receive_path': '/mobifone',
            'receive_port': 9998}
        self.tx_helper = self.add_helper(TransportHelper(MobifoneHttpTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_mobifone.stop()
        yield super(MobifoneHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.mobifone_calls.put(request)
        request.setResponseCode(self.mock_server_response_code)
        return self.mock_server_response

    @inlineCallbacks
    def test_outbound_ok(self):
        self.mock_server_response = "ybs_autocreate_status%3DOK"
        self.mock_server_response_code = http.OK
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id = '1')

        req = yield self.mobifone_calls.get()

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_ok_accent(self):
        self.mock_server_response = "ybs_autocreate_status%3DOK"
        self.mock_server_response_code = http.OK
        yield self.tx_helper.make_dispatch_outbound(
            "für me", message_id = '1')
        
        req = yield self.mobifone_calls.get()
        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    #@inlineCallbacks
    #def test_outbound_ok_customized_id(self):
        #self.mock_server_response = "ybs_autocreate_status%3DOK"
        #self.mock_server_response_code = http.OK
        #yield self.tx_helper.make_dispatch_outbound(
            #"hello world",
            #message_id = '1',
            #transport_metadata={'customized_id': 'myid'})

        #req = yield self.mobifone_calls.get()
        #self.assertEqual('myid', req.args['origin'][0])

        #[event] = yield self.tx_helper.get_dispatched_events()
        #self.assertEqual(event['event_type'], 'ack')
        #self.assertEqual(event['user_message_id'], '1')
        #self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_nack_http(self):
        self.mock_server_response = "timeout"
        self.mock_server_response_code = http.REQUEST_TIMEOUT
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id = '1')
 
        req = yield self.mobifone_calls.get()

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "HTTP ERROR 408 - timeout")

    @inlineCallbacks
    def test_outbound_nack_transport(self):
        yield self.mock_mobifone.stop()
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id = '1')
 
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "TRANSPORT ERROR Connection refused")    

    @inlineCallbacks
    def test_outbound_nack_service(self):
        self.mock_server_response = (
            "ybs_autocreate_status%3DERROR%26"
            "ybs_autocreate_message%3DYBS%2BAutoCreate%2B"
            "Subsystem%3A%2BAccess%2Bdenied%2Bdue%2Bto%2B"
            "wrong%2Bauthorization%2Bcode")
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id = '1')
        
        req = yield self.mobifone_calls.get()

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "SERVICE ERROR ERROR - YBS AutoCreate Subsystem: Access denied due to wrong authorization code")

    @inlineCallbacks
    def test_inbound(self):
        url = "http://localhost:%s%s?phone=41791234567&smscenter=9292&text=Hello+World" % (
            self.config['receive_port'], self.config['receive_path'])

        response = yield http_request_full(url, method='GET')
        self.assertEqual(response.code, http.OK)
        
        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual('Hello World', user_msg['content'])
        self.assertEqual('41791234567', user_msg['from_addr'])
        self.assertEqual('8282', user_msg['to_addr'])

    @inlineCallbacks
    def test_inbound_phone_with_plus(self):
        t = Template("http://localhost:$port$path?phone=%2B41791234567&smscenter=9292&text=Hello+World")
        url = t.substitute(port=self.config['receive_port'],
                           path=self.config['receive_path'])
        
        response = yield http_request_full(url, method='GET')
        self.assertEqual(response.code, http.OK)
        
        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual('Hello World', user_msg['content'])
        self.assertEqual('+41791234567', user_msg['from_addr'])
        self.assertEqual('8282', user_msg['to_addr'])
