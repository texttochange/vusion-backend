import json
import re

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import VumiTestCase, MockHttpServer
from vumi.utils import http_request_full

from transports.orange_sdp.orange_http import OrangeSdpHttpTransport


class OrangeSdpMessageMaker:

    def mk_notify_sms_reception(self):
        return {
            "inboundSMSMessageNotification": {
                "inboundSMSMessage": {
                    "senderAddress": "tel:+22507099573",
                    "destinationAddress": "+2257839",
                    "message": "Burrito test",
                    "dateTime": "2012-10-10T11:17:27+02:00"}}}

    def mk_notify_sms_delivery_receipt(self,
                                       delivery_status="DeliveredToTerminal"):
        return {
            "deliveryInfoNotification": {
                "deliveryInfo": {
                    "address": "tel:+22507099573",
                    "deliveryStatus": delivery_status},
                "callbackData": "1"}}

    def mk_mt_response(self):
        return {
            "resourceReference":{
                "resourceURL": "http://10.99.163.149:7601/1/smsmessaging/outbound/tel%3A%2B2257840/requests/00JZM14316777526Z1010111109"}}

    def mk_start_delivery_deceipt_response(self):
        return {
            "deliveryReceiptSubscription": {
                "callbackReference": {
                    "callbackData":"callFunction()",
                    "notifyURL":"http://90.27.20.111:3005/restSmsService/delivery"},
                "resourceURL":"http://10.99.163.149:7601/1 /smsmessaging/outbound/%2B33688663346/subscriptions/00JZM14216777523Z1010111029"}}

    def mk_start_sms_notification_response(self):
        return {
            "resourceReference": {
                "resourceURL":"http://10.99.163.149:7601/1/smsmessaging/inbound/subscriptions/00JZM14216777525Z1010111616"}}

    def mk_serviceError_response(self):
        return {
            "requestError": {
            "serviceException": {
                "messageId": "SVC0002",
                "text": "Invalid Input Value (...)"}}}

    def mk_policy_error_response(self):
        return {
            "requestError": {
                "policyException": {
                    "messageId": "POL1102",
                    "text": "SMS Bunch ASP Control (...)"}}}

## TODO: Delivery Receipt support
class OrangeSdpHttpTransportTestCase(VumiTestCase, OrangeSdpMessageMaker):

    @inlineCallbacks
    def setUp(self):
        self.orange_calls = DeferredQueue()
        self.orange_content_calls = []
        self.mock_orange_server = MockHttpServer(self.handle_request)
        self.mock_orange_server_response = ''
        self.mock_orange_server_response_code = http.CREATED
        yield self.mock_orange_server.start()
        self.config = {
            'url': self.mock_orange_server.url,
            'service_provider_id': '9876543210',
            'password': '888',
            'product_id': '98765432100004',
            'receive_domain': 'http://localhost',
            'receive_port': 9998,
            'receive_path': '/orangeSdp'}
        self.tx_helper = self.add_helper(TransportHelper(OrangeSdpHttpTransport))
        OrangeSdpHttpTransport.get_timestamp = lambda m: "20101105010125300"

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_orange_server.stop()
        yield super(OrangeSdpHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.orange_calls.put(request)
        self.orange_content_calls.append(request.content.read())
        request.setResponseCode(self.mock_orange_server_response_code)
        return self.mock_orange_server_response

    def assert_auth(self, req):
        auth_header = req.getHeader('Authorization')
        self.assertEqual(
            auth_header,
            ('AUTH spId="9876543210",'
             'spPassword="85d31acc80283a80ce77a1c01465ff3e",'
             'timeStamp="20101105010125300",'
             'productId="98765432100004"'))

    @inlineCallbacks
    def test_set_callbacks(self):
        self.mock_orange_server_response_code = http.CREATED
        self.mock_orange_server_response = json.dumps(
            self.mk_notify_sms_reception())
        self.transport = yield self.tx_helper.get_transport(self.config)

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        req_content = json.loads(self.orange_content_calls.pop())
        self.assertEqual(
            req_content, 
            {"notifyURL": "http://localhost:9998/orangeSdp",
             "clientCorrelator": "ttc_mo"})

        events = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(len(events), 0)

    @inlineCallbacks
    def test_outbound_ok(self):
        self.transport = yield self.tx_helper.get_transport(self.config)        
        yield self.orange_calls.get()
        self.orange_content_calls.pop()
        
        self.mock_orange_server_response_code = http.CREATED
        self.mock_orange_server_response = json.dumps(self.mk_mt_response())
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr='256-8181')

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        self.assertTrue(re.match(r'^/1/smsmessaging/outbound/tel:\+2568181/requests$', req.uri))

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_ok_fail_http(self):
        self.transport = yield self.tx_helper.get_transport(self.config)        
        yield self.orange_calls.get()
        self.orange_content_calls.pop()
        
        self.mock_orange_server_response_code = http.BAD_REQUEST
        self.mock_orange_server_response = json.dumps(self.mk_policy_error_response())
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')

        req = yield self.orange_calls.get()

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(
            event['nack_reason'], 
            ('HTTP/SERVICE ERROR 400 - {"requestError": {"policyException": '
             '{"text": "SMS Bunch ASP Control (...)",'
             ' "messageId": "POL1102"}}}'))

    @inlineCallbacks
    def test_outbound_ok_fail_transport(self):
        self.transport = yield self.tx_helper.get_transport(self.config)        
        yield self.orange_calls.get()
        self.orange_content_calls.pop()
        
        self.mock_orange_server.stop()
        
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(
            event['nack_reason'],
            'TRANSPORT ERROR Connection refused')

    @inlineCallbacks
    def test_inbound(self):
        self.transport = yield self.tx_helper.get_transport(self.config)        
        yield self.orange_calls.get()
        self.orange_content_calls.pop()

        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(
            url,
            headers={'Content-Type': ['application/json']},
            method='POST',
            data=json.dumps(self.mk_notify_sms_reception()))
        
        self.assertEqual(response.code, http.OK)
        
        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual('Burrito test', user_msg['content'])
        self.assertEqual('+22507099573', user_msg['from_addr'])

    #@inlineCallbacks
    #def test_dlr(self):
        #self.transport = yield self.tx_helper.get_transport(self.config)        
        #yield self.orange_calls.get()
        #self.orange_content_calls.pop()

        #url = ("http://localhost:%s%s"
               #% (self.config['receive_port'], self.config['receive_path']))
        #response = yield http_request_full(
            #url,
            #headers={'Content-Type': ['application/json']},
            #method='POST',
            #data=json.dumps(self.mk_notify_sms_delivery_receipt()))
        
        #self.assertEqual(response.code, http.OK)

        #[event] = yield self.tx_helper.get_dispatched_inbound()
        #self.assertEqual(event['event_type'], 'delivered')
        #self.assertEqual(event['user_message_id'], '1')
