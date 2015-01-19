import json
import re

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import VumiTestCase
from vumi.utils import http_request_full

from transports.tests.utils import MockHttpServer
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

    def mk_start_delivery_receipt_response(self, shortcode, sub_id="00JZM14216777523Z1010111029"):
        return {
            "deliveryReceiptSubscription": {
                "callbackReference": {
                    "callbackData":"callFunction()",
                    "notifyURL":"http://90.27.20.111:3005/restSmsService/delivery"},
                "resourceURL":"http://10.99.163.149:7601/1 /smsmessaging/outbound/+%s/subscriptions/%s" % (shortcode, sub_id)}}

    def mk_start_sms_notification_response(self, sub_id="00JZM14216777525Z1010111616"):
        return {
            "resourceReference": {
                "resourceURL":"http://10.99.163.149:7601/1/smsmessaging/inbound/subscriptions/%s" % sub_id}}

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


class OrangeSdpHttpTransportTestCase(VumiTestCase, OrangeSdpMessageMaker):

    @inlineCallbacks
    def setUp(self):
        self.orange_calls = DeferredQueue()
        self.orange_content_calls = []
        self.mock_orange_server = MockHttpServer(self.handle_request)
        self.mock_orange_server_response = []
        yield self.mock_orange_server.start()
        self.config = {
            'url': self.mock_orange_server.url,
            'service_provider_id': '9876543210',
            'password': '888',
            'product_id': '98765432100004',
            'receive_domain': 'http://localhost',
            'receive_port': 9998,
            'receive_path': '/orangeSdp',
            'shortcodes': {
                '256': ['8181'],    #as shortcode: internation-prefix
                '254': ['8181', '21222']}
        }
        self.tx_helper = self.add_helper(TransportHelper(OrangeSdpHttpTransport))
        OrangeSdpHttpTransport.get_timestamp = lambda m: "20101105010125300"

    @inlineCallbacks
    def tearDown(self):
        self.tearnDown_transport_callback_done()
        yield self.mock_orange_server.stop()
        yield super(OrangeSdpHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.orange_calls.put(request)
        self.orange_content_calls.append(request.content.read())
        try:
            (resp_code, resp_content) = self.mock_orange_server_response.pop(0)
            request.setResponseCode(resp_code)
            return resp_content
        except Exception as ex:
            pass

    def assert_auth(self, req):
        auth_header = req.getHeader('Authorization')
        self.assertEqual(
            auth_header,
            ('AUTH spId="9876543210",'
             'spPassword="85d31acc80283a80ce77a1c01465ff3e",'
             'timeStamp="20101105010125300",'
             'productId="98765432100004"'))

    @inlineCallbacks
    def setUp_transport_callback_done(self):
        self.mock_orange_server_response.append((
            http.CREATED,
            json.dumps(self.mk_start_sms_notification_response('Z'))))
        ##dlr subscriptions
        self.mock_orange_server_response.append((
            http.CREATED,
            json.dumps(self.mk_start_delivery_receipt_response('2568181','Y'))))
        self.mock_orange_server_response.append((
            http.CREATED,
            json.dumps(self.mk_start_delivery_receipt_response('2548181','W'))))
        self.mock_orange_server_response.append((
            http.CREATED,
            json.dumps(self.mk_start_delivery_receipt_response('25421222','X'))))

        self.transport = yield self.tx_helper.get_transport(self.config)

        self.orange_calls = DeferredQueue()
        self.orange_content_calls = []

    def tearnDown_transport_callback_done(self):
        self.transport.subscription_ids = []

    @inlineCallbacks
    def test_dlr_set_callbacks(self):
        ##mo subscription
        self.mock_orange_server_response.append((
            http.CREATED, 
            json.dumps(self.mk_start_sms_notification_response('Z'))))
        ##dlr subscriptions
        self.mock_orange_server_response.append((
            http.CREATED, 
            json.dumps(self.mk_start_delivery_receipt_response('2568181','Y'))))
        self.mock_orange_server_response.append((
            http.CREATED,
            json.dumps(self.mk_start_delivery_receipt_response('2548181','W'))))
        self.mock_orange_server_response.append((
            http.CREATED,
            json.dumps(self.mk_start_delivery_receipt_response('25421222','X'))))

        self.transport = yield self.tx_helper.get_transport(self.config)

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        self.assertEqual(
            req.uri, '/1/smsmessaging/inbound/subscriptions')
        req_content = json.loads(self.orange_content_calls.pop(0))
        self.assertEqual(
            req_content, 
            {"notifyURL": "http://localhost:9998/orangeSdp",
             "clientCorrelator": "ttc_mo"})

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        self.assertEqual(
            req.uri, '/1/smsmessaging/outbound/+2568181/subscriptions')
        req_content = json.loads(self.orange_content_calls.pop(0))
        self.assertEqual(
            req_content, 
            {"notifyURL": "http://localhost:9998/orangeSdp",
             "clientCorrelator": "ttc_dlr_2568181"})

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        self.assertEqual(
            req.uri, '/1/smsmessaging/outbound/+2548181/subscriptions')
        req_content = json.loads(self.orange_content_calls.pop(0))
        self.assertEqual(
            req_content, 
            {"notifyURL": "http://localhost:9998/orangeSdp",
             "clientCorrelator": "ttc_dlr_2548181"})

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        self.assertEqual(
            req.uri, '/1/smsmessaging/outbound/+25421222/subscriptions')
        req_content = json.loads(self.orange_content_calls.pop(0))
        self.assertEqual(
            req_content, 
            {"notifyURL": "http://localhost:9998/orangeSdp",
             "clientCorrelator": "ttc_dlr_25421222"})

        ##stop subscriptions
        self.mock_orange_server_response.append((
            http.NO_CONTENT,''))
        self.mock_orange_server_response.append((
            http.NO_CONTENT,''))
        self.mock_orange_server_response.append((
            http.NO_CONTENT,''))
        self.mock_orange_server_response.append((
            http.NO_CONTENT,''))
        yield self.transport.stop_callbacks();

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        self.assertEqual(
            req.uri, '/1/smsmessaging/inbound/subscriptions/Z')

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        self.assertEqual(
            req.uri, '/1/smsmessaging/outbound/subscriptions/Y')

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        self.assertEqual(
            req.uri, '/1/smsmessaging/outbound/subscriptions/W')

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        self.assertEqual(
            req.uri, '/1/smsmessaging/outbound/subscriptions/X')

        events = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(len(events), 0)

    @inlineCallbacks
    def test_outbound_ok(self):
        yield self.setUp_transport_callback_done()

        self.mock_orange_server_response.append((
            http.CREATED, json.dumps(self.mk_mt_response())))
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr='8181', to_addr='254111')

        req = yield self.orange_calls.get()
        self.assert_auth(req)
        self.assertEqual(
            '/1/smsmessaging/outbound/tel:+2548181/requests',
            req.uri)

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_ok_fail_http(self):
        yield self.setUp_transport_callback_done()

        self.mock_orange_server_response.append(
            (http.BAD_REQUEST, json.dumps(self.mk_policy_error_response())))
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr="8181", to_addr="254111")

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
        yield self.setUp_transport_callback_done()

        self.mock_orange_server.stop()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr='8181', to_addr='254111')

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(
            event['nack_reason'],
            'TRANSPORT ERROR Connection refused')

    @inlineCallbacks
    def test_inbound(self):
        yield self.setUp_transport_callback_done()

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

    @inlineCallbacks
    def test_dlr_delivered(self):
        yield self.setUp_transport_callback_done()

        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(
            url,
            headers={'Content-Type': ['application/json']},
            method='POST',
            data=json.dumps(self.mk_notify_sms_delivery_receipt()))

        self.assertEqual(response.code, http.OK)

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], '1')

    @inlineCallbacks
    def test_dlr_impossible(self):
        yield self.setUp_transport_callback_done()

        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(
            url,
            headers={'Content-Type': ['application/json']},
            method='POST',
            data=json.dumps(self.mk_notify_sms_delivery_receipt(
                "DeliveryImpossible")))

        self.assertEqual(response.code, http.OK)

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'failed')
        self.assertEqual(event['user_message_id'], '1')
