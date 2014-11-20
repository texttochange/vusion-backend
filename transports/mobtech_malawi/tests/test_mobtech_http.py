import urllib

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.helpers import TransportHelper 
from vumi.tests.utils import VumiTestCase, MockHttpServer
from vumi.utils import http_request_full

from transports import MobtechHttpTransport

class MobtechMlHttpTransportTestCase(VumiTestCase):
      
    @inlineCallbacks
    def setUp(self):
        self.mobtech_calls = DeferredQueue()
        self.mock_mobtech = MockHttpServer(self.handle_request)
        self.mock_server_response = ''
        self.mock_server_response_code = http.OK
        yield self.mock_mobtech.start()
        
        self.domain = 'localhost'
        self.receive_path = 'mobtech'
        self.mo_path = 'mo'
        self.delivery_path = 'delivery'
        self.receive_port = 9998
        self.config = {
            'url': self.mock_mobtech.url,
            'username': 'myusername',
            'password': 'mypassword',
            'mt_response_regex': '^(?P<status>\d+): +(?P<message>[\w\s]+)',
            'domain': self.domain,
            'receive_path': self.receive_path,
            'mo_receive_path': self.mo_path,
            'delivery_receive_path': self.delivery_path,
            'receive_port': self.receive_port,
            'delivery_url_params': 'type=%d&receiver=%p&reply=%A&time=%t&usr=%n&message=%b&dlr-mask=7',
            'delivery_regex': 'dlvrd:(?P<dlvrd>\d*)',
            'stat_regex': 'stat:(?P<stat>[A-Z]{7})'}
        self.tx_helper = self.add_helper(TransportHelper(MobtechHttpTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_mobtech.stop()
        yield super(MobtechMlHttpTransportTestCase, self).tearDown()
        
    def handle_request(self, request):
        self.mobtech_calls.put(request)
        request.setResponseCode(self.mock_server_response_code)
        return self.mock_server_response

    @inlineCallbacks
    def test_outbound_ok(self):
        self.mock_server_response = "0: Accepted for delivery"
        self.mock_server_response_code = http.OK
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')
        
        req = yield self.mobtech_calls.get()
        headers = dict(req.requestHeaders.getAllRawHeaders())
        self.assertEqual(headers['Content-Type'], ['application/x-www-form-urlencoded'])
        self.assertEqual("myusername", req.args['username'][0])
        self.assertEqual("mypassword", req.args['password'][0])
        self.assertEqual("+41791234567", req.args['to'][0])
        self.assertEqual("9292", req.args["from"][0])
        self.assertEqual("hello world", req.args["text"][0])
        self.assertEqual("1", req.args["messageid"][0])

         ## assert the event
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_oubound_nack_service(self):
        self.mock_server_response = "01: Message too long"
        self.mock_server_response_code = http.OK
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')
        req = yield self.mobtech_calls.get()

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "SERVICE ERROR 01 - Message too long")
        
    @inlineCallbacks
    def test_outbound_nack_http(self):
        self.mock_server_response = ""
        self.mock_server_response_code = http.INTERNAL_SERVER_ERROR
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')
        
        req = yield self.mobtech_calls.get()

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "HTTP ERROR 500 - ")

    @inlineCallbacks
    def test_outbound_nack_transport(self):
        self.mock_server_response = "something happened"
        self.mock_server_response_code = http.OK        
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')
        req = yield self.mobtech_calls.get()
        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "TRANSPORT ERROR 'NoneType' object has no attribute 'groupdict'")
        
    @inlineCallbacks
    def test_inbound(self):
        url_template = "http://localhost:%s/%s/%s"
        url = url_template % (self.receive_port, self.receive_path, self.mo_path)
        params = {'from': '26477',
                  'to': '4444',
                  'text': 'Hello World',
                  'operator': 'something'}
        
        response = yield http_request_full(
            url,
            data=urllib.urlencode(params),
            headers={'Content-Type': ['application/x-www-form-urlencoded']},
            method='POST')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, 'OK')
        
        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual("Hello World", user_msg['content'])
        self.assertEqual("26477", user_msg['from_addr'])
        self.assertEqual("4444", user_msg['to_addr'])

    @inlineCallbacks
    def test_delivery_report_delivered_dlvrd_only(self):
        url_template = "http://localhost:%s/%s/%s?%s"
        url_params = {'messageid': '4345',
                      'type': 'titi',
                      'receiver': 'tata',
                      'reply': 'id:c449ab9744f47b6af1879e49e75e4f40 sub:001 dlvrd:1 submit date:0610191018',
                      'time': 'tutu',
                      'usr': 'tyty',
                      'message': 'tete',
                      'dlr-mask': '7'}
        url = url_template % (self.receive_port, self.receive_path, self.delivery_path, urllib.urlencode(url_params))

        response = yield http_request_full(
            url,
            None,
            headers={"Content-Type": ["text/xml"]},
            method='GET')
        self.assertEqual(response.code, http.OK)
        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], '4345')

    @inlineCallbacks
    def test_delivery_report_delivered(self):
        url_template = "http://localhost:%s/%s/%s?%s"
        url_params = {'messageid': '4345',
                      'type': 'titi',
                      'receiver': 'tata',
                      'reply': 'id:0134231900 sub:000 dlvrd:000 submit date:1311211757 done date:1311211757 stat:DELIVRD err:000 Text:This is test 20 from',
                      'time': 'tutu',
                      'usr': 'tyty',
                      'message': 'tete',
                      'dlr-mask': '7'}
        url = url_template % (self.receive_port, self.receive_path, self.delivery_path, urllib.urlencode(url_params))

        response = yield http_request_full(
            url,
            None,
            headers={"Content-Type": ["text/xml"]},
            method='GET')
        self.assertEqual(response.code, http.OK)

        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['user_message_id'], '4345')

    @inlineCallbacks
    def test_delivery_report_delivered_not_smpp_delivery(self):
        url_template = "http://localhost:%s/%s/%s?%s"
        url_params = {'messageid': '4345',
                      'type': 'titi',
                      'receiver': 'tata',
                      'reply': 'ACK',
                      'time': 'tutu',
                      'usr': 'tyty',
                      'message': 'tete',
                      'dlr-mask': '7'}
        url = url_template % (self.receive_port, self.receive_path, self.delivery_path, urllib.urlencode(url_params))

        response = yield http_request_full(
            url,
            "",
            headers={"Content-Type": ["text/xml"]},
            method='GET')
        self.assertEqual(response.code, http.INTERNAL_SERVER_ERROR)

    @inlineCallbacks
    def test_delivery_report_failed(self):
        url_template = "http://localhost:%s/%s/%s?%s"
        url_params = {'messageid': '4345',
                      'type': 'titi',
                      'receiver': 'tata',
                      'reply': 'id:c449ab9744f47b6af1879e49e75e4f40 sub:001 dlvrd:0 submit date:0610191018',
                      'time': 'tutu',
                      'usr': 'tyty',
                      'message': 'tete',
                      'dlr-mask': '7'}
        url = url_template % (self.receive_port, self.receive_path, self.delivery_path, urllib.urlencode(url_params))

        response = yield http_request_full(
            url,
            None,
            headers={"Content-Type": ["text/xml"]},
            method='GET')
        self.assertEqual(response.code, http.OK)

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'failed')
        self.assertEqual(event['user_message_id'], '4345')

    @inlineCallbacks
    def test_delivery_report_failed_stat(self):
        url_template = "http://localhost:%s/%s/%s?%s"
        url_params = {'messageid': '4345',
                      'type': 'titi',
                      'receiver': 'tata',
                      'reply': 'id:c449ab9744f47b6af1879e49e75e4f40 sub:001 dlvrd:0 submit date:0610191018 stat:REJECTD',
                      'time': 'tutu',
                      'usr': 'tyty',
                      'message': 'tete',
                      'dlr-mask': '7'}
        url = url_template % (self.receive_port, self.receive_path, self.delivery_path, urllib.urlencode(url_params))

        response = yield http_request_full(
            url,
            None,
            headers={"Content-Type": ["text/xml"]},
            method='POST')
        self.assertEqual(response.code, http.OK)

        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['delivery_status'], 'failed')
        self.assertEqual(event['user_message_id'], '4345')
        self.assertEqual(event['failure_reason'], 'REJECTD')
