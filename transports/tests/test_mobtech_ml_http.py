import urllib

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.test_base import TransportTestCase
from vumi.tests.utils import get_stubbed_worker, TestResourceWorker
from vumi.message import TransportMessage
from vumi.utils import http_request_full

from tests.utils import MessageMaker
from transports import MobtechMlHttpTransport


class MobtechMlHttpTransportTestCase(MessageMaker, TransportTestCase):
    
    transport_name = 'mobtech'
    transport_type = 'sms'
    transport_class = MobtechMlHttpTransport
    
    @inlineCallbacks
    def setUp(self):
        yield super(MobtechMlHttpTransportTestCase, self).setUp()
        self.send_path = '/sendsms'
        self.send_port = 9999
        self.domain = "localhost"
        self.receive_path = 'mobtech'
        self.mo_path = 'mo'
        self.delivery_path = 'delivery'
        self.receive_port = 9998
        self.config = {
            'transport_name': self.transport_name,
            'url': 'http://localhost:%s%s' % (self.send_port, self.send_path),
            'user_name': 'username',
            'password': 'password',
            'mt_response_regex': r'^(?P<status>\d+): +(?P<message>[\w\s]+)',
            'domain': self.domain,
            'receive_path': self.receive_path,
            'mo_receive_path': self.mo_path,
            'delivery_receive_path': self.delivery_path,
            'receive_port': self.receive_port,
            'delivery_url_params': 'type=%d&receiver=%p&reply=%A&time=%t&usr=%n&message=%b&dlr-mask=7'}
        self.worker = yield self.get_transport(self.config)
        
    def make_resource_worker(self, response=None, code=http.OK, callback=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
            (self.send_path, TestResource, (response, code, callback))
        ])
        self._workers.append(w)
        return w.startWorker()
    
    def get_dispatched(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)

    @inlineCallbacks
    def test_sending_ok(self):
        msg = self.mkmsg_out()

        def assert_request(request):
            headers = dict(request.requestHeaders.getAllRawHeaders())
            self.assertEqual(headers['Content-Type'], ['application/x-www-form-urlencoded'])
            self.assertEqual(headers['Authorization'], ['Basic dXNlcm5hbWU6cGFzc3dvcmQ='])
            self.assertEqual("+41791234567", request.args['to'][0])
            self.assertEqual("9292", request.args["from"][0])
            self.assertEqual("hello world", request.args["text"][0])
            self.assertEqual(
                "http://%s:%s/%s/%s?messageid=%s&%s" % (self.domain,
                                                        self.receive_port,
                                                        self.receive_path,
                                                        self.delivery_path,
                                                        msg['message_id'],
                                                        self.config['delivery_url_params']),
                request.args["dlr-url"][0])

        yield self.make_resource_worker("0: Accepted for delivery", code=http.OK, callback=assert_request)
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('mobtech.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_fail_service(self):
        yield self.make_resource_worker("01: Message too long", code=http.OK)
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('mobtech.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name='mobtech',
                delivery_status='failed',
                failure_level='service',
                failure_code='01',
                failure_reason='Message too long',
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_fail_http(self):
        yield self.make_resource_worker("", code=http.INTERNAL_SERVER_ERROR)
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('mobtech.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name='mobtech',
                delivery_status='failed',
                failure_level='http',
                failure_code='500',
                failure_reason='',
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_receiving_sms(self):
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
        self.assertEqual(response.delivered_body, '')
        
        [smsg] = self.get_dispatched('mobtech.inbound')
        sms_in = TransportMessage.from_json(smsg.body)
        self.assertEqual(self.transport_name, sms_in['transport_name'])
        self.assertEqual("Hello World", sms_in['content'])
        self.assertEqual("26477", sms_in['from_addr'])
        self.assertEqual("4444", sms_in['to_addr'])

    @inlineCallbacks
    def test_delivery_report(self):
        url_template = "http://localhost:%s/%s/%s?messageid=4345&type=titi&receiver=tata&reply=toto&time=tutu&usr=tyty&message=tete&dlr-mask=7"
        url = url_template % (self.receive_port, self.receive_path, self.delivery_path)

        response = yield http_request_full(url, method='GET')
        self.assertEqual(response.code, http.OK)
        
        [smsg] = self.get_dispatched('mobtech.event')
        sms_delivery = TransportMessage.from_json(smsg.body)
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                delivery_status='delivered',
                user_message_id='4345'),
            sms_delivery)


class TestResource(Resource):
    isLeaf = True
    
    def __init__(self, response, code=http.OK, callback=None):
        self.response = response
        self.code = code
        self.callback = callback

    def render_POST(self, request):
        if self.callback is not None:
            self.callback(request)
        request.setResponseCode(self.code)
        return self.response
