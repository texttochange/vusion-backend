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
            self.assertEqual("myusername", request.args['username'][0])
            self.assertEqual("mypassword", request.args['password'][0])
            self.assertEqual("+41791234567", request.args['to'][0])
            self.assertEqual("9292", request.args["from"][0])
            self.assertEqual("hello world", request.args["text"][0])
            self.assertEqual("1", request.args["messageid"][0])

        yield self.make_resource_worker("0: Accepted for delivery", code=http.OK, callback=assert_request)
        yield self.dispatch(msg)
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
    def test_sending_fail_transport(self):
        yield self.make_resource_worker("something happen", code=http.OK)
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('mobtech.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name='mobtech',
                delivery_status='failed',
                failure_level='transport',
                failure_code='',
                failure_reason='something happen',
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
        self.assertEqual(response.delivered_body, 'OK')
        
        [smsg] = self.get_dispatched('mobtech.inbound')
        sms_in = TransportMessage.from_json(smsg.body)
        self.assertEqual(self.transport_name, sms_in['transport_name'])
        self.assertEqual("Hello World", sms_in['content'])
        self.assertEqual("26477", sms_in['from_addr'])
        self.assertEqual("4444", sms_in['to_addr'])

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
        
        [smsg] = self.get_dispatched('mobtech.event')
        sms_delivery = TransportMessage.from_json(smsg.body)
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                delivery_status='delivered',
                user_message_id='4345'),
            sms_delivery)

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
        
        [smsg] = self.get_dispatched('mobtech.event')
        sms_delivery = TransportMessage.from_json(smsg.body)
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                delivery_status='delivered',
                user_message_id='4345'),
            sms_delivery)

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
        
        [smsg] = self.get_dispatched('mobtech.event')
        sms_delivery = TransportMessage.from_json(smsg.body)
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name='mobtech',
                delivery_status='failed',
                failure_level='service',
                failure_code='XX',
                failure_reason='XX',
                user_message_id='4345'),
            sms_delivery)

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
        
        [smsg] = self.get_dispatched('mobtech.event')
        sms_delivery = TransportMessage.from_json(smsg.body)
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name='mobtech',
                delivery_status='failed',
                failure_level='service',
                failure_code='XX',
                failure_reason='REJECTD',
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