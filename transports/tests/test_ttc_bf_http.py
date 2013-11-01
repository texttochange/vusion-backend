# encoding: utf-8
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.test_base import TransportTestCase
from vumi.tests.utils import get_stubbed_worker, TestResourceWorker
from vumi.message import TransportMessage
from vumi.utils import http_request_full

from tests.utils import MessageMaker

from transports import TtcBfHttpTransport


class TtcBfHttpTransportTestCase(MessageMaker, TransportTestCase):
    
    transport_name = 'ttc_bf'
    transport_type = 'sms'
    transport_class = TtcBfHttpTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TtcBfHttpTransportTestCase, self).setUp()
        self.send_path = 'sendsms'
        self.send_port = 9999
        self.receive_path = 'ttc_bf'
        self.receive_port = 9998
        self.config = {
            'transport_name': self.transport_name,
            'send_url': 'http://localhost:%s/%s' % (self.send_port, self.send_path),
            'receive_path': self.receive_path,
            'receive_port': self.receive_port,
            'default_shortcode': '3400'}
        self.worker = yield self.get_transport(self.config)

    def make_resource_worker(self, response='', code=http.OK, callback=None):
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
        msg = self.mkmsg_out(content=u"ça éprôùvè")
        
        def assert_request(request):
            self.assertEqual("+41791234567", request.args['to_addr'][0])
            self.assertEqual("3400", request.args["from_addr"][0])
            self.assertEqual("ça éprôùvè", request.args["message"][0])
        
        yield self.make_resource_worker(code=http.OK, callback=assert_request)
        yield self.dispatch(msg)
        [smsg] = self.get_dispatched('%s.event' % self.transport_name)
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_fail_service(self):
        yield self.make_resource_worker("an error occured", code=http.INTERNAL_SERVER_ERROR)
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('%s.event' % self.transport_name)
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                delivery_status='failed',
                failure_level='service',
                failure_code='500',
                failure_reason='an error occured',
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_fail_http(self):
        yield self.make_resource_worker("", code=http.INTERNAL_SERVER_ERROR)
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('%s.event' % self.transport_name)
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                delivery_status='failed',
                failure_level='http',
                failure_code='500',
                failure_reason='',
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_receiving_sms(self):
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
        
        [smsg] = self.get_dispatched('%s.inbound' % self.transport_name)
        sms_in = TransportMessage.from_json(smsg.body)
        self.assertEqual(self.transport_name, sms_in['transport_name'])
        self.assertEqual(u"ça éprôùvè", sms_in['content'])
        self.assertEqual("26477", sms_in['from_addr'])
        self.assertEqual("3400", sms_in['to_addr'])


class TestResource(Resource):
    isLeaf = True
    
    def __init__(self, response, code=http.OK, callback=None):
        self.response = response
        self.code = code
        self.callback = callback

    def render_GET(self, request):
        if self.callback is not None:
            self.callback(request)
        request.setResponseCode(self.code)
        return self.response