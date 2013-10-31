from urllib import urlencode

from twisted.internet.defer import inlineCallbacks
from twisted.web import http

from vumi.transports.tests.test_base import TransportTestCase
from vumi.tests.utils import get_stubbed_worker, TestResourceWorker
from vumi.message import TransportMessage

from tests.utils import MessageMaker


class TtcBfHttpTransportTestCase(TransportTestCase, MessageMaker):
    
    transport_name = 'ttc_bf'
    transport_type = 'sms'
    transport_class = TtcBfTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TtcBfHttpTransportTestCase, self).setUp()
        self.send_path = 'sendsms'
        self.send_port = 9999
        self.receive_path = 'ttc_bf'
        self.receive_port = 9998
        self.config({
            'transport_name': self.transport_name,
            'send_url': 'http://localhost:%s/%s' % (self.send_port, self.send_path),
            'receive_path': self.receive_path,
            'receive_port': self.receive_port,
        })
        self.worker = yield self.get_transport(self.config)

    def make_resource_worker(self, response=None, code=http.OK, callback=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
            (self.send_path, TestResource, (response, code, callback))
        ])
        self._workers.append(w)
        return w.startWorker()

    def get_dispached(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)

    @inlineCallbacks
    def test_sending_ok(self):
        msg = self.mkmsg_out(content="ça éprôùvè")
        
        def assert_request(request):
            self.assertEqual("+41791234567", request.args['to'][0])
            self.assertEqual("9292", request.args["from"][0])
            self.assertEqual("ça éprôùvè", request.args["text"][0])
        
        yield self.make_resource_worker(code=http.OK, callback=assert_request)
        yield self.dispatch(msg)
        [smsg] = self.get_dispached('%s.event' % self.transport_name)
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg))

    @inlineCallbacks
    def test_sending_fail_service(self):
        yield self.make_resource_worker(code=http.INTERNAL_SERVER_ERROR)
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('%s.event' % self.transport_name)
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name='ttc_bf',
                delivery_status='failed',
                failure_level='service',
                failure_code='500',
                failure_reason='',
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
        url_template = "http://localhost:%s/%s?%s"
        params = {'from': '26477',
                  'to': '4444',
                  'text': "ça éprôùvè"}
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
        self.assertEqual("ça éprôùvè", sms_in['content'])
        self.assertEqual("26477", sms_in['from_addr'])
        self.assertEqual("4444", sms_in['to_addr'])
