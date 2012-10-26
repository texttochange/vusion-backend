from uuid import uuid4
from datetime import datetime
import re
from string import Template

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.test_base import TransportTestCase
from transports import YoUgHttpTransport
from vumi.tests.utils import (get_stubbed_worker, TestResourceWorker,
                              RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full
from vumi.message import TransportMessage, TransportEvent, TransportUserMessage

from tests.utils import MessageMaker


class YoUgHttpTransportTestCase(MessageMaker, TransportTestCase):

    transport_name = 'yo'
    transport_type = 'sms'
    transport_class = YoUgHttpTransport

    @inlineCallbacks
    def setUp(self):
        yield super(YoUgHttpTransportTestCase, self).setUp()
        self.send_path = '/sendsms'
        self.send_port = 9999
        self.config = {
            'transport_name': 'yo',
            'url': 'http://localhost:%s%s' % (self.send_port, self.send_path),
            'ybsacctno': 'ybsacctno',
            'password': 'password',
            'default_origin': '8282',
            'receive_path': '/yo',
            'receive_port': 9998}
        self.worker = yield self.get_transport(self.config)
        self.today = datetime.utcnow().date()

    def make_resource_worker(self, response, code=http.OK, send_id=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
            (self.send_path, TestResource, ( response, code, send_id))])
        self._workers.append(w)
        return w.startWorker()

    @inlineCallbacks
    def test_sending_one_sms_ok(self):
        #mocked_message_id = str(uuid4())
        mocked_message = "ybs_autocreate_status%3DOK"
        #HTTP response
        yield self.make_resource_worker(mocked_message)
        #Message to transport
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('yo.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_one_sms_customized_id(self):
        required_args = {'origin': 'myid'}
        mocked_response = "ybs_autocreate_status%3DOK"
        #HTTP response
        yield self.make_resource_worker(mocked_response, send_id='myid')
        #Message to transport
        yield self.dispatch(
            self.mkmsg_out(transport_metadata={'customized_id': 'myid'}))
        [smsg] = self.get_dispatched('yo.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_one_sms_http_failure(self):
        mocked_message = "timeout"
        mocked_error = http.REQUEST_TIMEOUT

        #HTTP response
        yield self.make_resource_worker(mocked_message, mocked_error)
        yield self.dispatch(self.mkmsg_out(to_addr='+256788601462'))

        [smsg] = self.get_dispatched('yo.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                sent_message_id='1',
                delivery_status='failed',
                failure_level='http',
                failure_code=http.REQUEST_TIMEOUT,
                failure_reason='timeout'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_one_sms_service_failure(self):
        mocked_message = ("ybs_autocreate_status%3DERROR%26"
                          "ybs_autocreate_message%3DYBS%2BAutoCreate%2B"
                          "Subsystem%3A%2BAccess%2Bdenied%2Bdue%2Bto%2B"
                          "wrong%2Bauthorization%2Bcode")

        #HTTP response
        yield self.make_resource_worker(mocked_message)
        yield self.dispatch(self.mkmsg_out(to_addr='+788601462'))
        [smsg] = self.get_dispatched('yo.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                sent_message_id='1',
                delivery_status='failed',
                failure_level='service',
                failure_code='ERROR',
                failure_reason="YBS AutoCreate Subsystem: Access denied due to wrong authorization code"),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_receiving_one_sms(self):
        url = "http://localhost:%s%s?sender=41791234567&code=9292&message=Hello+World" % (
            self.config['receive_port'], self.config['receive_path'])
        response = yield http_request_full(url, method='GET')
        [smsg] = self.get_dispatched('yo.inbound')

        self.assertEqual(response.code, http.OK)
        self.assertEqual('Hello World',
                         TransportMessage.from_json(smsg.body)['content'])
        self.assertEqual('+41791234567',
                         TransportMessage.from_json(smsg.body)['from_addr'])
        self.assertEqual('9292',
                         TransportMessage.from_json(smsg.body)['to_addr'])

    @inlineCallbacks
    def test_receiving_one_sms_phone_with_plus(self):
        t = Template("http://localhost:$port$path?sender=%2B41791234567&code=9292&message=Hello+World")
        url = t.substitute(port=self.config['receive_port'],
                           path=self.config['receive_path'])
        response = yield http_request_full(url, method='GET')
        [smsg] = self.get_dispatched('yo.inbound')

        self.assertEqual(response.code, http.OK)
        self.assertEqual('Hello World',
                         TransportMessage.from_json(smsg.body)['content'])
        self.assertEqual('+41791234567',
                         TransportMessage.from_json(smsg.body)['from_addr'])
        self.assertEqual('9292',
                         TransportMessage.from_json(smsg.body)['to_addr'])

    def get_dispatched(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)


class TestResource(Resource):
    isLeaf = True

    def __init__(self, response, code=http.OK, send_id=None):
        self.response = response
        self.code = code
        self.send_id = send_id

    def render_GET(self, request):
        regex = re.compile('^(\+|00|0)')
        request.setResponseCode(self.code)
        if (not ('destinations' in request.args) or
                regex.match(request.args['destinations'][0]) or
                not ('origin' in request.args) or
                not ('password' in request.args) or
                not ('sms_content' in request.args) or
                not ('ybsacctno' in request.args) or
                (self.send_id is not None and self.send_id != request.args['origin'][0])):
            return "ybs_autocreate_status=ERROR"
        else:
            return self.response
