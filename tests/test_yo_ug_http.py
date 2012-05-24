from uuid import uuid4
from datetime import datetime
import re

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.test_base import TransportTestCase
from transports import YoUgHttpTransport
from vumi.tests.utils import (get_stubbed_worker, TestResourceWorker,
                              RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full
from vumi.message import TransportMessage, TransportEvent, TransportUserMessage


class YoUgHttpTransportTestCase(TransportTestCase):

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
            'receive_path': '/yo',
            'receive_port': 9998
        }
        self.worker = yield self.get_transport(self.config)
        self.today = datetime.utcnow().date()

    def mkmsg_fail(self, user_message_id='1',
                  failure_level='', failure_code=0,
                  failure_reason='',
                  transport_metadata={}):
        if transport_metadata is None:
            transport_metadata = {}
        return TransportEvent(
            event_id=RegexMatcher(r'^[0-9a-fA-F]{32}$'),
            event_type='delivery_report',
            delivery_status='failed',
            failure_level=failure_level,
            failure_code=failure_code,
            failure_reason=failure_reason,
            user_message_id=user_message_id,
            timestamp=UTCNearNow(),
            transport_name=self.transport_name,
            transport_metadata=transport_metadata,
            )

    def mkmsg_in(self, content='Hello World',
                 from_addr='+41791234567',
                 session_event=TransportUserMessage.SESSION_NONE,
                 message_id='abc', transport_type=None,
                 transport_metadata=None):
        if transport_type is None:
            transport_type = self.transport_type
        if transport_metadata is None:
            transport_metadata = {}
        return TransportUserMessage(
            from_addr=from_addr,
            to_addr='9292',
            group=None,
            message_id=message_id,
            transport_name=self.transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            timestamp=UTCNearNow(),
            )

    def make_resource_worker(self, msg, code=http.OK, send_id=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
            (self.send_path, TestResource, (msg, code, send_id))])
        self._workers.append(w)
        return w.startWorker()

    @inlineCallbacks
    def test01_sending_one_sms_ok(self):
        #mocked_message_id = str(uuid4())
        mocked_message = "ybs_autocreate_status%3DOK"
        #HTTP response
        yield self.make_resource_worker(mocked_message)
        #Message to transport
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('yo.event')
        self.assertEqual(self.mkmsg_delivery(user_message_id='1'),
                         TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test02_sending_one_sms_http_failure(self):
        mocked_message = "timeout"
        mocked_error = http.REQUEST_TIMEOUT

        #HTTP response
        yield self.make_resource_worker(mocked_message, mocked_error)
        yield self.dispatch(self.mkmsg_out(to_addr='+256788601462'))

        [smsg] = self.get_dispatched('yo.event')
        self.assertEqual(self.mkmsg_fail(
            failure_level='http',
            failure_code=http.REQUEST_TIMEOUT,
            failure_reason='timeout'),
                         TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test03_sending_one_sms_service_failure(self):
        mocked_message = "ybs_autocreate_status%3DERROR%26ybs_autocreate_message%3DYBS%2BAutoCreate%2BSubsystem%3A%2BAccess%2Bdenied%2Bdue%2Bto%2Bwrong%2Bauthorization%2Bcode"

        #HTTP response
        yield self.make_resource_worker(mocked_message)
        yield self.dispatch(self.mkmsg_out(to_addr='+788601462'))
        [smsg] = self.get_dispatched('yo.event')
        self.assertEqual(self.mkmsg_fail(
            failure_level='service',
            failure_code='ERROR',
            failure_reason="YBS AutoCreate Subsystem: Access denied due to wrong authorization code"),
                         TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test04_receiving_one_sms(self):
        url = "http://localhost:%s%s?sender=41791234567&code=9292&message=Hello+World" % (self.config['receive_port'],
                                         self.config['receive_path'])
        response = yield http_request_full(url, method='GET')
        [smsg] = self.get_dispatched('yo.inbound')

        self.assertEqual(response.code, http.OK)
        self.assertEqual(self.mkmsg_in(from_addr='+41791234567',
                                       content='Hello World'),
                         TransportMessage.from_json(smsg.body))

    def get_dispatched(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)


class TestResource(Resource):
    isLeaf = True

    def __init__(self, message, code=http.OK, send_id=None):
        self.message = message
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
            not ('ybsacctno' in request.args)
            ):
            return "ybs_autocreate_status=ERROR"
        else:
            return self.message
