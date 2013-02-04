from uuid import uuid4
from datetime import datetime
import re
from string import Template

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.test_base import TransportTestCase
from transports import MobivateHttpTransport
from vumi.tests.utils import (get_stubbed_worker, TestResourceWorker,
                              RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full
from vumi.message import TransportMessage, TransportEvent, TransportUserMessage

from tests.utils import MessageMaker


class MobivateHttpTransportTestCase(MessageMaker, TransportTestCase):
    
    transport_name = 'mobivate'
    transport_type = 'sms'
    transport_class = MobivateHttpTransport
    
    @inlineCallbacks
    def setUp(self):
        yield super(MobivateHttpTransportTestCase, self).setUp()
        self.send_path = '/sendsms'
        self.send_port = 9999
        self.config ={
            'transport_name': self.transport_name,
            'url': 'http://localhost:%s%s' % (self.send_port, self.send_path),
            'user_name': 'username',
            'password': 'password',
            'default_origin': '55882',
            'receive_path': '/mobivate',
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
        mocked_message = "ybs_autocreate_status%3DOK"
        yield self.make_resource_worker(mocked_message)
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('mobivate.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

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
        if (not ('recipient' in request.args) or
                regex.match(request.args['recipient'][0]) or
                not ('originator' in request.args) or
                not ('user_name' in request.args) or
                not ('password' in request.args) or
                not ('message_text' in request.args) or
                (self.send_id is not None and self.send_id != request.args['originator'][0])):
            return "ERROR"
        else:
            return self.response
