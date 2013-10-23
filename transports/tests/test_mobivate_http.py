import re

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.test_base import TransportTestCase
from vumi.tests.utils import (get_stubbed_worker, TestResourceWorker,
                              RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full
from vumi.message import TransportMessage, TransportEvent, TransportUserMessage

from tests.utils import MessageMaker
from transports import MobivateHttpTransport

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
        
    def make_resource_worker(self, response, code=http.OK, send_id=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
            (self.send_path, TestResource, ( response, code, send_id))])
        self._workers.append(w)
        return w.startWorker()

    def get_dispatched(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)
    
    @inlineCallbacks
    def test_sending_one_sms_ok(self):
        mocked_message = "0"
        yield self.make_resource_worker(mocked_message)
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('mobivate.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_one_sms_fail(self):
        mocked_message = "500\nSome internal issue"
        yield self.make_resource_worker(mocked_message)
        yield self.dispatch(self.mkmsg_out(to_addr="256"))
        [smsg] = self.get_dispatched('mobivate.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                delivery_status='failed',
                failure_level='service',
                failure_code="500",
                failure_reason="Some internal issue",
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_receiving_sms(self):
        params = ("ORIGINATOR=61412345678&RECIPIENT=1987654&PROVIDER=telstra"
                  "&MESSAGE_TEXT=Hello%20There!")
        url = ("http://localhost:%s%s/SMSfromMobiles?%s" %
               (self.config['receive_port'], self.config['receive_path'], params))

        response = yield http_request_full(url, method='GET')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, '0')
        
        [smsg] = self.get_dispatched('mobivate.inbound')
        sms_in = TransportMessage.from_json(smsg.body)
        self.assertEqual(self.transport_name, sms_in['transport_name'])
        self.assertEqual("Hello There!", sms_in['content'])
        self.assertEqual("61412345678", sms_in['from_addr'])
        self.assertEqual("1987654", sms_in['to_addr'])

    @inlineCallbacks
    def test_receiving_delivery_report(self):
        params = ("ORIGINATOR=61412345678&RECIPIENT=1987654&PROVIDER=telstra"
                  "&MESSAGE_TEXT=Hello%20There!&ID=939ec52e333fbf124a87845d3a5d72e1"
                  "&REFERENCE=ABC123&RESULT=1")
        url = ("http://localhost:%s%s/DeliveryReciept?%s" %
               (self.config['receive_port'], self.config['receive_path'], params))

        response = yield http_request_full(url, method='GET')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, '0')        
        
        [smsg] = self.get_dispatched('mobivate.event')
        sms_delivery = TransportMessage.from_json(smsg.body)        
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                delivery_status='delivered',
                user_message_id='ABC123'),
            sms_delivery)


class TestResource(Resource):
    isLeaf = True
    
    def __init__(self, response, code=http.OK, send_id=None):
        self.response = response
        self.code = code
        self.send_id = send_id
        
    def render_GET(self, request):
        regex = re.compile('^(\+|00|0)[0-9]*')
        request.setResponseCode(self.code)
        if (not ('RECIPIENT' in request.args) or
                regex.match(request.args['RECIPIENT'][0]) or
                not ('ORIGINATOR' in request.args) or
                not ('USER_NAME' in request.args) or
                not ('PASSWORD' in request.args) or
                not ('MESSAGE_TEXT' in request.args) or
                not ('REFERENCE' in request.args) or
                (self.send_id is not None and self.send_id != request.args['originator'][0])):
            return "501"
        else:
            return self.response
