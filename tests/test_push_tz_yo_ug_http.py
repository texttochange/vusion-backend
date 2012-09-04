from uuid import uuid4
from datetime import datetime
import xml.etree.cElementTree as ET

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from tests.utils import MessageMaker

from vumi.transports.tests.test_base import TransportTestCase
from transports import PushYoTransport
from vumi.tests.utils import (get_stubbed_worker, TestResourceWorker,
                              RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full
from vumi.message import TransportMessage, TransportEvent, TransportUserMessage

from tests.utils import MessageMaker

class PushYoTransportTestCase(TransportTestCase, MessageMaker):

    transport_name = 'push'
    transport_type = 'sms'
    transport_class = PushYoTransport

    service_id = 9483
    password = 'password'
    channel = 0
    receipt = 'Y'
    max_segments = 0

    push_request = ('<?xml version="1.0"?>'
                    '<methodCall>'
                    '<methodName>EAPIGateway.SendSMS</methodName>'
                    '<params><param><value><struct>'
                    '<member><name>Service</name>'
                    '<value><int>%s</int></value></member>'
                    '<member><name>Password</name>'
                    '<value><string>%s</string></value></member>'
                    '<member><name>Channel</name>'
                    '<value><int>%s</int></value></member>'
                    '<member><name>Receipt</name>'
                    '<value><string>%s</string></value></member>'
                    '<member><name>Numbers</name>'
                    '<value><string>+41791234567</string></value></member>'
                    '<member><name>Source</name>'
                    '<value><string>9292</string></value></member>'
                    '<member><name>SMSText</name>'
                    '<value><string>hello world</string></value></member>'
                    '<member><name>MaxSegments</name>'
                    '<value><int>%s</int></value></member>'
                    '</struct></value></param></params>'
                    '</methodCall>' % (service_id, password, channel,
                                       receipt, max_segments))

    push_response = ('<?xml version="1.0" encoding="windows-1251"?>'
                     '<methodResponse><params><param>'
                     '<value><struct>'
                     '<member>'
                     '<name>Identifier</name>'
                     '<value><string>00815B71</string></value>'
                     '</member>'
                     '</struct></value>'
                     '</param></params></methodResponse>')

    @inlineCallbacks
    def setUp(self):
        yield super(PushYoTransportTestCase, self).setUp()
        self.send_path = '/sendsms'
        self.send_port = 9999
        self.config = {
            'transport_name': 'push',
            'url': 'http://localhost:%s%s' % (self.send_port, self.send_path),
            'service_id': self.service_id,
            'password': self.password,
            'channel': self.channel,
            'receipt': self.receipt,
            'max_segments': self.max_segments,
            'receive_path': '/yo',
            'receive_port': 9998
        }
        self.worker = yield self.get_transport(self.config)
        self.today = datetime.utcnow().date()

    def make_resource_worker(self, request, message, code=http.OK, send_id=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
            (self.send_path, TestResource, (request, message, code, send_id))])
        self._workers.append(w)
        return w.startWorker()

    @inlineCallbacks
    def test_sending_one_sms_ok(self):
        #mocked_message_id = str(uuid4())
        #mocked_message = self.push_response
        #HTTP response
        yield self.make_resource_worker(self.push_request, self.push_response)
        #Message to transport
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('push.event')
        self.assertEqual(self.mkmsg_delivery(user_message_id='1',
                                             transport_metadata={'Identifier':'00815B71'}),
                         TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_one_sms_http_failure(self):
        mocked_message = "gztzzz"
        mocked_error = http.INTERNAL_SERVER_ERROR     

        #HTTP response
        yield self.make_resource_worker(self.push_request, 
                                        mocked_message, mocked_error)
        yield self.dispatch(self.mkmsg_out(to_addr='256788601462'))

        [smsg] = self.get_dispatched('push.event')
        self.assertEqual(self.mkmsg_transport_fail(
            failure_level='service',
            failure_code=0,
            failure_reason='Failure during xml parsing'),
                         TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_receiving_one_sms(self):
        url = "http://localhost:%s%s?sender=0041791234567&code=9292&message=Hello+World" % (self.config['receive_port'],
                                         self.config['receive_path'])
        response = yield http_request_full(url, method='GET')
        [smsg] = self.get_dispatched('push.inbound')

        self.assertEqual(response.code, http.OK)
        self.assertEqual('Hello World',
                         TransportMessage.from_json(smsg.body)['content'])
        self.assertEqual('9292',
                         TransportMessage.from_json(smsg.body)['to_addr'])
        self.assertEqual('+41791234567',
                         TransportMessage.from_json(smsg.body)['from_addr'])

    @inlineCallbacks
    def test_receiving_one_sms_incomplete_number(self):
        url = "http://localhost:%s%s?sender=41791234567&code=9292&message=Hello+World" % (self.config['receive_port'],
                                         self.config['receive_path'])
        response = yield http_request_full(url, method='GET')
        [smsg] = self.get_dispatched('push.inbound')

        self.assertEqual(response.code, http.OK)
        self.assertEqual('Hello World',
                         TransportMessage.from_json(smsg.body)['content'])
        self.assertEqual('9292',
                         TransportMessage.from_json(smsg.body)['to_addr'])
        self.assertEqual('+41791234567',
                         TransportMessage.from_json(smsg.body)['from_addr'])

    def get_dispatched(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)


class TestResource(Resource):
    isLeaf = True
    
    push_response_err = ('<?xml version="1.0" encoding="windows-1251"?>'
                     '<methodResponse><params><param>'
                     '<value><struct>'
                     '<member>'
                     '<name>Error</name>'
                     '<value><string>%s</string></value>'
                     '</member>'
                     '</struct></value>'
                     '</param></params></methodResponse>')

    def __init__(self, request, response, code=http.OK, send_id=None):
        self.request = request
        self.response = response
        self.code = code
        self.send_id = send_id

    def render_POST(self, request):
        request.setResponseCode(self.code)
        if self.code!=http.OK :
            return
        expect = ET.fromstring(self.request)
        request_content = request.content.read().replace("\n","")
        content = ET.fromstring(request_content)
        report = Reporter()
        if xml_compare(expect, content, report):
            return self.response
        else:
            msg = (self.push_response_err % report.tostring()) 
            return msg


def xml_compare(x1, x2, reporter=None):
    if x1.tag != x2.tag:
        if reporter:
            reporter('Tags do not match: %s and %s' % (x1.tag, x2.tag))
        return False
    for name, value in x1.attrib.items():
        if x2.attrib.get(name) != value:
            if reporter:
                reporter('Attributes do not match: %s=%r, %s=%r'
                         % (name, value, name, x2.attrib.get(name)))
            return False
    for name in x2.attrib:
        if name not in x1.attrib:
            if reporter:
                reporter('x2 has an attribute x1 is missing: %s'
                         % name)
            return False
    if not text_compare(x1.text, x2.text):
        if reporter:
            reporter('text: %r != %r' % (x1.text, x2.text))
        return False
    if not text_compare(x1.tail, x2.tail):
        if reporter:
            reporter('tail: %r != %r' % (x1.tail, x2.tail))
        return False
    cl1 = x1.getchildren()
    cl2 = x2.getchildren()
    if (len(cl1)>0 and cl1[0] and cl1[0].getchildren()[0].tag=='name'):
        cl1.sort(key=lambda x: x.getchildren()[0].text)
        cl2.sort(key=lambda x: x.getchildren()[0].text)
    if len(cl1) != len(cl2):
        if reporter:
            reporter('children length differs, %i != %i'
                     % (len(cl1), len(cl2)))
        return False
    i = 0
    for c1, c2 in zip(cl1, cl2):
        i += 1
        if not xml_compare(c1, c2, reporter=reporter):
            if reporter:
                if (len(c1) and c1[0].tag == 'name'):
                    reporter('children %i do not match: %s'
                             % (i, c1[0].text))
                else:
                    reporter('children %i do not match: %s'
                             % (i, c1.tag))
            return False
    return True


def text_compare(t1, t2):
    if not t1 and not t2:
        return True
    if t1 == '*' or t2 == '*':
        return True
    return (t1 or '').strip() == (t2 or '').strip()


class Reporter:
    def __init__(self):
        self.report = []
        
    def __call__(self, message):
        self.report.insert(0, message)
        
    def tostring(self):
        summary = ""
        for message in self.report:
            summary = summary + message + ".\n"
        return summary
        
