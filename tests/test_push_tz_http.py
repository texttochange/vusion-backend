
from datetime import datetime
from xml.etree import ElementTree

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource
from twisted.trial.unittest import TestCase

from vumi.transports.tests.test_base import TransportTestCase
from vumi.utils import http_request_full
from vumi.tests.utils import get_stubbed_worker, TestResourceWorker
from vumi.message import TransportMessage

from tests.utils import MessageMaker
from transports.push_tz_http import PushTransport, PushXMLParser


class PushRequestMaker:

    def mkrequest_bulk(self):
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<bulk-request login="mylogin" password="mypassword" '
            'delivery-notification-requested="Y" '
            'ref-id="2012-09-04T12:22:02" version="1.0">'
            '<message id="1" '
            'msisdn="+41791234567" '
            'service-number="9292" '
            'validity-period="1" '
            'priority="1">'
            '<content type="text/plain">Hello World</content>'
            '</message>'
            '</bulk-request>')

    def mkresponse_bulk(self):
        return (
            '<?xml version="1.0" encoding="windows-1251"?>'
            '<bulk-response version="1.0"/>')

    def mkrequest_incomming(self):
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<sms-request version="1.0">'
            '<message id="54321" '
            'submit-date="2008-10-13 13:30:10" '
            'msisdn="+41791234567" '
            'service-number="9292" '
            'operator="operator-smpp" '
            'operator_id="100" '
            'keyword="This" '
            'message-count="1">'
            '<content type="text/plain">Hello World</content>'
            '</message>'
            '</sms-request>')


class PushParserTestCase(TestCase):

    def test_generate_bulk_request(self):
        message_dict = {
            'login': 'mylogin',
            'password': 'mypassword',
            'ref-id': '2012-09-04T12:22:02',
            'delivery-notification-requested': 'Y',
            'messages': [{
                'id': '1',
                'msisdn': '+41791234567',
                'service-number': '9292',
                'validity-period': '1',
                'priority': '1',
                'content': 'Hello World'}]
        }
        parser = PushXMLParser()

        expect = PushRequestMaker().mkrequest_bulk()

        self.assertTrue(xml_compare(
            ElementTree.fromstring(parser.build_bulk_request(message_dict)),
            ElementTree.fromstring(expect),
            Reporter()))


class PushTransportTestCase(TransportTestCase, MessageMaker):

    transport_name = 'push'
    transport_type = 'sms'
    transport_class = PushTransport
    login = "mylogin"
    password = "mypassword"

    @inlineCallbacks
    def setUp(self):
        yield super(PushTransportTestCase, self).setUp()
        self.send_path = '/sendsms'
        self.send_port = 9999
        self.config = {
            'transport_name': 'push',
            'url': 'http://localhost:%s%s' % (self.send_port, self.send_path),
            'login': self.login,
            'password': self.password,
            'delivery_notification_requested': 'Y',
            'service_number': '4949',
            'validity_period': '1',
            'priority': '1',
            'receive_path': '/yo',
            'receive_port': 9998}
        self.worker = yield self.get_transport(self.config)
        self.today = datetime.utcnow().date()
        self.maker = PushRequestMaker()

    def make_resource_worker(self, req, msg, code=http.OK, send_id=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
            (self.send_path, PushTestResource, (req, msg, code, send_id))])
        self._workers.append(w)
        return w.startWorker()

    def get_dispatched(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)

    @inlineCallbacks
    def test_sending_one_sms_ok(self):
        #HTTP response
        yield self.make_resource_worker(
            self.maker.mkrequest_bulk(),
            self.maker.mkresponse_bulk()
        )
        #Message to transport
        yield self.dispatch(self.mkmsg_out(to_addr='+41791234567'))
        [smsg] = self.get_dispatched('push.event')
        self.assertEqual(self.mkmsg_delivery(user_message_id='1'),
                         TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_receiving_one_sms(self):
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(
            url, data=self.maker.mkrequest_incomming())
        [smsg] = self.get_dispatched('push.inbound')

        self.assertEqual(response.code, http.OK)
        self.assertEqual('Hello World',
                         TransportMessage.from_json(smsg.body)['content'])
        self.assertEqual('9292',
                         TransportMessage.from_json(smsg.body)['to_addr'])
        self.assertEqual('+41791234567',
                         TransportMessage.from_json(smsg.body)['from_addr'])


class PushTestResource(Resource):
    isLeaf = True

    def __init__(self, request, response, code=http.OK, send_id=None):
        self.request = request
        self.response = response
        self.code = code
        self.send_id = send_id

    def render_POST(self, request):
        request.setResponseCode(self.code)
        if self.code != http.OK:
            return
        return self.response


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
