
from datetime import datetime
from xml.etree import ElementTree

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource
from twisted.trial.unittest import TestCase
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol

from vumi.transports.tests.test_base import TransportTestCase
from vumi.utils import http_request_full
from vumi.tests.utils import get_stubbed_worker, TestResourceWorker
from vumi.message import TransportMessage

from tests.utils import MessageMaker
from transports.push_tz_http import PushTransport, PushXMLParser
from transports.tests.utils import xml_compare, Reporter

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
            'msisdn="41791234567" '
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
            'default_origin': '3939',
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
            'default_origin': '15001',
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

        self.assertEqual(response.delivered_body, 
                         '<?xml version="1.0" encoding="UTF-8"?><sms-response version="1.0"/>')
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
