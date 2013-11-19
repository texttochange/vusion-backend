#encoding: UTF-8
from uuid import uuid4
from datetime import datetime

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource
from twisted.trial.unittest import TestCase

from vumi.transports.tests.test_base import TransportTestCase
from vumi.tests.utils import (get_stubbed_worker, TestResourceWorker,
                              RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full
from vumi.message import TransportMessage, TransportEvent, TransportUserMessage

from tests.utils import ObjectMaker
from transports.cm_nl_http import CmTransport, CMXMLParser


class CmParserTestCase(TestCase, ObjectMaker):
    
    def setUp(self):
        config = {
            'minimum_number_of_message_part': '1',
            'maximum_number_of_message_part': '3'
        }
        self.parser = CMXMLParser(config)
    
    def mk_mt_request(self, from_addr="317777", to_addr="2567777", content="Hello World"):
        return (
            '<MESSAGES>'
            '<CUSTOMER ID="myID" />'
            '<USER LOGIN="myLogin" PASSWORD="myPassword" />'
            '<MSG>'
            '<FROM>%s</FROM>'
            '<BODY HEADER="" TYPE="TEXT">%s</BODY>'
            '<TO>%s</TO>'
            '<MINIMUMNUMBEROFMESSAGEPARTS>1</MINIMUMNUMBEROFMESSAGEPARTS>'
            '<MAXIMUMNUMBEROFMESSAGEPARTS>3</MAXIMUMNUMBEROFMESSAGEPARTS>'
            '</MSG>'
            '</MESSAGES>' % (from_addr, content, to_addr))
    
    def mk_mt_request_unicode(self, from_addr="317777", to_addr="2567777", content="Hello World"):
        return (
            '<MESSAGES>'
            '<CUSTOMER ID="myID" />'
            '<USER LOGIN="myLogin" PASSWORD="myPassword" />'
            '<MSG>'
            '<FROM>%s</FROM>'
            '<DCS>8</DCS>'
            '<BODY HEADER="" TYPE="TEXT">%s</BODY>'
            '<TO>%s</TO>'
            '<MINIMUMNUMBEROFMESSAGEPARTS>1</MINIMUMNUMBEROFMESSAGEPARTS>'
            '<MAXIMUMNUMBEROFMESSAGEPARTS>6</MAXIMUMNUMBEROFMESSAGEPARTS>'
            '</MSG>'
            '</MESSAGES>' % (from_addr, content, to_addr))    
    
    def test_generate_mtrequest(self):
        content = self.mk_content(154)
        expected = self.mk_mt_request(content=content)
        message_dict = {
            'customer_id': 'myID',
            'login': 'myLogin',
            'password': 'myPassword',
            'from_addr': '317777',
            'to_addr': '2567777',
            'content': content}

        output = self.parser.build(message_dict)

        self.assertEqual(expected, output)
    
    ## TODO require a GSM 03.38 detector
    #def test_generate_mtrequest_ascii(self):
        #content = 'é'
        #expected = self.mk_mt_request(content=content)
        #message_dict = {
            #'customer_id': 'myID',
            #'login': 'myLogin',
            #'password': 'myPassword',
            #'from_addr': '317777',
            #'to_addr': '2567777',
            #'content': content}

        #output = self.parser.build(message_dict)

        #self.assertEqual(expected, output)

    ## TODO require a GSM 03.38 detector
    #def test_generate_mtrequest_unicode(self):
        #content = '爱'  #love in simplified chineese  
        #expected = self.mk_mt_request_unicode(content=content)
        #message_dict = {
            #'customer_id': 'myID',
            #'login': 'myLogin',
            #'password': 'myPassword',
            #'from_addr': '317777',
            #'to_addr': '2567777',
            #'content': content}

        #output = self.parser.build(message_dict)

        #self.assertEqual(expected, output)


class CmTransportTestCase(TransportTestCase):

    transport_name = 'cm'
    transport_type = 'sms'
    transport_class = CmTransport

    cm_incomming_template = (
        'http://localhost:%s%s?recipient=0041791234567&'
        'operator=MTN&originator=9292&message=Hello+World')

    @inlineCallbacks
    def setUp(self):
        yield super(CmTransportTestCase, self).setUp()
        self.send_path = '/sendsms'
        self.send_port = 9999
        self.config = {
            'transport_name': 'cm',
            'url': 'http://localhost:%s%s' % (self.send_port, self.send_path),
            'login': 'login',
            'password': 'password',
            'customer_id': '3454',
            'receive_path': '/cm',
            'receive_port': 9998,
            'minimum_number_of_message_part': '1',
            'maximum_number_of_message_part': '3'
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
            transport_metadata=transport_metadata)

    def mkmsg_in(self, content='Hello World',
                 from_addr='41791234567',
                 session_event=TransportUserMessage.SESSION_NONE,
                 message_id='abc', transport_type=None,
                 transport_metadata=None):
        if transport_type is None:
            transport_type = self.transport_type
        if transport_metadata is None:
            transport_metadata = {}
        return TransportUserMessage(
            from_addr='+41791234567',
            to_addr='9292',
            group=None,
            #message_id=message_id,
            transport_name=self.transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            timestamp=UTCNearNow())

    def make_resource_worker(self, msg, code=http.OK, send_id=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
            (self.send_path, TestResource, (msg, code, send_id))])
        self._workers.append(w)
        return w.startWorker()

    @inlineCallbacks
    def test_sending_one_sms_ok(self):
        #mocked_message_id = str(uuid4())
        mocked_message = ""
        #HTTP response
        yield self.make_resource_worker(mocked_message)
        #Message to transport
        yield self.dispatch(self.mkmsg_out(to_addr='+41791234567'))
        [smsg] = self.get_dispatched('cm.event')
        self.assertEqual(self.mkmsg_delivery(user_message_id='1'),
                         TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_one_sms_http_failure(self):
        mocked_message = "timeout"
        mocked_error = http.REQUEST_TIMEOUT

        #HTTP response
        yield self.make_resource_worker(mocked_message, mocked_error)
        yield self.dispatch(self.mkmsg_out(to_addr='256788601462'))

        [smsg] = self.get_dispatched('cm.event')
        self.assertEqual(
            self.mkmsg_fail(failure_level='http',
                            failure_code=http.REQUEST_TIMEOUT,
                            failure_reason='timeout'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_one_sms_service_failure(self):
        mocked_message = "Error: ERROR Unknown error"

        #HTTP response
        yield self.make_resource_worker(mocked_message)
        yield self.dispatch(self.mkmsg_out(to_addr='788601462'))
        [smsg] = self.get_dispatched('cm.event')
        self.assertEqual(
            self.mkmsg_fail(failure_level='service',
                            failure_reason="Error: ERROR Unknown error"),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_receiving_one_sms(self):
        url = (self.cm_incomming_template
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, method='GET')
        [smsg] = self.get_dispatched('cm.inbound')

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

    def __init__(self, message, code=http.OK, send_id=None):
        self.message = message
        self.code = code
        self.send_id = send_id

    def render_GET(self, request):
        request.setResponseCode(self.code)
        return self.message

    def render_POST(self, request):
        request.setResponseCode(self.code)
        return self.message
