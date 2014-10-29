# encoding: UTF-8

from uuid import uuid4
from datetime import datetime

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource
from twisted.trial.unittest import TestCase


#from vumi.transports.tests.test_base import TransportTestCas
from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import (
    VumiTestCase, MockHttpServer, RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full
from vumi.message import TransportMessage, TransportEvent, TransportUserMessage

from tests.utils import ObjectMaker
from transports.cm_nl.cm_nl_http import CmTransport, CMXMLParser


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


class CmTransportTestCase(VumiTestCase):

    #transport_name = 'cm'
    #transport_type = 'sms'
    #transport_class = CmTransport

    cm_incomming_template = (
        'http://localhost:%s%s?recipient=0041791234567&'
        'operator=MTN&originator=9292&message=Hello+World')

    @inlineCallbacks
    def setUp(self):
        self.cm_calls = DeferredQueue()
        self.mock_cm = MockHttpServer(self.handle_request)
        self.mock_server_response = ''
        self.mock_server_response_code = http.OK
        yield self.mock_cm.start()
        
        #yield super(CmTransportTestCase, self).setUp()
        #self.send_path = '/sendsms'
        #self.send_port = 9999
        self.config = {
            #'url': 'http://localhost:%s%s' % (self.send_port, self.send_path),
            'url': self.mock_cm.url,
            'login': 'login',
            'password': 'password',
            'customer_id': '3454',
            'receive_path': '/cm',
            'receive_port': 9998,
            'minimum_number_of_message_part': '1',
            'maximum_number_of_message_part': '3'
        }
        self.tx_helper = self.add_helper(
            TransportHelper(CmTransport))
        #self.worker = yield self.get_transport(self.config)
        self.transport = yield self.tx_helper.get_transport(self.config)
        #self.transport_url = self.transport.get_transport_url()
        self.today = datetime.utcnow().date()

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_cm.stop()
        yield super(CmTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.cm_calls.put(request)
        request.setResponseCode(self.mock_server_response_code)
        return self.mock_server_response

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

    #def make_resource_worker(self, msg, code=http.OK, send_id=None):
        #w = get_stubbed_worker(TestResourceWorker, {})
        #w.set_resources([
            #(self.send_path, TestResource, (msg, code, send_id))])
        #self._workers.append(w)
        #return w.startWorker()

    @inlineCallbacks
    def test_outbound_ok(self):
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", to_addr="2561111111", message_id='1')
        req = yield self.cm_calls.get()
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)        
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'POST')
        self.assertEqual(req.args, {})
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['user_message_id'], '1')

    @inlineCallbacks
    def test_outbound_http_failure(self):
        self.mock_server_response = "timeout"
        self.mock_server_response_code = http.REQUEST_TIMEOUT

        yield self.tx_helper.make_dispatch_outbound("hello world")
        [fail] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual('http', fail['failure_level'])
        self.assertEqual(http.REQUEST_TIMEOUT, fail['failure_code'])
        self.assertEqual('timeout', fail['failure_reason'])


    @inlineCallbacks
    def test_outbound_service_failure(self):
        self.mock_server_response = "Error: ERROR Unknown error"
        self.mock_server_response_code = http.OK
        
        yield self.tx_helper.make_dispatch_outbound("Hello world")
        [fail] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual('service', fail['failure_level'])
        self.assertEqual("Error: ERROR Unknown error", fail['failure_reason'])


    @inlineCallbacks
    def test_inbound(self):
        url = (self.cm_incomming_template
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, method='GET')
        [smsg] = self.tx_helper.get_dispatched_inbound()

        self.assertEqual(http.OK, response.code)
        self.assertEqual('Hello World', smsg['content'])
        self.assertEqual('9292', smsg['to_addr'])
        self.assertEqual('+41791234567',smsg['from_addr'])
