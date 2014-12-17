# encoding: utf-8
import re
from xml.etree import ElementTree

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource
from twisted.trial.unittest import TestCase

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import (
    VumiTestCase, MockHttpServer, RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full

from transports.movilgate.movilgate_http import (
    MovilgateHttpTransport, MovilgateXMLParser)
from transports.tests.utils import xml_compare, Reporter


class MovilgateRequestMaker:

    def mk_mt_request(self):
        return (
            '<MTRequest>'
            '<Proveedor Id="mylogin" Password="mypassword"/>'
            '<Servicio Id="2229.tigo.bo" ContentType="0" CreateSession="0"/>'
            '<Telefono msisdn="256788" IdTran="12345678"/>'
            '<Contenido>ola mundo</Contenido>'
            '</MTRequest>')

    def mk_mt_request_foreign_language(self):
        return (
            '<MTRequest>'
            '<Proveedor Id="mylogin" Password="mypassword"/>'
            '<Servicio Id="2229.tigo.bo" ContentType="0" CreateSession="0"/>'
            '<Telefono msisdn="256788" IdTran="12345678"/>'
            '<Contenido>ola España</Contenido>'
            '</MTRequest>')

    def mk_mt_response_ok(self):
        return (
            '<MTResponse>'
            '<Transaccion estado="0" IdTran="123" Fecha="2005-02-0 1 20:40:30"/>'
            '<Texto>OK : Transaccion Exitosa</Texto>'
            '</MTResponse>')

    def mk_mt_response_fail(self):
        return (
            '<MTResponse>'
            '<Transaccion estado="3" IdTran="123" Fecha="2005-02-0 1 20:40:30"/>'
            '<Texto>Fail: some reason</Texto>'
            '</MTResponse>')

    def mk_mo_request(self):
        return (
            '<MORequest>'
            '<Servicio Id="2229.tigo.bo"/>'
            '<Telefono msisdn="41791234567" IdTran="12345678"/>'
            '<Contenido>hello world</Contenido>'
            '</MORequest>')


class MovilgateParserTestCase(TestCase):

    def test_generate_mtrequest(self):
        message_dict = {
            'proveedor': {
                'id': 'mylogin',
                'password': 'mypassword'},
            'servicio': {
                'id': '2229.tigo.bo'},
            'telephono':{
                'msisdn': '256788',
                'id_tran': '12345678'
                },
            'contenido': 'ola mundo',
        }
        parser = MovilgateXMLParser()
        output = parser.build(message_dict)
        expect = MovilgateRequestMaker().mk_mt_request()
        reporter = Reporter()

        self.assertTrue(xml_compare(
            ElementTree.fromstring(output),
            ElementTree.fromstring(expect),
            reporter), reporter.tostring())
    
    def test_generate_mtrequest_foreign_language(self):
        message_dict_foreign_language = {
            'proveedor': {
                'id': 'mylogin',
                'password': 'mypassword'},
            'servicio': {
                'id': '2229.tigo.bo'},
            'telephono':{
                'msisdn': '256788',
                'id_tran': '12345678'
                },
            'contenido': u'ola España',
        }
        parser = MovilgateXMLParser()
        output = parser.build(message_dict_foreign_language)
        expect = MovilgateRequestMaker().mk_mt_request_foreign_language()
        reporter = Reporter()

        self.assertTrue(xml_compare(
            ElementTree.fromstring(output),
            ElementTree.fromstring(expect),
            reporter), reporter.tostring())


class MovilgateHttpTransportTestCase(VumiTestCase, MovilgateRequestMaker):

    @inlineCallbacks
    def setUp(self):
        self.movilgate_calls = DeferredQueue()
        self.movilgate_calls_body = []
        self.mock_movilgate = MockHttpServer(self.handle_request)
        self.mock_server_response = ''
        self.mock_server_response_code = http.OK
        self.mock_server_response_headers = {}
        yield self.mock_movilgate.start()
        self.config = {
            'url': self.mock_movilgate.url,
            'proveedor_id': 'mylogin',
            'proveedor_password': 'mypassword',
            'servicio_id': 'myservicio', 
            'receive_path': '/movilgate',
            'receive_port': 9998}
        self.tx_helper = self.add_helper(TransportHelper(MovilgateHttpTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_movilgate.stop()
        yield super(MovilgateHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.movilgate_calls.put(request)
        self.movilgate_calls_body.append(request.content.read())
        request.setResponseCode(self.mock_server_response_code)
        for key, value in self.mock_server_response_headers.iteritems():
            request.setHeader(key, value)
        return self.mock_server_response

    @inlineCallbacks
    def test_oubound_ok_shortnumber(self):
        self.mock_server_response = self.mk_mt_response_ok()
        self.mock_server_response_code = http.OK
        self.mock_server_response_headers = {'X-Movilgate-Carrier': '2229.tigo.bo'}
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr='2229')

        ## assert the request
        req = yield self.movilgate_calls.get()
        headers = dict(req.requestHeaders.getAllRawHeaders())
        self.assertEqual(headers['Content-Type'], ['text/xml; charset=UTF-8'])
        body = self.movilgate_calls_body.pop()
        encoded_msg = ElementTree.fromstring(body)
        self.assertEqual(encoded_msg.find('Servicio').attrib['Id'], "")
        self.assertEqual(encoded_msg.find('Servicio').attrib['ShortNumber'], "2229")
        idTran = re.compile("^[0-9]{5,}$");
        self.assertTrue(idTran.match(encoded_msg.find('Telefono').attrib['IdTran']))

        ## assert the event
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_fail_shortnumber_unknown(self):
        self.mock_server_response = ''
        self.mock_server_response_code = http.INTERNAL_SERVER_ERROR
        self.mock_server_response_headers = {'X-Movilgate-Status': 'ERROR_NO_CARRIERDETECT'}

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr='2229')

        ## wait for the request to arrive
        req = yield self.movilgate_calls.get()

        ## assert the event
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "SERVICE ERROR 500 - ERROR_NO_CARRIERDETECT")

    @inlineCallbacks
    def test_outbound_ok(self):
        self.mock_server_response = self.mk_mt_response_ok()
        self.mock_server_response_code = http.OK
        self.mock_server_response_headers = {'X-Movilgate-Carrier': '2229.tigo.bo'}

        transport_metadata = {'telefono_id_tran': '12345678', 'servicio_id': '2229.tigo.bo'}        
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr='2229', transport_metadata=transport_metadata)

        ## assert the http request
        req = yield self.movilgate_calls.get()        
        headers = dict(req.requestHeaders.getAllRawHeaders())
        self.assertEqual(headers['Content-Type'], ['text/xml; charset=UTF-8'])
        body = self.movilgate_calls_body.pop()
        encoded_msg = ElementTree.fromstring(body)
        self.assertEqual(encoded_msg.find('Servicio').attrib['Id'],"2229.tigo.bo")
        self.assertTrue('ShortNumber' not in encoded_msg.find('Servicio').attrib)
        self.assertEqual(encoded_msg.find('Telefono').attrib['IdTran'],"12345678") 

        ## assert the event
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_ok_message_with_accent(self):
        self.mock_server_response = self.mk_mt_response_ok()
        self.mock_server_response_code = http.OK
        self.mock_server_response_headers = {'X-Movilgate-Carrier': '2229.tigo.bo'}

        content = u'für me'  
        transport_metadata = {'telefono_id_tran': '12345678', 'servicio_id': '2229.tigo.bo'}        
        yield self.tx_helper.make_dispatch_outbound(
            content, message_id='1', from_addr='2229', transport_metadata=transport_metadata)

        ## assert http request
        req = yield self.movilgate_calls.get()        
        headers = dict(req.requestHeaders.getAllRawHeaders())
        self.assertEqual(headers['Content-Type'], ['text/xml; charset=UTF-8'])
        body = self.movilgate_calls_body.pop()
        encoded_msg = ElementTree.fromstring(body)
        self.assertTrue(isinstance(encoded_msg.find('Contenido').text, unicode))
        self.assertEqual(content, encoded_msg.find('Contenido').text)

         ## assert the event
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_ok_servicioid_missing(self):
        self.mock_server_response = self.mk_mt_response_ok()
        self.mock_server_response_code = http.OK
        self.mock_server_response_headers = {'X-Movilgate-Carrier': '2229.tigo.bo'}
        transport_metadata = {'telefono_id_tran': '12345678'}
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr='2229', transport_metadata=transport_metadata)

        req = yield self.movilgate_calls.get()
        body = self.movilgate_calls_body.pop()
        encoded_msg = ElementTree.fromstring(body)
        self.assertEqual(encoded_msg.find('Servicio').attrib['Id'], "")
        self.assertEqual(encoded_msg.find('Servicio').attrib['ShortNumber'], "2229")
        idTran = re.compile("^[0-9]{5,}$");
        self.assertTrue(idTran.match(encoded_msg.find('Telefono').attrib['IdTran']))

        ## assert the event
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_ok_idtran_missing(self):
        self.mock_server_response = self.mk_mt_response_ok()
        self.mock_server_response_code = http.OK
        self.mock_server_response_headers = {'X-Movilgate-Carrier': '2229.tigo.bo'}
        transport_metadata = {'servicio_id': '2229.tigo.bo'}
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr='2229', transport_metadata=transport_metadata)

        req = yield self.movilgate_calls.get()
        body = self.movilgate_calls_body.pop()
        encoded_msg = ElementTree.fromstring(body)
        self.assertEqual(encoded_msg.find('Servicio').attrib['Id'], "2229.tigo.bo")
        self.assertTrue('ShortNumber' not in encoded_msg.find('Servicio').attrib)
        self.assertEqual(encoded_msg.find('Telefono').attrib['IdTran'], "0")

        ## assert the event
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_fail_service(self):
        self.mock_server_response = self.mk_mt_response_fail()
        self.mock_server_response_code = http.OK
        self.mock_server_response_headers = {'X-Movilgate-Carrier': '2229.tigo.bo'}        
        transport_metadata = {'telefono_id_tran': '12345678', 'servicio_id': '2229.tigo.bo'}
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr='2229', transport_metadata=transport_metadata)

        ## assert the event
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "SERVICE ERROR 3 - Fail: some reason")


    @inlineCallbacks
    def test_outbound_fail_transport(self):
        yield self.mock_movilgate.stop()        
        transport_metadata = {'telefono_id_tran': '12345678', 'servicio_id': '2229.tigo.bo'}
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1', from_addr='2229', transport_metadata=transport_metadata)

        ## assert the event
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "TRANSPORT ERROR Connection refused")

    @inlineCallbacks
    def test_inbound(self):
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, data=self.mk_mo_request())
        self.assertEqual(response.code, http.OK)

        [user_msg] = yield self.tx_helper.get_dispatched_inbound()        
        self.assertEqual('hello world', user_msg['content'])
        self.assertEqual('41791234567', user_msg['from_addr'])
        self.assertEqual('2229', user_msg['to_addr'])
        self.assertEqual('12345678', user_msg['transport_metadata']['telefono_id_tran'])
        self.assertEqual('2229.tigo.bo', user_msg['transport_metadata']['servicio_id'])

    @inlineCallbacks
    def test_inbound_ping(self):
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, data='\n')
        self.assertEqual(response.code, http.OK)

        user_msgs = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual(len(user_msgs), 0)

    @inlineCallbacks
    def test_inbound_fail(self):
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, data=' something is wrong')
        self.assertEqual(response.code, http.INTERNAL_SERVER_ERROR)
        user_msgs = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual(len(user_msgs), 0)

    @inlineCallbacks
    def test_receiving_ping(self):
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, data='')
        self.assertEqual(200, response.code)
