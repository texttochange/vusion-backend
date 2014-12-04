# encoding: utf-8
import re
from xml.etree import ElementTree

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource
from twisted.trial.unittest import TestCase

from vumi.transports.tests.test_base import TransportTestCase
from vumi.tests.utils import (get_stubbed_worker, TestResourceWorker,
                              RegexMatcher, UTCNearNow)
from vumi.utils import http_request_full
from vumi.message import TransportMessage, TransportEvent, TransportUserMessage

from tests.utils import MessageMaker

from transports import MovilgateHttpTransport
from transports.movilgate_http import MovilgateXMLParser
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


class MovilgateHttpTransportTestCase(MessageMaker, TransportTestCase,
                                     MovilgateRequestMaker):
    
    transport_name = 'movilgate'
    transport_type = 'sms'
    transport_class = MovilgateHttpTransport
    
    @inlineCallbacks
    def setUp(self):
        yield super(MovilgateHttpTransportTestCase, self).setUp()
        self.send_path = '/sendsms'
        self.send_port = 9999
        self.config ={
            'transport_name': self.transport_name,
            'url': 'http://localhost:%s%s' % (self.send_port, self.send_path),
            'proveedor_id': 'mylogin',
            'proveedor_password': 'mypassword',
            'servicio_id': 'myservicio', 
            'receive_path': '/movilgate',
            'receive_port': 9998}
        self.worker = yield self.get_transport(self.config)
        
    def make_resource_worker(self, response=None, code=http.OK, callback=None, headers={}):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
            (self.send_path, TestResource, (response, code, callback, headers))
        ])
        self._workers.append(w)
        return w.startWorker()

    def get_dispatched(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)

    @inlineCallbacks
    def test_sending_mt_ok_shortnumber(self):
        def assert_request(request):              
            headers = dict(request.requestHeaders.getAllRawHeaders())
            self.assertEqual(headers['Content-Type'], ['text/xml; charset=UTF-8'])
            body = request.content.read()
            encoded_msg = ElementTree.fromstring(body)
            self.assertEqual(encoded_msg.find('Servicio').attrib['Id'], "")
            self.assertEqual(encoded_msg.find('Servicio').attrib['ShortNumber'], "229")
            idTran = re.compile("^[0-9]{5,}$");
            self.assertTrue(idTran.match(encoded_msg.find('Telefono').attrib['IdTran']))
            
        yield self.make_resource_worker(
            self.mk_mt_response_ok(), 
            code=http.OK, 
            callback=assert_request,
            headers={'X-Movilgate-Carrier': '2229.tigo.bo'})
        
        yield self.dispatch(self.mkmsg_out(from_addr='229'))
        [smsg] = self.get_dispatched('movilgate.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_mt_fail_shortnumber_unknown(self):
        def assert_request(request):              
            pass
            
        yield self.make_resource_worker(
            self.mk_mt_response_ok(), 
            code=http.INTERNAL_SERVER_ERROR, 
            callback=assert_request,
            headers={'X-Movilgate-Status': 'ERROR_NO_CARRIERDETECT'})
        
        yield self.dispatch(self.mkmsg_out(from_addr='229'))
        [smsg] = self.get_dispatched('movilgate.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                sent_message_id='1',
                delivery_status='failed',
                failure_level='service',
                failure_code='',
                failure_reason='ERROR_NO_CARRIERDETECT'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_mt_ok(self):
        def assert_request(request):              
            headers = dict(request.requestHeaders.getAllRawHeaders())
            self.assertEqual(headers['Content-Type'], ['text/xml; charset=UTF-8'])
            body = request.content.read()
            encoded_msg = ElementTree.fromstring(body)
            self.assertEqual(encoded_msg.find('Servicio').attrib['Id'],"2229.tigo.bo")
            self.assertTrue('ShortNumber' not in encoded_msg.find('Servicio').attrib)
            self.assertEqual(encoded_msg.find('Telefono').attrib['IdTran'],"12345678") 
            
        yield self.make_resource_worker(self.mk_mt_response_ok(), code=http.OK, callback=assert_request)
        transport_metadata = {'telefono_id_tran': '12345678', 'servicio_id': '2229.tigo.bo'}
        yield self.dispatch(self.mkmsg_out(transport_metadata=transport_metadata))
        [smsg] = self.get_dispatched('movilgate.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))
    
    @inlineCallbacks
    def test_sending_mt_ok_foreign_language(self):
        def assert_request(request):
            headers = dict(request.requestHeaders.getAllRawHeaders())
            self.assertEqual(headers['Content-Type'], ['text/xml; charset=UTF-8'])
            body = request.content.read()
            encoded_msg = ElementTree.fromstring(body)
            self.assertTrue(isinstance(encoded_msg.find('Contenido').text, unicode))
            
        yield self.make_resource_worker(self.mk_mt_response_ok(), code=http.OK, callback=assert_request)
        transport_metadata = {'telefono_id_tran': '12345678', 'servicio_id': '2229.tigo.bo'}
        my_msg = u'für me'
        yield self.dispatch(self.mkmsg_out(content=my_msg, transport_metadata=transport_metadata))
        [smsg] = self.get_dispatched('movilgate.event')
        self.assertEqual(
            self.mkmsg_ack(user_message_id='1',
                           sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_mt_ok_servicioid_missing(self):
        def assert_request(request):
            body = request.content.read()
            encoded_msg = ElementTree.fromstring(body)
            self.assertEqual(encoded_msg.find('Servicio').attrib['Id'], "")
            self.assertEqual(encoded_msg.find('Servicio').attrib['ShortNumber'], "229")
            idTran = re.compile("^[0-9]{5,}$");
            self.assertTrue(idTran.match(encoded_msg.find('Telefono').attrib['IdTran']))
            
        yield self.make_resource_worker(
            self.mk_mt_response_ok(), 
            code=http.OK, 
            callback=assert_request,
            headers={'X-Movilgate-Carrier': '2229.tigo.bo'})
        
        transport_metadata = {'telefono_id_tran': '12345678'}
        yield self.dispatch(self.mkmsg_out(from_addr='229', transport_metadata=transport_metadata))
        [smsg] = self.get_dispatched('movilgate.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_sending_mt_ok_idtran_missing(self):
        def assert_request(request):
            body = request.content.read()
            encoded_msg = ElementTree.fromstring(body)
            self.assertEqual(encoded_msg.find('Servicio').attrib['Id'], "2229.tigo.bo")
            self.assertTrue('ShortNumber' not in encoded_msg.find('Servicio').attrib)
            self.assertEqual(encoded_msg.find('Telefono').attrib['IdTran'], "0")
            
        yield self.make_resource_worker(
            self.mk_mt_response_ok(), 
            code=http.OK, 
            callback=assert_request)
        
        transport_metadata = {'servicio_id': '2229.tigo.bo'}
        yield self.dispatch(self.mkmsg_out(from_addr='229', transport_metadata=transport_metadata))
        [smsg] = self.get_dispatched('movilgate.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1'),
            TransportMessage.from_json(smsg.body))
        
    @inlineCallbacks
    def test_sending_mt_fail(self):
        yield self.make_resource_worker(self.mk_mt_response_fail())
        transport_metadata = {'telefono_id_tran': '12345678', 'servicio_id': '2229.tigo.bo'}
        yield self.dispatch(self.mkmsg_out(transport_metadata=transport_metadata))
        [smsg] = self.get_dispatched('movilgate.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                sent_message_id='1',
                delivery_status='failed',
                failure_level='service',
                failure_code='3',
                failure_reason='Fail: some reason'),
            TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_receiving_mo(self):
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, data=self.mk_mo_request())
        [smsg] = self.get_dispatched('movilgate.inbound')
        
        self.assertEqual(response.code, http.OK)
        msg_in = TransportMessage.from_json(smsg.body)
        self.assertEqual('hello world', msg_in['content'])
        self.assertEqual('41791234567', msg_in['from_addr'])
        self.assertEqual('2229', msg_in['to_addr'])
        self.assertEqual('12345678', msg_in['transport_metadata']['telefono_id_tran'])
        self.assertEqual('2229.tigo.bo', msg_in['transport_metadata']['servicio_id'])

    @inlineCallbacks
    def test_receiving_ping(self):
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, data='\n')
        
        msgs = self.get_dispatched('movilgate.inbound')
        
        self.assertEqual(response.code, http.OK)
        self.assertEqual(len(msgs), 0)

    @inlineCallbacks
    def test_receiving_fail(self):
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, data=' something strange')
        
        msgs = self.get_dispatched('movilgate.inbound')
        
        self.assertEqual(response.code, http.INTERNAL_SERVER_ERROR)
        self.assertEqual(len(msgs), 0)

    @inlineCallbacks
    def test_receiving_ping(self):
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, data='')
        self.assertEqual(200, response.code)


class TestResource(Resource):
    isLeaf = True
    
    def __init__(self, response, code=http.OK, callback=None, headers={}):
        self.response = response
        self.code = code
        self.callback = callback       #this is the test function containing all the assertions (a closure)
        self.headers = headers

    def render_POST(self, request):
        if self.callback is not None:
            self.callback(request)    # this callback function has 1 parameter: the request
        request.setResponseCode(self.code)
        for key, value in self.headers.iteritems():
            request.setHeader(key, value)
        return self.response
