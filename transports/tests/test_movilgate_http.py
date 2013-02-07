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
            '<Servicio Id="texttochange" ContentType="0" CreateSession="0"/>'
            '<Telefono msisdn="256788" IdTran="123"/>'
            '<Contenido>ola mundo</Contenido>'
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
            '<Servicio Id="20500 .personal.ar"/>'
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
                'id': 'texttochange'},
            'telephono':{
                'msisdn': '256788',
                'id_tran': '123'
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
            'default_origin': '9292',
            'receive_path': '/movilgate',
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
        yield self.make_resource_worker(self.mk_mt_response_ok())
        yield self.dispatch(self.mkmsg_out())
        [smsg] = self.get_dispatched('movilgate.event')
        self.assertEqual(
            self.mkmsg_ack(user_message_id='1',
                           sent_message_id='1'),
            TransportMessage.from_json(smsg.body))
        
    @inlineCallbacks
    def test_sending_one_sms_fail(self):
        yield self.make_resource_worker(self.mk_mt_response_fail())
        yield self.dispatch(self.mkmsg_out())
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
    def test_receiving_one_sms(self):
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(url, data=self.mk_mo_request())
        [smsg] = self.get_dispatched('movilgate.inbound')
        
        self.assertEqual(response.code, http.OK)
        msg_in = TransportMessage.from_json(smsg.body)
        self.assertEqual('hello world', msg_in['content'])
        self.assertEqual('41791234567', msg_in['from_addr'])
        self.assertEqual('9292', msg_in['to_addr'])

class TestResource(Resource):
    isLeaf = True
    
    def __init__(self, response, code=http.OK, send_id=None):
        self.response = response
        self.code = code
        self.send_id = send_id
        
    def render_POST(self, request):
        request.setResponseCode(self.code)
        return self.response
