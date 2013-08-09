
from twisted.internet.defer import inlineCallbacks
from twisted.web.resource import Resource
from twisted.web import http

from vumi.transports.tests.test_base import TransportTestCase
from vumi.tests.utils import get_stubbed_worker, TestResourceWorker
from vumi.message import TransportMessage

from tests.utils import MessageMaker

from transports import ForwardHttp

class ForwardHttpTransportTestCase(MessageMaker, TransportTestCase):
    
    transport_name = 'forward'
    transport_type = 'http_forward'
    transport_class = ForwardHttp
    
    @inlineCallbacks
    def setUp(self):
        yield super(ForwardHttpTransportTestCase, self).setUp()
        self.config = {
            'transport_name': self.transport_name,
            'message_replacement':{
                'content': '\[MESSAGE\]',
                'from_addr': '\[PROGRAM\]'},
            'message_metadata_replacement': {
                'participant_phone': '\[FROM\]',
                'program_shortcode': '\[TO\]'}
        }
        self.worker = yield self.get_transport(self.config)

    def make_resource_worker(self, send_paths, responses):
        w = get_stubbed_worker(TestResourceWorker, {})
        resources = []
        for site, details in send_paths.iteritems():
            resources.append((details['path'], TestResource, responses[site]))
        w.set_resources(resources)
        self._workers.append(w)
        return w.startWorker()
    
    def get_dispatched(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)

    def assert_request(self, request, path, args):
        self.assertEqual(request.path, path)
        self.assertEqual(request.args, args)    

    @inlineCallbacks
    def test_sending_ok_same_message_multiple_forward(self):
        send_paths = {
            'partner1': {
                'path': '/sendsms1',
                'port': 9999},
            'partner2': {
                'path': '/sendsms2',
                'port': 9999}}        
        responses = {
            'partner1': [
                '',
                http.OK,
                self.assert_request,
                '/sendsms1'],
            'partner2': [
                '',
                http.OK,
                self.assert_request,                
                '/sendsms2']}
        msg1 = self.mkmsg_out(to_addr="http://localhost:9999/sendsms1",
                             from_addr="myprogram",
                             message_id='1',
                             transport_metadata={
                                 'program_shortcode': '256-8181',
                                 'participant_phone': '+6'})
        msg2 = self.mkmsg_out(to_addr="http://localhost:9999/sendsms2",
                              from_addr="myprogram",
                              message_id='2',
                              transport_metadata={
                                  'program_shortcode': '256-8181',
                                  'participant_phone': '+6'})
        yield self.make_resource_worker(send_paths, responses)
        yield self.dispatch(msg1)
        yield self.dispatch(msg2)        
        [ack1, ack2] = self.get_dispatched('forward.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(ack1.body))
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='2',
                sent_message_id='2',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(ack2.body))

    @inlineCallbacks
    def test_sending_ok_url_with_arguments(self):
        send_paths = {
            'partner1': {
                'path': '/sendsms1',
                'port': 9999}}        
        responses = {
            'partner1': [
                 '',
                 http.OK,
                 self.assert_request,
                 '/sendsms1',
                 {'message': ['Hello'],
                  'to': ['256-8181'],
                  'from': ['+6'],
                  'program': ['myprogram']}]}
        msg = self.mkmsg_out(to_addr="http://localhost:9999/sendsms1?message=[MESSAGE]&from=[FROM]&to=[TO]&program=[PROGRAM]",
                             from_addr="myprogram",
                             content="Hello",
                             message_id='1',
                             transport_metadata={
                                 'program_shortcode': '256-8181',
                                 'participant_phone': '+6'})
        yield self.make_resource_worker(send_paths, responses)
        yield self.dispatch(msg)
        [ack] = self.get_dispatched('forward.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(ack.body))

    @inlineCallbacks
    def test_send_fail_service_error(self):
        send_paths = {
            'partner1': {
                'path': '/sendsms1',
                'port': 9999}}        
        responses = {
            'partner1': [
                 'SOME INTERNAL STUFF HAPPEN',
                 http.INTERNAL_SERVER_ERROR,
                 self.assert_request,
                 '/sendsms1']}
        msg = self.mkmsg_out(to_addr="http://localhost:9999/sendsms1",
                             from_addr="myprogram",
                             content="Hello",
                             message_id='1',
                             transport_metadata={
                                 'program_shortcode': '256-8181',
                                 'participant_phone': '+6'})
        yield self.make_resource_worker(send_paths, responses)
        yield self.dispatch(msg)
        [fail] = self.get_dispatched('forward.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                delivery_status='failed',
                failure_level='http',
                failure_code=http.INTERNAL_SERVER_ERROR,
                failure_reason='SOME INTERNAL STUFF HAPPEN',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(fail.body))

    @inlineCallbacks
    def test_send_fail_connection_error(self):
        send_paths = {}
        responses = {}
        msg = self.mkmsg_out(to_addr="http://localhost:9997/sendsms2",
                             from_addr="myprogram",
                             content="Hello",
                             message_id='1',
                             transport_metadata={
                                 'program_shortcode': '256-8181',
                                 'participant_phone': '+6'})
        yield self.make_resource_worker(send_paths, responses)
        yield self.dispatch(msg)
        [fail] = self.get_dispatched('forward.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                delivery_status='failed',
                failure_level='transport',
                failure_code=None,
                failure_reason='Connection refused',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(fail.body))        
    
    @inlineCallbacks
    def test_send_fail_wrong_url_format(self):
        send_paths = {}
        responses = {}
        msg = self.mkmsg_out(to_addr="htp://localhost:9997/sendsms",
                             from_addr="myprogram",
                             content="Hello",
                             message_id='1',
                             transport_metadata={
                                 'program_shortcode': '256-8181',
                                 'participant_phone': '+6'})
        yield self.make_resource_worker(send_paths, responses)
        yield self.dispatch(msg)
        [fail] = self.get_dispatched('forward.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                delivery_status='failed',
                failure_level='transport',
                failure_code=None,
                failure_reason='Unsupported scheme: \'htp\'',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(fail.body))

class TestResource(Resource):
    isLeaf = True
    
    def __init__(self, response, code, assert_request, path, args={}):
        self.response = response
        self.code = code
        self.assert_request = assert_request
        self.path = path
        self.args = args

    def render_GET(self, request):
        self.assert_request(request, self.path, self.args)
        request.setResponseCode(self.code)
        return self.response
        
