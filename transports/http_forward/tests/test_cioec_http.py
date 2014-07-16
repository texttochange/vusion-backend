import json
from hashlib import sha1
from datetime import datetime
from base64 import b64encode

from twisted.internet.defer import inlineCallbacks
from twisted.web.resource import Resource
from twisted.web import http

from vumi.transports.tests.test_base import TransportTestCase
from vumi.tests.utils import get_stubbed_worker, TestResourceWorker
from vumi.message import TransportMessage

from tests.utils import MessageMaker

from transports import CioecHttp


class CioecHttpTransportTestCase(MessageMaker, TransportTestCase):
    
    transport_name = 'embolivia'
    transport_type = 'http_forward'
    transport_class = CioecHttp
    
    @inlineCallbacks
    def setUp(self):
        yield super(CioecHttpTransportTestCase, self).setUp()
        self.config = {
            'transport_name': self.transport_name,
            'api_key': 'a2edrfaQ',
            'salt': 'CIOEC', 
            'api': {'/api/registration': ['phone', 'name', 'email', 'sector'],
                    '/api/unregistration': ['phone'],
                    '/api/publishOffer': ['phone', 'message']}
        }
        self.worker = yield self.get_transport(self.config)
        self.worker.get_date = lambda: "2014-06-09"

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
        data = json.loads(request.content.read())
        self.assertEqual(data, args)

    def assert_authentication(self, request):
        headers = request.getAllHeaders()
        self.assertTrue('authorization' in headers, "authorization header is missing")
        value = headers['authorization']
        
        auth = sha1('%s%s%s' % (self.config['api_key'], self.config['salt'], datetime.now().strftime('%Y-%m-%d')))
        auth = b64encode("%s:api_token" % '3a08dec65c1d4a9fa452e23a21e7a42fddf392a1')
        self.assertEqual(value, "Basic %s" % auth)

    @inlineCallbacks
    def test_sms_registration(self):
        send_paths = {
            'partner1': {
                'path': '/api/registration',
                'port': 9999}}
        response_body = {
            "status":"success",
            "data": {
                "ids":[{"code":"aQx3","phone":"+59177777"}],
                "message":"X user registered"
            }
        }
        responses = {
            'partner1': [
                 json.dumps(response_body),
                 http.OK,
                 self.assert_request,
                 '/api/registration',
                 {"data":[{
                     "phone": "+6",
                     "name": "Sandra",
                     "sector": "Productor",
                     "email": "me@gmail.com",
                 }]},
                 self.assert_authentication
            ]}
        yield self.make_resource_worker(send_paths, responses)

        msg = self.mkmsg_out(
            to_addr="http://localhost:9999/api/registration",
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6',
                'participant_profile': [{'label': 'name',
                                         'value': 'Sandra'},
                                        {'label': 'sector',
                                         'value': 'Productor'},
                                        {'label': 'email',
                                         'value': 'me@gmail.com'}]})
        yield self.dispatch(msg)
        [ack] = self.get_dispatched('embolivia.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(ack.body))

    @inlineCallbacks
    def test_sms_registration_validation_failed(self):
        send_paths = {
            'partner1': {
                'path': '/api/registration',
                'port': 9999}}
        response_body = {
            "status":"fail",
            "data": {
                "error": "E010",
                "message": "Name is required"
            }
        }
        responses = {
            'partner1': [
                 json.dumps(response_body),
                 http.OK,
                 self.assert_request,
                 '/api/registration',
                 {"data":[{
                     "name": "Sandra",
                     "phone": "+6",
                     "sector": "Productor",
                     "email": "me@gmail.com",
                 }]}
            ]}
        yield self.make_resource_worker(send_paths, responses)

        msg = self.mkmsg_out(
            to_addr="http://localhost:9999/api/registration",
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6',
                'participant_profile': [
                    {'label': 'name',
                     'value': 'Sandra'},                    
                    {'label': 'sector',
                     'value': 'Productor'},
                    {'label': 'email',
                     'value': 'me@gmail.com'}]})
        yield self.dispatch(msg)
        [fail] = self.get_dispatched('embolivia.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                delivery_status='failed',
                failure_level='service',
                failure_code='E010',
                failure_reason='Name is required',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(fail.body))

    @inlineCallbacks
    def test_sms_registration_data_missing(self):
        send_paths = {
            'partner1': {
                'path': '/api/registration',
                'port': 9999}}
        response_body = {
            "status":"fail",
            "data": {
                "error": "E010",
                "message": "Name is required"
            }
        }
        responses = {
            'partner1': [
                 json.dumps(response_body),
                 http.OK,
                 self.assert_request,
                 '/api/registration',
                 {"data":[{
                     "phone": "+6",
                     "sector": "Productor",
                     "email": "me@gmail.com",
                 }]}
            ]}
        yield self.make_resource_worker(send_paths, responses)

        msg = self.mkmsg_out(
            to_addr="http://localhost:9999/api/registration",
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6',
                'participant_profile': [{'label': 'sector',
                                         'value': 'Productor'},
                                        {'label': 'email',
                                         'value': 'me@gmail.com'}]})
        yield self.dispatch(msg)
        [fail] = self.get_dispatched('embolivia.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                delivery_status='failed',
                failure_level='transport',
                failure_code=None,
                failure_reason='name is missing',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(fail.body))

    @inlineCallbacks
    def test_sms_unregistration(self):
        send_paths = {
            'partner1': {
                'path': '/api/unregistration',
                'port': 9999}}
        response_body = {
            "status":"success",
            "data": {
                "ids":[{"code":"aQx3","phone":"+59177777"}],
                "message":"user has unregistered"
            }
        }
        responses = {
            'partner1': [
                 json.dumps(response_body),
                 http.OK,
                 self.assert_request,
                 '/api/unregistration',
                 {"data":[{
                     "phone": "+6",
                 }]}
            ]}
        yield self.make_resource_worker(send_paths, responses)

        msg = self.mkmsg_out(
            to_addr="http://localhost:9999/api/unregistration",
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        yield self.dispatch(msg)
        [ack] = self.get_dispatched('embolivia.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(ack.body))

    @inlineCallbacks
    def test_sms_publishing_offer(self):
        send_paths = {
            'partner1': {
                'path': '/api/publishOffer',
                'port': 9999}}
        response_body = {
            "status":"success",
            "data": {
                "ids":[{"code":"aQx3","phone":"+59177777"}],
                "message":"X offers published"
            }
        }
        responses = {
            'partner1': [
                 json.dumps(response_body),
                 http.OK,
                 self.assert_request,
                 '/api/publishOffer',
                 {"data":[{
                     "phone": "+6",
                     "message": "OFFERT potatoes ,1kg, 22 ,La paz"
                 }]}
            ]}
        yield self.make_resource_worker(send_paths, responses)

        msg = self.mkmsg_out(
            to_addr="http://localhost:9999/api/publishOffer",
            from_addr="myprogram",
            content="OFFERT potatoes ,1kg, 22 ,La paz",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        yield self.dispatch(msg)
        [ack] = self.get_dispatched('embolivia.event')
        self.assertEqual(
            self.mkmsg_ack(
                user_message_id='1',
                sent_message_id='1',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(ack.body))

    @inlineCallbacks
    def test_send_fail_http_error(self):
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
        msg = self.mkmsg_out(
            to_addr="http://localhost:9999/sendsms1",
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        yield self.make_resource_worker(send_paths, responses)
        yield self.dispatch(msg)
        [fail] = self.get_dispatched('embolivia.event')
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
        msg = self.mkmsg_out(
            to_addr="http://localhost:9997/sendsms2",
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        yield self.make_resource_worker(send_paths, responses)
        yield self.dispatch(msg)
        [fail] = self.get_dispatched('embolivia.event')
        self.assertEqual(
            self.mkmsg_delivery(
                transport_name=self.transport_name,
                user_message_id='1',
                delivery_status='failed',
                failure_level='transport',
                failure_code=None,
                failure_reason='ConnectionRefusedError(\'Connection refused\',)',
                transport_metadata={'transport_type':'http_forward'}),
            TransportMessage.from_json(fail.body))  


class TestResource(Resource):
    isLeaf = True
    
    def __init__(self, response, code, assert_request, path, args={}, assert_headers=None):
        self.response = response
        self.code = code
        self.assert_request = assert_request
        self.path = path
        self.args = args
        self.assert_headers = assert_headers

    def render_POST(self, request):
        self.assert_request(request, self.path, self.args)
        if self.assert_headers is not None:
            self.assert_headers(request)
        request.setResponseCode(self.code)
        return self.response