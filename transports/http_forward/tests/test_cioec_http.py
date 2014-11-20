import json
from hashlib import sha1
from datetime import datetime
from base64 import b64encode

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import MockHttpServer, VumiTestCase
from vumi.tests.utils import RegexMatcher

from transports import CioecHttp


class CioecHttpTransportTestCase(VumiTestCase):
        
    @inlineCallbacks
    def setUp(self):
        self.cioec_calls = DeferredQueue()
        self.cioec_calls_body = [] 
        self.mock_cioec = MockHttpServer(self.handle_request)
        self.mock_cioec_response = ''
        self.mock_cioec_response_code = http.OK
        yield self.mock_cioec.start()
        
        self.config = {
            'api_key': 'a2edrfaQ',
            'salt': 'CIOEC', 
            'api': {
                '/api/registration': [
                    'phone',
                    'name',
                    {'label': 'email',
                     'default': 'not_defined'},
                     'sector'],
                '/api/unregistration': ['phone'],
                '/api/publishOffer': ['phone', 'message']}
        }
        self.tx_helper = self.add_helper(TransportHelper(CioecHttp))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport.get_date = lambda: "2014-06-09"

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_cioec.stop()
        yield super(CioecHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.cioec_calls.put(request)
        self.cioec_calls_body.append(request.content.read())
        request.setResponseCode(self.mock_cioec_response_code)
        return self.mock_cioec_response

    def assert_authentication(self, request):
        headers = request.getAllHeaders()
        self.assertTrue('authorization' in headers, "authorization header is missing")
        value = headers['authorization']
        
        auth = b64encode("3a08dec65c1d4a9fa452e23a21e7a42fddf392a1:api_token")
        self.assertEqual(value, "Basic %s" % auth)

    @inlineCallbacks
    def test_outbound_ok_registration(self):
        response_body = {
            "status":"success",
            "message":"X user registered",
            "data": {
                "ids":[{"code":"aQx3","phone":"+59177777"}],
            }
        }
        self.mock_cioec_response = json.dumps(response_body)

        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/registration" % self.mock_cioec.url,
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
        req = yield self.cioec_calls.get()
        self.assert_authentication(req)
        req_body = self.cioec_calls_body.pop()
        self.assertEqual(
            json.loads(req_body),
            {"data":[{
                "phone": "+6",
                "name": "Sandra",
                "sector": "Productor",
                "email": "me@gmail.com",
                }]})

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_ok_registration_default_value(self):
            response_body = {
                "status":"success",
                "message":"X user registered",
                "data": {
                    "ids":[{"code":"aQx3","phone":"+59177777"}],
                }
            }
            self.mock_cioec_response = json.dumps(response_body)

            yield self.tx_helper.make_dispatch_outbound(
                to_addr="%sapi/registration" % self.mock_cioec.url,
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
                         'value': 'Productor'}]})
            req = yield self.cioec_calls.get()
            self.assert_authentication(req)
            req_body = self.cioec_calls_body.pop()
            self.assertEqual(
                json.loads(req_body),
                {"data":[{
                    "phone": "+6",
                    "name": "Sandra",
                    "sector": "Productor",
                    "email": "not_defined",
                    }]})
            
            [event] = self.tx_helper.get_dispatched_events()
            self.assertEqual(event['event_type'], 'ack')
            self.assertEqual(event['user_message_id'], '1')
            self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_fail_registration_validation_cioec(self):
        response_body = {
            "status":"fail",
            "error": "E010",
            "message": "Name is required"
        }
        self.mock_cioec_response = json.dumps(response_body)

        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/registration" % self.mock_cioec.url,
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
        req = yield self.cioec_calls.get()
        self.assertEqual('/api/registration', req.path)        
        
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "SERVICE ERROR E010 - Name is required")
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_fail_registration_validation_transport(self):
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/registration" % self.mock_cioec.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6',
                'participant_profile': [
                    {'label': 'sector',
                     'value': 'Productor'},
                    {'label': 'email',
                     'value': 'me@gmail.com'}]})
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "MISSING DATA name is missing")
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})
        
    @inlineCallbacks
    def test_outbound_ok_unregistration(self):
        response_body = {
            "status":"success",
            "message":"user has unregistered",
            "data": {
                "ids":[{"code":"aQx3","phone":"+59177777"}],
            }
        }
        self.mock_cioec_response = json.dumps(response_body)
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/unregistration" % self.mock_cioec.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        req = yield self.cioec_calls.get()
        self.assertEqual('/api/unregistration', req.path)
        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_ok_publishing_offer(self):
        response_body = {
            "status":"success",
            "message":"X offers published",
            "data": {
                "ids":[{"code":"aQx3","phone":"+59177777"}],
            }
        }
        self.mock_cioec_response = json.dumps(response_body)

        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/publishOffer" % self.mock_cioec.url,
            from_addr="myprogram",
            content="OFFERT potatoes ,1kg, 22 ,La paz",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        req = yield self.cioec_calls.get()
        self.assertEqual('/api/publishOffer', req.path)
        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_oubound_fail_http_error(self):
        self.mock_cioec_response = 'SOME INTERNAL STUFF HAPPEN'
        self.mock_cioec_response_code = http.INTERNAL_SERVER_ERROR
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%ssendsms1" % self.mock_cioec.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})

        req = yield self.cioec_calls.get()
        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "HTTP ERROR 500 - SOME INTERNAL STUFF HAPPEN")

    @inlineCallbacks
    def test_outbound_fail_connection_error(self):
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%ssendsms2" % self.mock_cioec.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        yield self.cioec_calls.get()
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], RegexMatcher("TRANSPORT ERROR ValueError"))
