import json
from hashlib import sha1
from datetime import datetime
from base64 import b64encode

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import MockHttpServer, VumiTestCase
from vumi.tests.utils import RegexMatcher

from transports import AskpeopleHttp


class AskpeopleHttpTransportTestCase(VumiTestCase):
        
    @inlineCallbacks
    def setUp(self):
        self.askpeople_calls = DeferredQueue()
        self.askpeople_calls_body = [] 
        self.mock_askpeople = MockHttpServer(self.handle_request)
        self.mock_askpeople_response = ''
        self.mock_askpeople_response_code = http.OK
        yield self.mock_askpeople.start()
        
        self.config = {
            'api_key': 'a2edrfaQ',
            'email': 'ttc@ttc.com',
            'password': 'ttc.password',
            'api': {
                '/api/answers': [                    
                    'question',
                    'reporter',
                    'answer',
                    'answer_text']}
        }
        self.tx_helper = self.add_helper(TransportHelper(AskpeopleHttp))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport.get_date = lambda: "2014-06-09"

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_askpeople.stop()
        yield super(AskpeopleHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.askpeople_calls.put(request)
        self.askpeople_calls_body.append(request.content.read())
        request.setResponseCode(self.mock_askpeople_response_code)
        return self.mock_askpeople_response

    def assert_authentication(self, request):
        headers = request.getAllHeaders()
        self.assertTrue('authorization' in headers, "authorization header is missing")
        value = headers['authorization']
        
        auth = "a2edrfaQ"
        self.assertEqual(value, "Basic %s" % auth)

    @inlineCallbacks
    def test_outbound_ok_answers_select(self):
        response_body = {
            "status":"success",
            "message":"X answer sent",
            "data": {
                "ids":[{"code":"aQx3","phone":"+59177777"}],
            }
        }
        self.mock_askpeople_response = json.dumps(response_body)

        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/answers" % self.mock_askpeople.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6',
                'participant_tags': ['imported', '134', '123'],
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'},
                    {'label': 'Answer21',
                     'value': 'Male'},
                    {'label': 'Answer22',
                     'value': 'Female'}]})
        req = yield self.askpeople_calls.get()
        self.assert_authentication(req)
        req_body = self.askpeople_calls_body.pop()
        self.assertEqual(
            json.loads(req_body),
            [{
                "question": "22",
                "reporter": "708",
                "answer": "123",
                }])

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_ok_answers_free(self):
        response_body = {
            "status":"success",
            "message":"X answer sent",
            "data": {
                "ids":[{"code":"aQx3","phone":"+59177777"}],
            }
        }
        self.mock_askpeople_response = json.dumps(response_body)

        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/answers" % self.mock_askpeople.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6',
                'participant_tags': ['imported', 'free', '123'],
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'},
                    {'label': 'Answer21',
                     'value': 'Male'}]})
        req = yield self.askpeople_calls.get()
        self.assert_authentication(req)
        req_body = self.askpeople_calls_body.pop()
        self.assertEqual(
            json.loads(req_body),
            [{
                "question": "21",
                "reporter": "708",
                "answer_text": "Male",
                }])

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_ok_answers_default_value(self):
            response_body = {
                "status":"success",
                "message":"X answer sent",
                "data": {
                    "ids":[{"code":"aQx3","phone":"+59177777"}],
                }
            }
            self.mock_askpeople_response = json.dumps(response_body)

            yield self.tx_helper.make_dispatch_outbound(
                to_addr="%sapi/answers" % self.mock_askpeople.url,
                from_addr="myprogram",
                content="Hello",
                message_id='1',
                transport_metadata={
                    'program_shortcode': '256-8181',
                    'participant_phone': '+6',
                    'participant_tags': ['imported', '456', '784','7855'],
                    'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'},
                    {'label': 'Answer4',
                     'value': 'tom'},
                    {'label': 'Answer7',
                     'value': 'John'},
                    {'label': 'Answer2',
                     'value': 'Janson'}]})
            req = yield self.askpeople_calls.get()
            self.assert_authentication(req)
            req_body = self.askpeople_calls_body.pop()
            self.assertEqual(
                json.loads(req_body),
                [{
                    "question": "2",
                    "reporter": "708",
                    "answer": "7855",
                    }])
            
            [event] = self.tx_helper.get_dispatched_events()
            self.assertEqual(event['event_type'], 'ack')
            self.assertEqual(event['user_message_id'], '1')
            self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_fail_answers_validation_askpeople(self):
        response_body = {
            "status":"fail",
            "error": "E010",
            "message": "Name is required"
        }
        self.mock_askpeople_response = json.dumps(response_body)

        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/answers" % self.mock_askpeople.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6',
                'participant_tags': ['imported', '134', '123'],
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'},
                    {'label': 'Answer21',
                     'value': 'Male'},
                    {'label': 'Answer22',
                     'value': 'Female'}]})
        req = yield self.askpeople_calls.get()
        self.assertEqual('/api/answers', req.path)        
        
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "SERVICE ERROR E010 - Name is required")
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_fail_answers_validation_transport(self):
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/answers" % self.mock_askpeople.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6',
                'participant_tags': ['imported', '134', '123'],
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'},
                    {'label': 'yAnswer21',
                     'value': 'Male'},
                    {'label': 'yAnswer22',
                     'value': 'Female'}]})
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "MISSING DATA question is missing")
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_outbound_fail_transport(self):
        yield self.mock_askpeople.stop()        
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/answers" % self.mock_askpeople.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
              transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6',
                'participant_tags': ['imported', '134', '123'],
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'},
                    {'label': 'Answer21',
                     'value': 'Male'},
                    {'label': 'Answer22',
                     'value': 'Female'}]})
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "TRANSPORT ERROR Connection refused")
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
        self.mock_askpeople_response = json.dumps(response_body)

        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/publishOffer" % self.mock_askpeople.url,
            from_addr="myprogram",
            content="OFFERT potatoes ,1kg, 22 ,La paz",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        req = yield self.askpeople_calls.get()
        self.assertEqual('/api/publishOffer', req.path)
        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})

    @inlineCallbacks
    def test_oubound_fail_http_error(self):
        self.mock_askpeople_response = 'SOME INTERNAL STUFF HAPPEN'
        self.mock_askpeople_response_code = http.INTERNAL_SERVER_ERROR
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%ssendsms1" % self.mock_askpeople.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})

        req = yield self.askpeople_calls.get()
        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "HTTP ERROR 500 - SOME INTERNAL STUFF HAPPEN")

    @inlineCallbacks
    def test_outbound_fail_connection_error(self):
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%ssendsms2" % self.mock_askpeople.url,
            from_addr="myprogram",
            content="Hello",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8181',
                'participant_phone': '+6'})
        yield self.askpeople_calls.get()
        [event] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], "TRANSPORT ERROR No JSON object could be decoded")
