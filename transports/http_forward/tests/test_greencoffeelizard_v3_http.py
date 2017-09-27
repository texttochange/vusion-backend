import json, ast
from hashlib import sha1
from datetime import datetime
from base64 import b64encode
from urlparse import urlparse

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import MockHttpServer, VumiTestCase
from vumi.tests.utils import RegexMatcher

from transports import GreencoffeelizardV3Http


class GreencoffeelizardV3HttpTransportTestCase(VumiTestCase):
        
    @inlineCallbacks
    def setUp(self):
        self.greencoffeelizardv3_calls = DeferredQueue()
        self.greencoffeelizardv3_calls_body = []
        self.mock_greencoffeelizardv3 = MockHttpServer(self.handle_request)
        self.mock_greencoffeelizardv3_2 = MockHttpServer(self.handle_request2)
        self.mock_greencoffeelizardv3_response = ''
        self.mock_greencoffeelizardv3_response2 = ''
        self.mock_greencoffeelizardv3_response_code = http.OK
        yield self.mock_greencoffeelizardv3_2.start()
        yield self.mock_greencoffeelizardv3.start()
        
        self.config = {
            'username': 'jo324444',
            'password': 'rrfffrrr',
            'api': {
                '/api/v3/search/': [                    
                    'q',
                    'format']},
            'no_count_loc_code_keyword': 'nolocationcode',
            'no_count_results_keyword': 'noresults',
            'no_events_at_loaction_keyword': 'nolater',
            'yes_agent_prices_keyword': 'coffeea',
            'yes_company_prices_keyword': 'coffeec',
            'yes_weather_keyword': 'weatherall',
            'yes_feedback_keyword': 'kfeedback',
            #'api_url_timeseries': 'https://greencoffee.lizard.net/api/v3/timeseries/'
            'api_url_timeseries': '%s/api/v3/timeseries/' % self.mock_greencoffeelizardv3_2.url
        }
        self.tx_helper = self.add_helper(TransportHelper(GreencoffeelizardV3Http))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport.get_date = lambda: "2014-06-09"

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_greencoffeelizardv3.stop()
        yield self.mock_greencoffeelizardv3_2.stop()
        yield super(GreencoffeelizardV3HttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.greencoffeelizardv3_calls.put(request)
        self.greencoffeelizardv3_calls_body.append(request.content.read())
        request.setResponseCode(self.mock_greencoffeelizardv3_response_code)
        return self.mock_greencoffeelizardv3_response    

    def handle_request2(self, request):
        self.greencoffeelizardv3_calls.put(request)
        self.greencoffeelizardv3_calls_body.append(request.content.read())
        request.setResponseCode(self.mock_greencoffeelizardv3_response_code)
        return self.mock_greencoffeelizardv3_response2

    def mock_response(self):
        json_file2='jsonfiles/greencoffee_response_2.json'        
        json_data2=open(json_file2)
        data2 = json.load(json_data2)
        response_body2 = data2
        
        json_file1='jsonfiles/greencoffee_response_1.json'        
        json_data1=open(json_file1)
        data1 = json.load(json_data1)        
        response_body = data1

        json_data1.close()
        json_data2.close()
        
        self.mock_greencoffeelizardv3_response = json.dumps(response_body)
        self.mock_greencoffeelizardv3_response2 = json.dumps(response_body2)

    @inlineCallbacks
    def test_outbound_get_content_for_agent_price(self):
        self.mock_response()
        
        yield self.tx_helper.make_dispatch_outbound(
            #to_addr="https://greencoffee.lizard.net/api/v3/search/",
            to_addr="%sapi/v3/search/" % self.mock_greencoffeelizardv3.url,
            from_addr="myprogram",
            content="Coffeea X. Kroong",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8281',
                'participant_phone': '+6',
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'}]})
        req = yield self.mock_greencoffeelizardv3_response
        reqdic = ast.literal_eval(req)
        
        req2 = yield self.mock_greencoffeelizardv3_response2
        reqdic2 = ast.literal_eval(req2)
        
        self.assertEqual(
            reqdic['count'], 3)
        self.assertEqual(
                    reqdic2['count'], 7)
        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual('kfeedback X. Kroong On 2017-07-28 03:00 AgentPrice=45700', user_msg['content'])
        [event] = yield self.tx_helper.get_dispatched_events()        
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})
        
        
    @inlineCallbacks
    def test_outbound_sent_weather_keyword(self):
        self.mock_response()
        
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/v3/search/" % self.mock_greencoffeelizardv3.url,
            from_addr="myprogram",
            content="weatherall X. Kroong",
            message_id='1',
            transport_metadata={
                'program_shortcode': '+2568281',
                'participant_phone': '+6',
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'}]})
        
        req = yield self.mock_greencoffeelizardv3_response
        reqdic = ast.literal_eval(req)                
        req2 = yield self.mock_greencoffeelizardv3_response2
        reqdic2 = ast.literal_eval(req2)        
        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual('kfeedback X. Kroong On 2017-07-31 15:00 WindDir=235, MinTemp=21, WindSpeed=2, MaxTemp=27, Precipitation=7',
                         user_msg['content'])
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})
    
    
    @inlineCallbacks
    def test_outbound_sent_worng_keyword(self):
        self.mock_response()
        
        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/v3/search/" % self.mock_greencoffeelizardv3.url,
            from_addr="myprogram",
            content="coffee X. Kroong",
            message_id='1',
            transport_metadata={
                'program_shortcode': '+2568281',
                'participant_phone': '+6',
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'}]})

        req = yield self.mock_greencoffeelizardv3_response
        reqdic = ast.literal_eval(req)

        req2 = yield self.mock_greencoffeelizardv3_response2
        reqdic2 = ast.literal_eval(req2)       
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})
