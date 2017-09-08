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
        self.mock_greencoffeelizardv3_response = ''
        self.mock_greencoffeelizardv3_response2 = ''
        self.mock_greencoffeelizardv3_response_code = http.OK
        yield self.mock_greencoffeelizardv3.start()
        
        self.config = {
            'username': 'jouke.jongsma',
            'password': 'ttttt',
            'api': {
                '/api/v3/search/': [                    
                    'q',
                    'format']},
            'no_prices_keyword': 'NOP',
            'yes_prices_keyword': 'coffeer',
            'api_url_timeseries': 'https://greencoffee.lizard.net/api/v3/timeseries/'
        }
        self.tx_helper = self.add_helper(TransportHelper(GreencoffeelizardV3Http))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport.get_date = lambda: "2014-06-09"

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_greencoffeelizardv3.stop()
        yield super(GreencoffeelizardV3HttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.greencoffeelizardv3_calls.put(request)
        self.greencoffeelizardv3_calls_body.append(request.content.read())
        request.setResponseCode(self.mock_greencoffeelizardv3_response_code)
        return self.mock_greencoffeelizardv3_response

    @inlineCallbacks
    def test_outbound_get_timeseries_url(self):
        response_body = {
            "count": 3,
            "results": [
                {
                    "id": 517060,
                    "title": "X. Ayun",
                    "description": "64_629_23797",
                    "rank": 1.12597,
                    "entity_name": "measuringstation",
                    "entity_id": 206439,
                    "entity_url": "https://greencoffee.lizard.net/api/v3/measuringstations/206439/",
                    "view": [
                        14.14652640139809,
                        108.2852213121359,
                        12
                    ]
                    },
                {
                    "id": 516921,
                    "title": "X. AYun",
                    "description": "64_633_23956",
                    "rank": 1.12597,
                    "entity_name": "measuringstation",
                    "entity_id": 206300,
                    "entity_url": "https://greencoffee.lizard.net/api/v3/measuringstations/206300/",
                    "view": [
                        13.69803938252308,
                        108.1675569741558,
                        12
                    ]
                    },
                {
                    "id": 516927,
                    "title": "X. AYun H",
                    "description": "64_638_24048",
                    "rank": 1.12273,
                    "entity_name": "measuringstation",
                    "entity_id": 206306,
                    "entity_url": "https://greencoffee.lizard.net/api/v3/measuringstations/206306/",
                    "view": [
                        13.55530111736451,
                        108.2507778264543,
                        12
                    ]
                }
            ]
        } 
        self.mock_greencoffeelizardv3_response = json.dumps(response_body)

        yield self.tx_helper.make_dispatch_outbound(
            #to_addr="https://greencoffee.lizard.net/api/v3/search/",
            to_addr="%sapi/v3/search/" % self.mock_greencoffeelizardv3.url,
            #to_addr="%sapi/v2/timeseries/" % self.mock_greencoffeelizardv3.url,
            from_addr="myprogram",
            content="coffee X. Kroong",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8281',
                'participant_phone': '+6',
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'}]})
        req = yield self.mock_greencoffeelizardv3_response
        reqdic = ast.literal_eval(req)
        self.assertEqual(
            reqdic['count'], 7)
        #[event] = yield self.tx_helper.get_dispatched_events()
        #self.assertEqual(event['event_type'], 'ack')
        #self.assertEqual(event['user_message_id'], '1')
        #self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})
        
        
    #@inlineCallbacks
    #def test_outbound_get_timeseries_value(self):
        #response_body = {
            #"count": 2,
            #"results": [
                #{
                    #"url": "%sapi/v2/timeseries/f440e143-c8f1-478d-b7b2-0275bcc6040a/?format=json" % self.mock_greencoffeelizard.url,
                    #"id": 194339,
                    #"uuid": "f440e143-c8f1-478d-b7b2-0275bcc6040a",
                    #"name": "dRBA",
                    #"organisation_code": "dRBA",
                    #"observation_type":{
                        #"url":"https://greencoffee.lizard.net/api/v2/parameterreferencedunits/566/?format=json",
                        #"code":"dRBA",
                        #"parameter_short_display_name":"Robusta, Bean, Agent Price",
                        #"referenced_unit_short_display_name":"d/kg","scale":"interval","description":"",
                        #"reference_frame":"",
                        #"compartment":""
                    #},
                #},
                
            #{
                #"url": "%sapi/v2/timeseries/fa22c9f2-08ee-4c12-8008-012722c3fa8c/?format=json" % self.mock_greencoffeelizard.url,
                #"id": 194340,
                #"uuid": "fa22c9f2-08ee-4c12-8008-012722c3fa8c",
                #"name": "dRBC",
                #"organisation_code": "dRBC",
                #"observation_type":{
                    #"url":"https://greencoffee.lizard.net/api/v2/parameterreferencedunits/567/?format=json",
                    #"code":"dRBC",
                    #"parameter_short_display_name":"Robusta, Bean, Company Price",
                    #"referenced_unit_short_display_name":"d/kg",
                    #"scale":"interval","description":"",
                    #"reference_frame":"","compartment":""
                #},
            #}
            #],
            #"events": [
                #{
                    #"timestamp": 1484784000000,
                    #"value": 46700
                #},
                #{
                    #"timestamp": 1487289600000,
                    #"value": 45700
                #},
                #{
                    #"timestamp": 1487635200000,
                    #"value": 46700
                #}
            #]
        #}

        #self.mock_greencoffeelizard_response = json.dumps(response_body)
        
        #yield self.tx_helper.make_dispatch_outbound(
            #to_addr="%sapi/v2/timeseries/" % self.mock_greencoffeelizard.url,
            #from_addr="myprogram",
            #content="coffee EaTam344",
            #message_id='1',
            #transport_metadata={
                #'program_shortcode': '+2568281',
                #'participant_phone': '+6',
                #'participant_profile': [
                    #{'label': 'reporterid',
                     #'value': '708'}]})
        #req = yield self.mock_greencoffeelizard_response
        #reqdic = ast.literal_eval(req)
        #self.assertEqual(
            #reqdic['events'],
            #[{"timestamp": 1484784000000, "value": 46700}, {"timestamp": 1487289600000, "value": 45700}, {"timestamp": 1487635200000, "value": 46700}])        
        #[event] = yield self.tx_helper.get_dispatched_events()
        #self.assertEqual(event['event_type'], 'ack')
        #self.assertEqual(event['user_message_id'], '1')
        #self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})
