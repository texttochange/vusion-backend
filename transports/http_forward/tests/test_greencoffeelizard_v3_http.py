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
            'username': 'jouke.jongsma',
            'password': 't7W#<Q?i37@4XR(j',
            'api': {
                '/api/v3/search/': [                    
                    'q',
                    'format']},
            'no_count_keyword': 'nolocation',
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
        response_body2 = {
            "count": 7,
            "results": [
                {
                    "url": "https://greencoffee.lizard.net/api/v3/timeseries/8b3a1590-a884-4123-85d7-d1031045e4f8/?format=json",
                    "id": 199801,
                    "uuid": "8b3a1590-a884-4123-85d7-d1031045e4f8",
                    "name": "dRBC",
                    "code": "dRBC",
                    "value_type": "float",
                    "location": {
                        "url": "https://greencoffee.lizard.net/api/v3/locations/395e4c40-107d-4294-9c12-6d046bc5e314/?format=json",
                        "uuid": "395e4c40-107d-4294-9c12-6d046bc5e314",
                        "name": "67_663_24667",
                        "code": "67_663_24667",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                107.6201020496574,
                                12.57096549076355,
                                0
                            ]
                        }
                        },
                    "observation_type": {
                        "url": "https://greencoffee.lizard.net/api/v3/observationtypes/567/?format=json",
                        "code": "dRBC",
                        "parameter": "Robusta, Bean, Company Price",
                        "unit": "d/kg",
                        "scale": "interval",
                        "description": "",
                        "domain_values": "",
                        "reference_frame": "",
                        "compartment": ""
                        },
                    "access_modifier": "Private",
                    "supplier_code": "dRBC",
                    "start": 1494892800000,
                    "end": 1502150400000,
                    "last_value": 45900,
                    "events": [
                        {
                            "max": 46000,
                            "timestamp": 1499126400000,
                            "min": 46000
                            },
                        {
                            "max": 46000,
                            "timestamp": 1499212800000,
                            "min": 46000
                            },
                        {
                            "max": 46000,
                            "timestamp": 1499299200000,
                            "min": 46000
                            },                
                        {
                            "max": 46000,
                            "timestamp": 1501200000000,
                            "min": 46000
                        }
                        ],
                    "percentiles": []
                    },
                {
                    "url": "https://greencoffee.lizard.net/api/v3/timeseries/5089bdea-0214-4494-b9e2-9a416bd1c675/?format=json",
                    "id": 199802,
                    "uuid": "5089bdea-0214-4494-b9e2-9a416bd1c675",
                    "name": "dRBA",
                    "code": "dRBA",
                    "value_type": "float","location": {
                        "url": "https://greencoffee.lizard.net/api/v3/locations/395e4c40-107d-4294-9c12-6d046bc5e314/?format=json",
                        "uuid": "395e4c40-107d-4294-9c12-6d046bc5e314",
                        "name": "67_663_24667",
                        "code": "67_663_24667",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                107.6201020496574,
                                12.57096549076355,
                                0
                            ]
                        }
                        },
                    "observation_type": {
                        "url": "https://greencoffee.lizard.net/api/v3/observationtypes/566/?format=json",
                        "code": "dRBA",
                        "parameter": "Robusta, Bean, Agent Price",
                        "unit": "d/kg",
                        "scale": "interval",
                        "description": "",
                        "reference_frame": "",
                        "compartment": ""
                        },
                    "access_modifier": "Private",
                    "supplier_code": "dRBA",
                    "start": 1494892800000,
                    "end": 1502150400000,
                    "last_value": 45600,
                    "events": [
                        {
                            "max": 45900,
                            "timestamp": 1499126400000,
                            "min": 45900
                            },
                        {
                            "max": 45800,
                            "timestamp": 1499212800000,
                            "min": 45800
                            },
                        {
                            "max": 44700,
                            "timestamp": 1501113600000,
                            "min": 44700
                            },
                        {
                            "max": 45700,
                            "timestamp": 1501200000000,
                            "min": 45700
                        }
                        ],
                    "percentiles": []
                    },
                {
                    "url": "https://greencoffee.lizard.net/api/v3/timeseries/5cd82357-b626-4aa5-929a-7d211eacd6d0/?format=json",
                    "id": 201009,
                    "uuid": "5cd82357-b626-4aa5-929a-7d211eacd6d0",
                    "name": "PRCP",
                    "code": "PRCP",
                    "value_type": "float",
                    "location": {
                        "url": "https://greencoffee.lizard.net/api/v3/locations/395e4c40-107d-4294-9c12-6d046bc5e314/?format=json",
                        "uuid": "395e4c40-107d-4294-9c12-6d046bc5e314",
                        "name": "67_663_24667",
                        "code": "67_663_24667",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                107.6201020496574,
                                12.57096549076355,
                                0
                            ]
                        }
                        },
                    "observation_type": {
                        "url": "https://greencoffee.lizard.net/api/v3/observationtypes/542/?format=json",
                        "code": "PRCP",
                        "parameter": "Precipitation",
                        "unit": "mm",
                        "scale": "ratio",
                        "description": "",
                        "reference_frame": "",
                        "compartment": ""
                        },
                    "access_modifier": "Private",
                    "supplier_code": "PRCP",
                    "start": 1498780800000,
                    "end": 1502409600000,
                    "last_value": 6,
                    "events": [
                        {
                            "max": 4,
                            "timestamp": 1498867200000,
                            "min": 4
                            },
                        {
                            "max": 8,
                            "timestamp": 1501200000000,
                            "min": 8
                            },
                        {
                            "max": 7,
                            "timestamp": 1501480800000,
                            "min": 7
                            },
                        {
                            "max": 7,
                            "timestamp": 1501502400000,
                            "min": 7
                        }
                        ],
                    "percentiles": []
                    },
                {
                    "url": "https://greencoffee.lizard.net/api/v3/timeseries/f82ea472-75ae-45a2-a41c-127069a489e9/?format=json",
                    "id": 201010,
                    "uuid": "f82ea472-75ae-45a2-a41c-127069a489e9",
                    "name": "TMAX",
                    "code": "TMAX",
                    "value_type": "float",
                    "location": {
                        "url": "https://greencoffee.lizard.net/api/v3/locations/395e4c40-107d-4294-9c12-6d046bc5e314/?format=json",
                        "uuid": "395e4c40-107d-4294-9c12-6d046bc5e314",
                        "name": "67_663_24667",
                        "code": "67_663_24667",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                107.6201020496574,
                                12.57096549076355,
                                0
                            ]
                        }
                        },
                    "observation_type": {
                        "url": "https://greencoffee.lizard.net/api/v3/observationtypes/543/?format=json",
                        "code": "TMAX",
                        "parameter": "Maximum Temperature",
                        "unit": "oC",
                        "scale": "interval",
                        "description": "",
                        "reference_frame": "",
                        "compartment": ""
                        },
                    "access_modifier": "Private",
                    "supplier_code": "TMAX",
                    "start": 1498780800000,
                    "end": 1502409600000,
                    "last_value": 28,
                    "events": [
                        {
                            "max": 28,
                            "timestamp": 1498867200000,
                            "min": 28
                            },
                        {
                            "max": 24,
                            "timestamp": 1501200000000,
                            "min": 24
                            },
                        {
                            "max": 27,
                            "timestamp": 1501502400000,
                            "min": 27
                        }
                        ],
                    "percentiles": []
                    },
                {
                    "url": "https://greencoffee.lizard.net/api/v3/timeseries/bc92f6ae-1881-4fa4-a409-a5df37fd7d47/?format=json",
                    "id": 201011,
                    "uuid": "bc92f6ae-1881-4fa4-a409-a5df37fd7d47",
                    "name": "TMIN",
                    "code": "TMIN",
                    "value_type": "float",
                    "location": {
                        "url": "https://greencoffee.lizard.net/api/v3/locations/395e4c40-107d-4294-9c12-6d046bc5e314/?format=json",
                        "uuid": "395e4c40-107d-4294-9c12-6d046bc5e314",
                        "name": "67_663_24667",
                        "code": "67_663_24667",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                107.6201020496574,
                                12.57096549076355,
                                0
                            ]
                        }
                        },
                    "observation_type": {
                        "url": "https://greencoffee.lizard.net/api/v3/observationtypes/548/?format=json",
                        "code": "TMIN",
                        "parameter": "Minimum Temperature",
                        "unit": "oC",
                        "scale": "interval",
                        "description": "",
                        "reference_frame": "",
                        "compartment": ""
                        },
                    "access_modifier": "Private",
                    "supplier_code": "TMIN",
                    "start": 1498780800000,
                    "end": 1502409600000,
                    "last_value": 21,
                    "events": [
                        {
                            "max": 21,
                            "timestamp": 1498867200000,
                            "min": 21
                            },
                        {
                            "max": 21,
                            "timestamp": 1501480800000,
                            "min": 21
                            },
                        {
                            "max": 21,
                            "timestamp": 1501502400000,
                            "min": 21
                        }
                        ],
                    "percentiles": []
                    },
                {
                    "url": "https://greencoffee.lizard.net/api/v3/timeseries/2213a72e-a880-47bd-81d0-10850b157448/?format=json",
                    "id": 201012,
                    "uuid": "2213a72e-a880-47bd-81d0-10850b157448",
                    "name": "WNDDIR",
                    "code": "WNDDIR",
                    "value_type": "float",
                    "location": {
                        "url": "https://greencoffee.lizard.net/api/v3/locations/395e4c40-107d-4294-9c12-6d046bc5e314/?format=json",
                        "uuid": "395e4c40-107d-4294-9c12-6d046bc5e314",
                        "name": "67_663_24667",
                        "code": "67_663_24667",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                107.6201020496574,
                                12.57096549076355,
                                0
                            ]
                        }
                        },
                    "observation_type": {
                        "url": "https://greencoffee.lizard.net/api/v3/observationtypes/557/?format=json",
                        "code": "WNDDIR",
                        "parameter": "Wind Direction",
                        "unit": "degree",
                        "scale": "interval",
                        "description": "",
                        "reference_frame": "",
                        "compartment": ""
                        },
                    "access_modifier": "Private",
                    "supplier_code": "WNDDIR",
                    "start": 1498780800000,
                    "end": 1502409600000,
                    "last_value": 263,
                    "events": [
                        {
                            "max": 226,
                            "timestamp": 1498867200000,
                            "min": 226
                            },
                        {
                            "max": 235,
                            "timestamp": 1498953600000,
                            "min": 235
                            },
                        {
                            "max": 235,
                            "timestamp": 1501502400000,
                            "min": 235
                        }
                        ],
                    "percentiles": []
                    },
                #{
                    #"url": "https://greencoffee.lizard.net/api/v3/timeseries/a28357b3-12f7-49ca-98e3-f2b317b25075/?format=json",
                    #"id": 201013,
                    #"uuid": "a28357b3-12f7-49ca-98e3-f2b317b25075",
                    #"name": "WNDSPD",
                    #"code": "WNDSPD",
                    #"value_type": "float",
                    #"location": {
                        #"url": "https://greencoffee.lizard.net/api/v3/locations/395e4c40-107d-4294-9c12-6d046bc5e314/?format=json",
                        #"uuid": "395e4c40-107d-4294-9c12-6d046bc5e314",
                        #"name": "67_663_24667",
                        #"code": "67_663_24667",
                        #"geometry": {
                            #"type": "Point",
                            #"coordinates": [
                                #107.6201020496574,
                                #12.57096549076355,
                                #0
                            #]
                        #}
                        #},
                    #"observation_type": {
                        #"url": "https://greencoffee.lizard.net/api/v3/observationtypes/554/?format=json",
                        #"code": "WNDSPD",
                        #"parameter": "Wind Speed",
                        #"unit": "m/s",
                        #"scale": "interval",
                        #"description": "",
                        #"reference_frame": "",
                        #"compartment": ""
                        #},
                    #"access_modifier": "Private",
                    #"supplier_code": "WNDSPD",
                    #"start": 1498780800000,
                    #"end": 1502409600000,
                    #"last_value": 2,
                    #"events": [
                        #{
                            #"max": 2,
                            #"timestamp": 1498867200000,
                            #"min": 2
                            #},
                        #{
                            #"max": 2,
                            #"timestamp": 1501394400000,
                            #"min": 2
                            #},
                        #{
                            #"max": 2,
                            #"timestamp": 1501502400000,
                            #"min": 2
                        #}
                        #],
                    #"percentiles": []
                #}
            ]
        }
        
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
        self.mock_greencoffeelizardv3_response2 = json.dumps(response_body2)        
        


    @inlineCallbacks
    def test_outbound_get_content_for_agent_price(self):
        self.mock_response()
        
        yield self.tx_helper.make_dispatch_outbound(
            #to_addr="https://greencoffee.lizard.net/api/v3/search/",
            to_addr="%sapi/v3/search/" % self.mock_greencoffeelizardv3.url,
            from_addr="myprogram",
            #content="weatherall X. Kroong",
            content="coffeea X. Kroong",
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
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        #self.assertEqual(event['user_message_id'], '1')
        #self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})
        
        
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
        #req = yield self.mock_greencoffeelizard_response
        #reqdic = ast.literal_eval(req)
        #self.assertEqual(
            #reqdic['events'],
            #[{"timestamp": 1484784000000, "value": 46700}, {"timestamp": 1487289600000, "value": 45700}, {"timestamp": 1487635200000, "value": 46700}])        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})
