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
            'username': 'joukeuiii',
            'password': 't7W7899kl',
            'api': {
                '/api/v3/timeseries/': [                    
                    'search',
                    'format']},
            'no_prices_keyword': 'NOP',
            'yes_prices_keyword': 'coffeer'
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

    #@inlineCallbacks
    #def test_outbound_get_timeseries_url(self):
        #response_body = {
            #"count": 7,
            #"results": [
                #{
                    #"url": "https://greencoffee.lizard.net/api/v3/timeseries/8b3a1590-a884-4123-85d7-d1031045e4f8/?format=json",
                    #"id": 199801,
                    #"uuid": "8b3a1590-a884-4123-85d7-d1031045e4f8",
                    #"name": "dRBC",
                    #"code": "dRBC",
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
                        #"url": "https://greencoffee.lizard.net/api/v3/observationtypes/567/?format=json",
                        #"code": "dRBC",
                        #"parameter": "Robusta, Bean, Company Price",
                        #"unit": "d/kg",
                        #"scale": "interval",
                        #"description": "",
                        #"domain_values": "",
                        #"reference_frame": "",
                        #"compartment": ""
                    #},
                    #"access_modifier": "Private",
                    #"supplier_code": "dRBC",
                    #"start": 1494892800000,
                    #"end": 1502150400000,
                    #"last_value": 45900,
                    #"events": [
                        #{
                            #"max": 46000,
                            #"timestamp": 1499126400000,
                            #"min": 46000
                        #},
                        #{
                            #"max": 46000,
                            #"timestamp": 1499212800000,
                            #"min": 46000
                        #},
                        #{
                            #"max": 46000,
                            #"timestamp": 1499299200000,
                            #"min": 46000
                        #},                
                        #{
                            #"max": 46000,
                            #"timestamp": 1501200000000,
                            #"min": 46000
                        #}
                    #],
                    #"percentiles": []
                #},
                #{
                    #"url": "https://greencoffee.lizard.net/api/v3/timeseries/5089bdea-0214-4494-b9e2-9a416bd1c675/?format=json",
                    #"id": 199802,
                    #"uuid": "5089bdea-0214-4494-b9e2-9a416bd1c675",
                    #"name": "dRBA",
                    #"code": "dRBA",
                    #"value_type": "float","location": {
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
                        #"url": "https://greencoffee.lizard.net/api/v3/observationtypes/566/?format=json",
                        #"code": "dRBA",
                        #"parameter": "Robusta, Bean, Agent Price",
                        #"unit": "d/kg",
                        #"scale": "interval",
                        #"description": "",
                        #"reference_frame": "",
                        #"compartment": ""
                    #},
                    #"access_modifier": "Private",
                    #"supplier_code": "dRBA",
                    #"start": 1494892800000,
                    #"end": 1502150400000,
                    #"last_value": 45600,
                    #"events": [
                        #{
                            #"max": 45900,
                            #"timestamp": 1499126400000,
                            #"min": 45900
                        #},
                        #{
                            #"max": 45800,
                            #"timestamp": 1499212800000,
                            #"min": 45800
                        #},
                        #{
                            #"max": 44700,
                            #"timestamp": 1501113600000,
                            #"min": 44700
                        #},
                        #{
                            #"max": 45700,
                            #"timestamp": 1501200000000,
                            #"min": 45700
                        #}
                    #],
                    #"percentiles": []
                #},
                #{
                    #"url": "https://greencoffee.lizard.net/api/v3/timeseries/5cd82357-b626-4aa5-929a-7d211eacd6d0/?format=json",
                    #"id": 201009,
                    #"uuid": "5cd82357-b626-4aa5-929a-7d211eacd6d0",
                    #"name": "PRCP",
                    #"code": "PRCP",
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
                        #"url": "https://greencoffee.lizard.net/api/v3/observationtypes/542/?format=json",
                        #"code": "PRCP",
                        #"parameter": "Precipitation",
                        #"unit": "mm",
                        #"scale": "ratio",
                        #"description": "",
                        #"reference_frame": "",
                        #"compartment": ""
                    #},
                    #"access_modifier": "Private",
                    #"supplier_code": "PRCP",
                    #"start": 1498780800000,
                    #"end": 1502409600000,
                    #"last_value": 6,
                    #"events": [
                        #{
                            #"max": 4,
                            #"timestamp": 1498867200000,
                            #"min": 4
                        #},
                        #{
                            #"max": 8,
                            #"timestamp": 1501200000000,
                            #"min": 8
                        #},
                        #{
                            #"max": 7,
                            #"timestamp": 1501480800000,
                            #"min": 7
                        #},
                        #{
                            #"max": 7,
                            #"timestamp": 1501502400000,
                            #"min": 7
                        #}
                    #],
                    #"percentiles": []
                #},
                #{
                    #"url": "https://greencoffee.lizard.net/api/v3/timeseries/f82ea472-75ae-45a2-a41c-127069a489e9/?format=json",
                    #"id": 201010,
                    #"uuid": "f82ea472-75ae-45a2-a41c-127069a489e9",
                    #"name": "TMAX",
                    #"code": "TMAX",
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
                        #"url": "https://greencoffee.lizard.net/api/v3/observationtypes/543/?format=json",
                        #"code": "TMAX",
                        #"parameter": "Maximum Temperature",
                        #"unit": "oC",
                        #"scale": "interval",
                        #"description": "",
                        #"reference_frame": "",
                        #"compartment": ""
                    #},
                    #"access_modifier": "Private",
                    #"supplier_code": "TMAX",
                    #"start": 1498780800000,
                    #"end": 1502409600000,
                    #"last_value": 28,
                    #"events": [
                        #{
                            #"max": 28,
                            #"timestamp": 1498867200000,
                            #"min": 28
                        #},
                        #{
                            #"max": 24,
                            #"timestamp": 1501200000000,
                            #"min": 24
                        #},
                        #{
                            #"max": 27,
                            #"timestamp": 1501502400000,
                            #"min": 27
                        #}
                    #],
                    #"percentiles": []
                #},
                #{
                    #"url": "https://greencoffee.lizard.net/api/v3/timeseries/bc92f6ae-1881-4fa4-a409-a5df37fd7d47/?format=json",
                    #"id": 201011,
                    #"uuid": "bc92f6ae-1881-4fa4-a409-a5df37fd7d47",
                    #"name": "TMIN",
                    #"code": "TMIN",
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
                        #"url": "https://greencoffee.lizard.net/api/v3/observationtypes/548/?format=json",
                        #"code": "TMIN",
                        #"parameter": "Minimum Temperature",
                        #"unit": "oC",
                        #"scale": "interval",
                        #"description": "",
                        #"reference_frame": "",
                        #"compartment": ""
                    #},
                    #"access_modifier": "Private",
                    #"supplier_code": "TMIN",
                    #"start": 1498780800000,
                    #"end": 1502409600000,
                    #"last_value": 21,
                    #"events": [
                        #{
                            #"max": 21,
                            #"timestamp": 1498867200000,
                            #"min": 21
                        #},
                        #{
                            #"max": 21,
                            #"timestamp": 1501480800000,
                            #"min": 21
                        #},
                        #{
                            #"max": 21,
                            #"timestamp": 1501502400000,
                            #"min": 21
                        #}
                    #],
                    #"percentiles": []
                #},
                #{
                    #"url": "https://greencoffee.lizard.net/api/v3/timeseries/2213a72e-a880-47bd-81d0-10850b157448/?format=json",
                    #"id": 201012,
                    #"uuid": "2213a72e-a880-47bd-81d0-10850b157448",
                    #"name": "WNDDIR",
                    #"code": "WNDDIR",
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
                        #"url": "https://greencoffee.lizard.net/api/v3/observationtypes/557/?format=json",
                        #"code": "WNDDIR",
                        #"parameter": "Wind Direction",
                        #"unit": "degree",
                        #"scale": "interval",
                        #"description": "",
                        #"reference_frame": "",
                        #"compartment": ""
                    #},
                    #"access_modifier": "Private",
                    #"supplier_code": "WNDDIR",
                    #"start": 1498780800000,
                    #"end": 1502409600000,
                    #"last_value": 263,
                    #"events": [
                        #{
                            #"max": 226,
                            #"timestamp": 1498867200000,
                            #"min": 226
                        #},
                        #{
                            #"max": 235,
                            #"timestamp": 1498953600000,
                            #"min": 235
                        #},
                        #{
                            #"max": 235,
                            #"timestamp": 1501502400000,
                            #"min": 235
                        #}
                    #],
                    #"percentiles": []
                #},
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
            #]
        #}
        
        #self.mock_greencoffeelizardv3_response = json.dumps(response_body)

        #yield self.tx_helper.make_dispatch_outbound(
            #to_addr="%sapi/v2/timeseries/" % self.mock_greencoffeelizardv3.url,
            #from_addr="myprogram",
            #content="coffee EaTam54",
            #message_id='1',
            #transport_metadata={
                #'program_shortcode': '256-8281',
                #'participant_phone': '+6',
                #'participant_profile': [
                    #{'label': 'reporterid',
                     #'value': '708'}]})
        #req = yield self.mock_greencoffeelizardv3_response
        #reqdic = ast.literal_eval(req)
        #self.assertEqual(
            #reqdic['count'], 7)
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
