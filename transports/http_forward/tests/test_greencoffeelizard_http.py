import json
from hashlib import sha1
from datetime import datetime
from base64 import b64encode
from urlparse import urlparse

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import MockHttpServer, VumiTestCase
from vumi.tests.utils import RegexMatcher

from transports import GreencoffeelizardHttp


class GreencoffeelizardHttpTransportTestCase(VumiTestCase):
        
    @inlineCallbacks
    def setUp(self):
        self.greencoffeelizard_calls = DeferredQueue()
        self.greencoffeelizard_calls_body = []
        self.mock_greencoffeelizard = MockHttpServer(self.handle_request)
        self.mock_greencoffeelizard_response = ''
        self.mock_greencoffeelizard_response2 = ''
        self.mock_greencoffeelizard_response_code = http.OK
        yield self.mock_greencoffeelizard.start()
        
        self.config = {
            'username': 'joukemax',
            'password': 't7888000op',
            'api': {
                '/api/v2/timeseries/': [                    
                    'location__organisation__name',
                    'location__code',
                    'format']}
        }
        self.tx_helper = self.add_helper(TransportHelper(GreencoffeelizardHttp))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport.get_date = lambda: "2014-06-09"

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_greencoffeelizard.stop()
        yield super(GreencoffeelizardHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.greencoffeelizard_calls.put(request)
        self.greencoffeelizard_calls_body.append(request.content.read())
        request.setResponseCode(self.mock_greencoffeelizard_response_code)
        return self.mock_greencoffeelizard_response

    @inlineCallbacks
    def test_outbound_get_timeseries_url(self):
        response_body = {
            "count": 2,
            "results": [
                {
                    "url": "https://greencoffee.lizard.net/api/v2/timeseries/f440e143-c8f1-478d-b7b2-0275bcc6040a/?format=json",
                    "id": 194339,
                    "uuid": "f440e143-c8f1-478d-b7b2-0275bcc6040a",
                    "name": "dRBA",
                    "organisation_code": "dRBA",
                    "observation_type":{
                        "url":"https://greencoffee.lizard.net/api/v2/parameterreferencedunits/566/?format=json",
                        "code":"dRBA",
                        "parameter_short_display_name":"Robusta, Bean, Agent Price",
                        "referenced_unit_short_display_name":"d/kg","scale":"interval","description":"",
                        "reference_frame":"",
                        "compartment":""
                    },
                },
                
            {
                "url": "https://greencoffee.lizard.net/api/v2/timeseries/fa22c9f2-08ee-4c12-8008-012722c3fa8c/?format=json",
                "id": 194340,
                "uuid": "fa22c9f2-08ee-4c12-8008-012722c3fa8c",
                "name": "dRBC",
                "organisation_code": "dRBC",
                "observation_type":{
                    "url":"https://greencoffee.lizard.net/api/v2/parameterreferencedunits/567/?format=json",
                    "code":"dRBC",
                    "parameter_short_display_name":"Robusta, Bean, Company Price",
                    "referenced_unit_short_display_name":"d/kg",
                    "scale":"interval","description":"",
                    "reference_frame":"","compartment":""
                },
            }
          ]
        }
        
        self.mock_greencoffeelizard_response = json.dumps(response_body)

        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/v2/timeseries/" % self.mock_greencoffeelizard.url,
            #to_addr= "https://greencoffee.lizard.net/api/v2/timeseries/?",
            from_addr="myprogram",
            content="coffee EaTam54",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8281',
                'participant_phone': '+6',
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'}]})
        req = yield self.mock_greencoffeelizard_response
        #self.assert_authentication(req)
        req_body = self.greencoffeelizard_calls_body.pop()
        self.assertEqual(
            req_body,
            'location__code=EaTam54&location__organisation__name=G4AW+Green+Coffee&format=json')
        self.assertEqual(
                    req,
                    '{"count": 2, "results": [{"name": "dRBA", "url": "https://greencoffee.lizard.net/api/v2/timeseries/f440e143-c8f1-478d-b7b2-0275bcc6040a/?format=json", "id": 194339, "organisation_code": "dRBA", "observation_type": {"reference_frame": "", "code": "dRBA", "scale": "interval", "url": "https://greencoffee.lizard.net/api/v2/parameterreferencedunits/566/?format=json", "referenced_unit_short_display_name": "d/kg", "compartment": "", "parameter_short_display_name": "Robusta, Bean, Agent Price", "description": ""}, "uuid": "f440e143-c8f1-478d-b7b2-0275bcc6040a"}, {"name": "dRBC", "url": "https://greencoffee.lizard.net/api/v2/timeseries/fa22c9f2-08ee-4c12-8008-012722c3fa8c/?format=json", "id": 194340, "organisation_code": "dRBC", "observation_type": {"reference_frame": "", "code": "dRBC", "scale": "interval", "url": "https://greencoffee.lizard.net/api/v2/parameterreferencedunits/567/?format=json", "referenced_unit_short_display_name": "d/kg", "compartment": "", "parameter_short_display_name": "Robusta, Bean, Company Price", "description": ""}, "uuid": "fa22c9f2-08ee-4c12-8008-012722c3fa8c"}]}')        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})
        
        
    @inlineCallbacks
    def test_outbound_get_timeseries_value(self):
        response_body = {
            "count": 2,
            "results": [
                {
                    "url": "https://greencoffee.lizard.net/api/v2/timeseries/f440e143-c8f1-478d-b7b2-0275bcc6040a/?format=json",
                    "id": 194339,
                    "uuid": "f440e143-c8f1-478d-b7b2-0275bcc6040a",
                    "name": "dRBA",
                    "organisation_code": "dRBA",
                    "observation_type":{
                        "url":"https://greencoffee.lizard.net/api/v2/parameterreferencedunits/566/?format=json",
                        "code":"dRBA",
                        "parameter_short_display_name":"Robusta, Bean, Agent Price",
                        "referenced_unit_short_display_name":"d/kg","scale":"interval","description":"",
                        "reference_frame":"",
                        "compartment":""
                    },
                },
                
            {
                "url": "https://greencoffee.lizard.net/api/v2/timeseries/fa22c9f2-08ee-4c12-8008-012722c3fa8c/?format=json",
                "id": 194340,
                "uuid": "fa22c9f2-08ee-4c12-8008-012722c3fa8c",
                "name": "dRBC",
                "organisation_code": "dRBC",
                "observation_type":{
                    "url":"https://greencoffee.lizard.net/api/v2/parameterreferencedunits/567/?format=json",
                    "code":"dRBC",
                    "parameter_short_display_name":"Robusta, Bean, Company Price",
                    "referenced_unit_short_display_name":"d/kg",
                    "scale":"interval","description":"",
                    "reference_frame":"","compartment":""
                },
            }
            ]
        }
        self.mock_greencoffeelizard_response = json.dumps(response_body)
        
        timeseries_url_response = {
            "url": "https://greencoffee.lizard.net/api/v2/timeseries/fa22c9f2-08ee-4c12-8008-012722c3fa8c/?format=json",
            "id": 194340,
            "uuid": "fa22c9f2-08ee-4c12-8008-012722c3fa8c",
            "name": "dRBC",
            "events": [
                {
                    "timestamp": 1484784000000,
                    "value": 46700
                },
                {
                    "timestamp": 1487289600000,
                    "value": 45700
                },
                {
                    "timestamp": 1487635200000,
                    "value": 46700
                }
            ]
        }
        
        self.mock_greencoffeelizard_response2 = json.dumps(timeseries_url_response)

        yield self.tx_helper.make_dispatch_outbound(
            to_addr="%sapi/v2/timeseries/" % self.mock_greencoffeelizard.url,
            #to_addr= "https://greencoffee.lizard.net/api/v2/timeseries/?",
            from_addr="myprogram",
            content="coffee EaTam344",
            message_id='1',
            transport_metadata={
                'program_shortcode': '256-8281',
                'participant_phone': '+6',
                'participant_profile': [
                    {'label': 'reporterid',
                     'value': '708'}]})
        req = yield self.mock_greencoffeelizard_response2
        #self.assert_authentication(req)
        req_body = self.greencoffeelizard_calls_body.pop()
        self.assertEqual(
            req_body,
           'location__code=EaTam344&location__organisation__name=G4AW+Green+Coffee&format=json')
        self.assertEqual(
            req,
            '{"url": "https://greencoffee.lizard.net/api/v2/timeseries/fa22c9f2-08ee-4c12-8008-012722c3fa8c/?format=json", "events": [{"timestamp": 1484784000000, "value": 46700}, {"timestamp": 1487289600000, "value": 45700}, {"timestamp": 1487635200000, "value": 46700}], "id": 194340, "name": "dRBC", "uuid": "fa22c9f2-08ee-4c12-8008-012722c3fa8c"}')        
        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['transport_metadata'], {'transport_type':'http_api'})
