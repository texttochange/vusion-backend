import re, json, sys, traceback, ast
from urlparse import urlparse
from hashlib import sha1
from datetime import datetime, timedelta
from base64 import b64encode
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.transports import Transport
from vumi.utils import http_request_full
from vumi import log

from vusion.error import MissingData


class GreencoffeelizardHttp(Transport):
    
    transport_type = 'http_api'
    
    def setup_transport(self):
        log.msg("Setup Greencoffee lizard http transport %s" % self.config)
        self.transport_metadata = {'transport_type': self.transport_type}

    def teardown_transport(self):
        log.msg("Stop forward http transport")

    def build_data(self, message, labels_to_add):
        param = {}
        for label_to_add in labels_to_add:
            if label_to_add == 'location__organisation__name':
                param['location__organisation__name'] = 'G4AW Green Coffee'
            elif label_to_add == 'location__code':
                keyword, location = message['content'].split()
                param['location__code'] = ast.literal_eval(json.dumps(location))                
            else:
                param['format'] = 'json'                
        return param

    #def get_date(self):
    #    u = datetime.now() - timedelta(days=1)
    #    return datetime.now().strftime('%Y-%m-%d')

    def build_timeseries_data(self):
        data = {}
        prev_time = datetime.now() - timedelta(days=1)
        data['start'] = int(prev_time.strftime('%s'))*1000
        data['end'] = int(datetime.now().strftime('%s'))*1000             
        return data


    #def build_timeseries_response(self, results):
        #response_timeseries_url = {}
        #for result in results:
            #response_timeseries_url = result['url']
        #return response_timeseries_url
    
    def build_timeseries_response(self, results):
            response_timeseries_url = {}
            for result in results:
                response_timeseries_url = result['url']
                observation_type = result['observation_type']['parameter_short_display_name']
                if observation_type.endswith('Agent Price'):
                    return response_timeseries_url
                else:
                    return

    def build_message_content(self, events):
            event_contents = {}
            for event in events:
                event_contents = event
            return event_contents 
    
    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outboung message to be processed %s" % repr(message))
        try:
            url = message['to_addr']
            url = urlparse(url)
            forward_url = "%s://%s%s" % (url.scheme, url.netloc, url.path)

            params = {} 
            if url.path in self.config['api']:
                params = self.build_data(message, self.config['api'][url.path])

            log.msg('Hitting %s with %s' % (forward_url, urlencode(params)))

            response = yield http_request_full(
                "%s?%s" % (forward_url.encode(), urlencode(params)),
                headers={'Content-Type': 'application/json',
                 'username': self.config['username'],
                 'password': self.config['password']},
                method='GET') 
            
            if response.code != 200:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
                log.error(reason)
                yield self.publish_nack(
                    message['message_id'], reason,
                    transport_metadata=self.transport_metadata)
                return

            response_body = json.loads(response.delivered_body)
            if response_body['count'] == '0':
                reason = "SERVICE ERROR %s - %s" % (response_body['error'], response_body['message'])
                log.error(reason)
                yield self.publish_nack(
                    message['message_id'], reason,
                    transport_metadata=self.transport_metadata)
                return 
                       
            data_timeseries = {}            
            data_timeseries = self.build_timeseries_data()
            data_timeseries_response = {}
            data_timeseries_response_url = self.build_timeseries_response(response_body['results'])
            
            timeseries_url_response = yield http_request_full(
                    "%s&%s" % (data_timeseries_response_url.encode('ASCII'), urlencode(data_timeseries)),
                    headers={'Content-Type': ['application/json, charset=UTF-8'],
                     'username': [self.config['username']],
                     'password': [self.config['password']]},
                    method='GET')
            
            #if timeseries_url_response.code != 200:
                #reason = "HTTP ERROR %s - %s" % (timeseries_url_responsee.code, timeseries_url_response.delivered_body)
                #log.error(reason)
                #yield self.publish_nack(
                    #message['message_id'], reason,
                    #transport_metadata=self.transport_metadata)
                #return
                
            response_timeseries_body = json.loads(timeseries_url_response.delivered_body)
            if not response_timeseries_body['events']:
                yield self.publish_message(
                    message_id=message['message_id'],
                    content='NO Prices yet try later', 
                    to_addr=message['transport_metadata']['participant_phone'],           
                    from_addr=message['transport_metadata']['program_shortcode'],        
                    provider='greencoffee',
                    transport_type='http')                
            else:
                message_content = self.build_message_content(response_timeseries_body['events'])         
                yield self.publish_message(
                   message_id=message['message_id'],
                   content='coffeer %s %s' % (datetime.fromtimestamp(message_content['timestamp']/1000).strftime('%Y-%m-%d %H:%M'), message_content['value']), 
                   to_addr=message['transport_metadata']['participant_phone'],           
                   from_addr=message['transport_metadata']['program_shortcode'],        
                   provider='greencoffee',
                   transport_type='http')
            
            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                transport_metadata=self.transport_metadata)
        except MissingData as ex:
            reason = "MISSING DATA %s" % ex.message
            yield self.publish_nack(
                message['message_id'], reason,
                transport_metadata=self.transport_metadata)
        except Exception as ex:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "TRANSPORT ERROR: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
            reason = "TRANSPORT ERROR %s" % (ex.message)
            yield self.publish_nack(
                message['message_id'],
                reason,
                transport_metadata=self.transport_metadata)
