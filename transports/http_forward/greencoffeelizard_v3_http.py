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


class GreencoffeelizardV3Http(Transport):
    
    transport_type = 'http_api'
    
    def setup_transport(self):
        log.msg("Setup Greencoffee V3 lizard http transport %s" % self.config)
        self.transport_metadata = {'transport_type': self.transport_type}

    def teardown_transport(self):
        log.msg("Stop forward http transport")

    def build_data(self, message, labels_to_add):
        param = {}
        for label_to_add in labels_to_add:
            if label_to_add == 'search':
                keyword, location = message['content'].split()
                param['search'] = ast.literal_eval(json.dumps(location))                
            else:
                param['format'] = 'json'                
        return param

    
    def build_timeseries_data(self):
        data = {}
        prev_time = datetime.now() - timedelta(days=10)
        data['start'] = int(prev_time.strftime('%s'))*1000
        data['end'] = int(datetime.now().strftime('%s'))*1000             
        return data


    def build_timeseries_response(self, results):
            response_timeseries_url = {}
            for result in results:
                response_timeseries_url = result['url']
                observation_type = result['observation_type']['parameter']
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
            
            data_timeseries = {}            
            data_timeseries = self.build_timeseries_data()

            response = yield http_request_full(
                "%s?%s&%s" % (forward_url.encode(), urlencode(params), urlencode(data_timeseries)),
                headers={'Content-Type': 'application/json'},
                method='GET') 
            
            if response.code != 200:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
                log.error(reason)
                yield self.publish_nack(
                    message['message_id'], reason,
                    transport_metadata=self.transport_metadata)
                return

            response_body = json.loads(response.delivered_body)
            log.msg('CALL BODY %s PARMS %s AND ' % (response_body, urlencode(params), urlencode(data_timeseries)))
            
            if response_body['count'] == '0':
                reason = "SERVICE ERROR %s - %s" % (response_body['error'], response_body['message'])
                log.error(reason)
                yield self.publish_nack(
                    message['message_id'], reason,
                    transport_metadata=self.transport_metadata)
                return 
                       
            #data_timeseries = {}            
            #data_timeseries = self.build_timeseries_data()
            #data_timeseries_response = {}
            #data_timeseries_response_url = self.build_timeseries_response(response_body['results'])
            
            if message['transport_metadata']['program_shortcode'].startswith('+'):
                shortcode = message['transport_metadata']['program_shortcode']
            else:
                country, shortcode  = message['transport_metadata']['program_shortcode'].split('-')
            if not response_body['events']:
                yield self.publish_message(
                    message_id=message['message_id'],
                    content=self.config['no_prices_keyword'], 
                    to_addr=shortcode,           
                    from_addr=message['transport_metadata']['participant_phone'],        
                    provider='greencoffee',
                    transport_type='http')                
            else:
                message_content = self.build_message_content(response_body['events'])         
                log.msg("MESSAGE EVENT %s" % message_content)
                yield self.publish_message(
                   message_id=message['message_id'],
                   content='%s %s %s' % (self.config['yes_prices_keyword'], datetime.fromtimestamp(message_content['timestamp']/1000).strftime('%Y-%m-%d %H:%M'), message_content['value']), 
                   to_addr=shortcode,           
                   from_addr=message['transport_metadata']['participant_phone'],        
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
