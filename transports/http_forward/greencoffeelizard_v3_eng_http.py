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


class GreencoffeelizardV3engHttp(Transport):
    
    transport_type = 'http_api'
    
    def setup_transport(self):
        log.msg("Setup Greencoffee V3 lizard http transport %s" % self.config)
        self.transport_metadata = {'transport_type': self.transport_type}

    def teardown_transport(self):
        log.msg("Stop forward http transport")

    def build_data(self, message, labels_to_add):
        param = {}
        for label_to_add in labels_to_add:
            if label_to_add == 'q':
                location = message['content'].split(' ', 1)[1]
                param['q'] = ast.literal_eval(json.dumps(location))                
            else:
                param['format'] = 'json'                
        return param
    
    def build_data_location_code(self, location_results):
        location={}
        location['search'] = location_results
        location['format'] = 'json'
        return location
    
    def build_timeseries_data(self):
        data = {}
        prev_time = datetime.now() - timedelta(days=3)
        data['start'] = int(prev_time.strftime('%s'))*1000
        data['end'] = int(datetime.now().strftime('%s'))*1000             
        return data

    def build_location_code_url_response(self, results, keyword_sent):
            #response_timeseries_url = {}
            response_dic_content = {}
            for result in results:
                observation_type = result['observation_type']['parameter']
                if keyword_sent == self.config.get('yes_agent_prices_keyword'):
                    if observation_type.endswith('Agent Price'):
                        response_dic_content['AgentPrice'] = self.build_message_content(result['events'])
                        return response_dic_content
                elif keyword_sent == self.config.get('yes_company_prices_keyword'):
                    if observation_type.endswith('Company Price'):
                        response_dic_content['CompanyPrice'] = self.build_message_content(result['events'])
                        return response_dic_content
                elif keyword_sent == self.config.get('yes_weather_keyword'):
                    if observation_type == 'Precipitation':
                        self.build_message_content(result['events'])['scale'] = 'mm'
                        response_dic_content['Luong mua'] = self.build_message_content(result['events'])
                    elif observation_type == 'Maximum Temperature':
                        self.build_message_content(result['events'])['scale'] = 'C'
                        response_dic_content['Nhiet do toi da'] = self.build_message_content(result['events'])
                    elif observation_type == 'Minimum Temperature':
                        self.build_message_content(result['events'])['scale'] = 'C'
                        response_dic_content['Nhiet do toi thieu'] = self.build_message_content(result['events'])
                    elif observation_type == 'Wind Direction':
                        response_dic_content['Huong gio'] = self.build_message_content(result['events'])
                    elif observation_type == 'Wind Speed':
                        self.build_message_content(result['events'])['scale'] = 'km/h'
                        response_dic_content['Toc do gio'] = self.build_message_content(result['events'])
            return response_dic_content

    def build_message_content(self, events):
            event_contents = {}
            for event in events:
                event_contents = event
            return event_contents
    
    def build_message_content_cond(self, event_contents):
        message_content = {}
        for k, v in event_contents.iteritems():
            if 'scale' in v:
                message_content[k] = "%s%s" % (v['max'], v['scale'])
            else:
                message_content[k] = v['max']
        content_gene = ', '.join("%s: %r" % (key,val) for (key,val) in message_content.iteritems())
        return content_gene        
    
    
    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outboung message testing to be processed %s" % repr(message))
        try:
            url = message['to_addr']
            url = urlparse(url)
            forward_url = "%s://%s%s" % (url.scheme, url.netloc, url.path)

            params = {} 
            if url.path in self.config['api']:
                params = self.build_data(message, self.config['api'][url.path])

            log.msg('Hitting %s with %s' % (forward_url, urlencode(params)))
            
            response_loc_code = yield http_request_full(
                "%s?%s" % (forward_url.encode(), urlencode(params)),
                headers={'Content-Type': 'application/json',
                         'username': self.config.get('username'),
                         'password': self.config.get('password')},
                method='GET')

            if response_loc_code.code != 200:
                reason = "HTTP ERROR %s - %s" % (response_loc_code.code, response_loc_code.delivered_body)
                log.error(reason)
                yield self.publish_nack(
                    message['message_id'], reason,
                    transport_metadata=self.transport_metadata)
                return

            response_loc_code_body = json.loads(response_loc_code.delivered_body)
            log.msg('location code BODY %s' % response_loc_code_body)
            
            if message['transport_metadata']['program_shortcode'].startswith('+'):
                shortcode = message['transport_metadata']['program_shortcode']
            else:
                country, shortcode  = message['transport_metadata']['program_shortcode'].split('-')            

            if response_loc_code_body['count'] == 0:                
                yield self.publish_message(
                    message_id=message['message_id'],
                    content=self.config.get('no_count_loc_code_keyword'),
                    to_addr=shortcode,           
                    from_addr=message['transport_metadata']['participant_phone'],        
                    provider='greencoffee',
                    transport_type='http')
                return

            data_timeseries = {}
            data_timeseries = self.build_timeseries_data()
            data_location_code = {}
            data_location_code = self.build_data_location_code(response_loc_code_body['results'][0]['description'])

            log.msg('loctioncode data: %s and Time: %s' % (data_location_code, data_timeseries))
            response = yield http_request_full(
                "%s?%s&%s" % (self.config.get('api_url_timeseries').encode(),
                           urlencode(data_location_code),
                           urlencode(data_timeseries)),
                headers={'Content-Type': 'application/json',
                         'username': self.config.get('username'),
                         'password': self.config.get('password')},
                method='GET')
            
            if response.code != 200:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
                log.error(reason)
                yield self.publish_nack(
                    message['message_id'],
                    reason,
                    transport_metadata=self.transport_metadata)
                return            
            
            response_body = json.loads(response.delivered_body)            
            
            if response_body['count'] == 0:
                yield self.publish_message(
                    message_id=message['message_id'],
                    content=self.config.get('no_count_results_keyword'),
                    to_addr=shortcode,           
                    from_addr=message['transport_metadata']['participant_phone'],        
                    provider='greencoffee',
                    transport_type='http')
            else:
                keyword_sent = message['content'].split(' ', 1)[0].lower()
                keywords_inuse = [self.config.get('yes_agent_prices_keyword'),
                                  self.config.get('yes_company_prices_keyword'),
                                  self.config.get('yes_weather_keyword')]
                if keyword_sent in keywords_inuse:
                    event_contents = self.build_location_code_url_response(response_body['results'], keyword_sent)
                    log.msg('Event contents %s' % event_contents)
                    message_content = self.build_message_content_cond(event_contents)
                    for k, v in event_contents.iteritems():
                                message_timestamp = v['timestamp']
                    
                    if keyword_sent == self.config.get('yes_weather_keyword'):
                        yield self.publish_message(
                            message_id=message['message_id'],
                            content='%s Du bao thoi tiet %s tai %s: %s' % (self.config.get('yes_feedback_keyword'),
                                                 datetime.fromtimestamp(message_timestamp/1000).strftime('%Y-%m-%d %H:%M'),
                                                 message['content'].split(' ', 1)[1],
                                                 message_content),
                            to_addr=shortcode,
                            from_addr=message['transport_metadata']['participant_phone'],
                            provider='greencoffee',
                            transport_type='http')
                    else:
                        yield self.publish_message(
                            message_id=message['message_id'],
                            content='%s %s tai %s: %s' % (self.config.get('yes_feedback_keyword'),
                                                datetime.fromtimestamp(message_timestamp/1000).strftime('%Y-%m-%d %H:%M'),
                                                message['content'].split(' ', 1)[1],
                                                message_content),
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
