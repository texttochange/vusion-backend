import re, json, sys, traceback, ast
from urlparse import urlparse
from hashlib import sha1
from datetime import datetime
from base64 import b64encode
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.transports import Transport
from vumi.utils import http_request_full
from vumi import log

from vusion.error import MissingData


class AskpeopleHttp(Transport):
    
    transport_type = 'http_api'
    
    def setup_transport(self):
        log.msg("Setup Askpeople http transport %s" % self.config)
        self.transport_metadata = {'transport_type': self.transport_type}

    def teardown_transport(self):
        log.msg("Stop forward http transport")

    def build_data(self, message, labels_to_add):
        log.msg('build data Hitting %s ' % (labels_to_add))
        data = {}
        for label_to_add in labels_to_add:
            if label_to_add == 'message':
                data['message'] = message['content']                
            else:
                self.extract_data_from_profile(
                    data,
                    message['transport_metadata']['participant_profile'],
                    message['transport_metadata']['participant_tags'],
                    label_to_add)
                if 'answer' in data and 'answer_text' in data:
                    data.pop('answer_text', 1)
        return [data]

    def extract_data_from_profile(self, data, participant_profile, participant_tags, label_rule):
        label = 'answer_text'
        default = None
        if isinstance(label_rule, dict):
            label = label_rule['label']
            default = label_rule['default']
        else:
            label = label_rule
            tags = participant_tags
            item = []
            profile2 = {}
            for index, profile in enumerate(participant_profile):
                if label == 'answer' or label == 'answer_text':
                    if tags[index] == 'free':
                        data.pop('answer', 0)
                        profile['value'] = profile['value']
                        profile['label'] = 'answer_text'
                    else:
                        profile['value'] = tags[index]
                        profile['label'] = label
                    item = [profile]                                  
                if profile['label'][:6] == 'Answer' and label == 'question':
                    profile2['value'] = profile['label'][6:]
                    profile2['label'] = label                    
                    item = [profile2]
                if profile['label'][:6] == 'report' and label == 'reporter':
                    profile['label'] = label
                    item = [profile]
        if item == []:
            if default is None:
                raise MissingData("%s is missing" % label)
            else:
                data[label] = default
        else:
            data[label] = item[0]['value']

    def get_date(self):
        return datetime.now().strftime('%Y-%m-%d')

    @inlineCallbacks
    def get_auth_header(self):
        url = "http://ask-people.vizzuality.com/api/auth"
        params = {'email': self.config['email'],
                  'password': self.config['password']} 
        response = yield http_request_full(
                url,
                urlencode(params),
                {'Content-Type': 'application/x-www-form-urlencoded'},
                'POST')
        
        data = ast.literal_eval(response.delivered_body)
        returnValue(data['auth_token'])

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outboung message to be processed %s" % repr(message))
        try:
            url = message['to_addr']
            url = urlparse(url)
            forward_url = "%s://%s%s" % (url.scheme, url.netloc, url.path)

            data = {} 
            if url.path in self.config['api']:
                data = self.build_data(message, self.config['api'][url.path])
            
            if self.config['api_key'] == 'a2edrfaQ':
                auth = self.config['api_key']
            else:
                auth = yield self.get_auth_header()

            log.msg('Hitting %s with %s' % (forward_url, json.dumps(data)))
            
            response = yield http_request_full(
                forward_url.encode('ASCII'),
                json.dumps(data),
                {'User-Agent': ['Vusion Askpeople Transport'],
                 'Content-Type': ['application/json,charset=UTF-8'],
                 'Authorization': ['Basic %s' % auth]},
                'POST')

            if response.code != 200:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
                log.error(reason)
                yield self.publish_nack(
                    message['message_id'], reason,
                    transport_metadata=self.transport_metadata)
                return
            
            response_body = json.loads(response.delivered_body)
            if response_body['status'] == 'fail':
                reason = "SERVICE ERROR %s - %s" % (response_body['error'], response_body['message'])
                log.error(reason)
                yield self.publish_nack(
                    message['message_id'], reason,
                    transport_metadata=self.transport_metadata)
                return

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
