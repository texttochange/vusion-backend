import re, json, sys, traceback
from urlparse import urlparse
from hashlib import sha1
from datetime import datetime
from base64 import b64encode

from twisted.internet.defer import inlineCallbacks

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
        return {'data': [data]}

    def extract_data_from_profile(self, data, participant_profile, participant_tags, label_rule):
        label = None
        default = None
        if isinstance(label_rule, dict):
            label = label_rule['label']
            default = label_rule['default']
        else:
            label = label_rule
            tag = participant_tags
            item = []
            for index, profile in enumerate(participant_profile):
                if label == 'answer':                                
                    profile['value'] = tag[index]
                    profile['label'] = label
                    item = [profile]                
                if profile['label'][:6] == 'Answer' and label == 'question':
                    profile['value'] = profile['label'][6:]
                    profile['label'] = label                    
                    item = [profile]
                if profile['label'][:6] == 'report' and label == 'reporter':
                    profile['label'] = label
                    item = [profile]            
        #item = [x for x in  participant_profile if label == x['label']]
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
    def handle_outbound_message(self, message):
        log.msg("Outboung message to be processed %s" % repr(message))
        try:
            url = message['to_addr']
            url = urlparse(url)
            forward_url = "%s://%s%s" % (url.scheme, url.netloc, url.path)

            data = {} 
            if url.path in self.config['api']:
                data = self.build_data(message, self.config['api'][url.path])
            
            #auth = sha1('%s%s%s' % (self.config['api_key'], self.config['salt'], self.get_date()))
            auth = self.config['api_key']
            auth = b64encode("%s" % auth)

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
