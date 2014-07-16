import re, json
from urlparse import urlparse
from hashlib import sha1
from datetime import datetime
from base64 import b64encode

from twisted.internet.defer import inlineCallbacks

from vumi.transports import Transport
from vumi.utils import http_request_full
from vumi import log

from vusion.error import MissingData


class CioecHttp(Transport):
    
    def setup_transport(self):
        log.msg("Setup embolivia http transport %s" % self.config)

    def build_data(self, message, labels_to_add):
        data = {}
        for label_to_add in labels_to_add:
            if label_to_add == 'phone':
                data['phone'] = message['transport_metadata']['participant_phone']
            elif label_to_add == 'message':
                data['message'] = message['content']                
            else:
                item = [x for x in  message['transport_metadata']['participant_profile'] if label_to_add == x['label']]
                if item == []:
                    raise MissingData("%s is missing" % label_to_add)
                data[item[0]['label']] = item[0]['value']
        return {'data': [data]}

    def get_date(self):
        return datetime.now().strftime('%Y-%m-%d')

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outboung message to be processed %s" % repr(message))
        try:
            url = message['to_addr']
            url = urlparse(url)
            forward_url = "%s://%s%s" % (url.scheme, url.netloc, url.path)

            log.msg('Hitting %s' % forward_url)

            data = {} 
            if url.path in self.config['api']:
                data = self.build_data(message, self.config['api'][url.path])
            
            auth = sha1('%s%s%s' % (self.config['api_key'], self.config['salt'], self.get_date()))
            auth = b64encode("%s:api_token" % auth.hexdigest())
            
            response = yield http_request_full(
                forward_url.encode('ASCII'),
                json.dumps(data),
                {'User-Agent': ['Vusion Cioec Transport'],
                 'Content-Type': ['application/json,charset=UTF-8'],
                 'Authorization': ['Basic %s' % auth]},
                'POST')

            if response.code != 200:
                log.msg("Http Error %s: %s"
                        % (response.code, response.delivered_body))
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='http',
                    failure_code=response.code,
                    failure_reason=response.delivered_body,
                    transport_metadata={'transport_type':'http_forward'})
                return
            response_body = json.loads(response.delivered_body)
            if response_body['status'] == 'fail':
                log.msg("Service Error: %s" % response.delivered_body)
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='service',
                    failure_code=response_body['data']['error'],
                    failure_reason=response_body['data']['message'],
                    transport_metadata={'transport_type':'http_forward'})
                return                
            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                transport_metadata={'transport_type': 'http_forward'})
        except MissingData as ex:
            log.msg("Missing Data error %s" % repr(ex))
            yield self.publish_delivery_report(
                user_message_id=message['message_id'],
                delivery_status='failed',
                failure_level='transport',
                failure_code=None,
                failure_reason=ex.message,
                transport_metadata={'transport_type':'http_forward'})            
        except Exception as ex:
            log.msg("Unexpected error %s" % repr(ex))
            yield self.publish_delivery_report(
                user_message_id=message['message_id'],
                delivery_status='failed',
                failure_level='transport',
                failure_code=None,
                failure_reason=repr(ex),
                transport_metadata={'transport_type':'http_forward'})

    def stopWorker(self):
        log.msg("stop forward http transport")
