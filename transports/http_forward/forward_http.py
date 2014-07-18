import re
import urllib
from urlparse import urlparse, parse_qs

from twisted.internet.defer import inlineCallbacks

from vumi.transports import Transport
from vumi.utils import http_request_full
from vumi import log


## Outgoing only transport, will generate a URL replacing element of the to_addr field
class ForwardHttp(Transport):
    
    def setup_transport(self):
        log.msg("Setup forward http transport %s" % self.config)
        self.message_replacement = self.config['message_replacement']
        self.compile_replacement(self.message_replacement)
        self.message_metadata_replacement = self.config['message_metadata_replacement']
        self.compile_replacement(self.message_metadata_replacement)

    def compile_replacement(self, replacements={}):
        for field, regex_txt in replacements.iteritems():
            replacements[field] = re.compile(regex_txt)

    def replace_arguments_get_url(self, dictionnary, replacement_rules, url):
        for field, regex in replacement_rules.iteritems():
            try:
                url = regex.sub(urllib.quote(dictionnary[field]), url)
            except:
                pass
        return url
    
    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outboung message to be processed %s" % repr(message))
        try:
            url = message['to_addr']
            url = self.replace_arguments_get_url(message, self.message_replacement, url)
            url = self.replace_arguments_get_url(message['transport_metadata'], self.message_metadata_replacement, url)
            url = urlparse(url)
            params = parse_qs(url.query)
            for key, [param] in params.iteritems():
                params[key] = param
            forward_url = "%s://%s%s?%s" % (url.scheme, url.netloc, url.path, urllib.urlencode(params))
            
            log.msg('Hitting %s' % forward_url)
            
            response = yield http_request_full(
                forward_url.encode('ASCII', 'replace'),
                "",
                {'User-Agent': ['Vusion ForwardHttp Transport'],
                 'Content-Type': ['application/json,charset=UTF-8']},
                'GET')
            
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
            
            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                transport_metadata={'transport_type': 'http_forward'})
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