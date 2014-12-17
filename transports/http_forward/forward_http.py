import re, sys, traceback, urllib

from urlparse import urlparse, parse_qs

from twisted.internet.defer import inlineCallbacks

from vumi.transports.base import Transport
from vumi.utils import http_request_full
from vumi import log


## Outgoing only transport, will generate a URL replacing element of the to_addr field
class ForwardHttp(Transport):
    
    transport_type = 'http_api'

    def setup_transport(self):
        log.msg("Setup Forward HTTP transport %r" % self.config)
        self.message_replacement = self.config['message_replacement']
        self.compile_replacement(self.message_replacement)
        self.message_metadata_replacement = self.config['message_metadata_replacement']
        self.compile_replacement(self.message_metadata_replacement)
        self.transport_metadata = {'transport_type': self.transport_type}

    def teardown_transport(self):
        log.msg("Stop Forward HTTP Transport")

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
        log.msg("Outboung message %r" % message)
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
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
                log.error(reason)
                yield self.publish_nack(
                    message['message_id'],
                    reason,
                    transport_metadata=self.transport_metadata)
                return
            
            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
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
