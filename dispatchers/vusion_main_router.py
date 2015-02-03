import re

from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import ContentKeywordRouter
from vumi.utils import get_first_word
from vumi import log

from vusion.utils import get_first_msg_word
from vusion import clean_keyword


## this router apply the following rules
## inbound => keyword match on content
## event => exact match on message_id
##
class VusionMainRouter(ContentKeywordRouter):
    
    @inlineCallbacks
    def dispatch_outbound_message(self, msg):
        yield self._redis_d
        transport_name = self.get_transport_name(msg)
        if transport_name is None:
            log.error("No transport for type %s from %s" % (msg['transport_type'], msg['from_addr'],))
            return
        self.publish_transport(transport_name, msg)
        message_key = self.get_message_key(msg['message_id'])
        yield self.session_manager.create_session(
            message_key, name=msg['transport_name'])
            
    def get_transport_name(self, msg):
        match_transport_name = None
        transport_mappings = self.transport_mappings.get(msg['transport_type'])
        if transport_mappings is None:
            return None
        if msg['transport_type'] == 'sms':
            match_transport_name = transport_mappings.get(msg['from_addr'])
        elif msg['transport_type'] == 'http_api':
            match_transport_name = transport_mappings
        if match_transport_name is None:
            return None
        if isinstance(match_transport_name, str):
            return match_transport_name
        return None

    def is_msg_matching_routing_rules(self, keyword, msg, rule):
        rule['keyword'] = clean_keyword(rule['keyword'])
        return super(VusionMainRouter, self).is_msg_matching_routing_rules(
            clean_keyword(keyword), msg, rule)

    def dispatch_inbound_message(self, msg):
        keyword = get_first_msg_word(msg['content']).lower()
        matched = False
        for rule in self.rules:
            if self.is_msg_matching_routing_rules(keyword, msg, rule):
                matched = True
                # copy message so that the middleware doesn't see a particular
                # message instance multiple times
                self.publish_exposed_inbound(rule['app'], msg.copy())
        if not matched:
            if self.fallback_application is not None:
                self.publish_exposed_inbound(self.fallback_application, msg)
            else:
                log.error(DispatcherError(
                    "No transport for %s" % (msg['from_addr'],)))