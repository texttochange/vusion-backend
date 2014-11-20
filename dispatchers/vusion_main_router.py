import re

from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import ContentKeywordRouter
from vumi import log

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