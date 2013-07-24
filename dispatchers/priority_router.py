import re

from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import ContentKeywordRouter
from vumi import log

class PriorityContentKeywordRouter(ContentKeywordRouter):
    
    @inlineCallbacks
    def dispatch_outbound_message(self, msg):
        transport_name = self.get_transport_name(msg)
        if transport_name is None:
            log.error("No transport for %s" % (msg['from_addr'],))
            return
        self.publish_transport(transport_name, msg)
        message_key = self.get_message_key(msg['message_id'])
        self.r_server.set(message_key, msg['transport_name'])
        yield self.r_server.expire(message_key,
                                   self.expire_routing_timeout)
            
    def get_transport_name(self, msg):
        match_transport_name = None
        transport_mappings = self.transport_mappings.get(msg['transport_type'])
        if transport_mappings is None:
            return None
        for to_address, transport_name in transport_mappings.iteritems():
            regex_to_address = re.compile(to_address)
            if re.match(regex_to_address, msg['from_addr']):
                match_transport_name = transport_name
                break
        if match_transport_name is None:
            return None
        if isinstance(match_transport_name, str):
            return match_transport_name
        if isinstance(match_transport_name, dict):
            if ('priority' in msg['transport_metadata']
                    and msg['transport_metadata']['priority'] in match_transport_name):
                return transport_name[msg['transport_metadata']['priority']]
            return transport_name['default']
        return None