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
        transport_name = self.transport_mappings.get(msg['from_addr'])
        if transport_name is None:
            return None
        if isinstance(transport_name, str):
            return transport_name
        if isinstance(transport_name, dict):
            if ('priority' in msg['transport_metadata']
                    and msg['transport_metadata']['priority'] in transport_name):   
                return transport_name[msg['transport_metadata']['priority']]
            return transport_name['default']
        return None