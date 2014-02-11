import re

from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import BaseDispatchRouter
from vumi import log


##ToAddrRouter is routing accoding to regex on the message's to_addr
##field.
class ToAddrRouter(BaseDispatchRouter):

    def setup_routing(self):
        self.to_addr_mappings = []
        transport_mappings = self.config.get('transport_mappings', {})
        for transport_name, to_addr_patterns in transport_mappings.iteritems():
            for to_addr_pattern in to_addr_patterns:
                to_addr_regex = '^\+%s%s' % (self.config.get('country_code',''), to_addr_pattern)
                self.to_addr_mappings.append((transport_name, re.compile(to_addr_regex)))

    def dispatch_outbound_message(self, msg):
        dispatched = False
        for (transport_name, transport_regex) in self.to_addr_mappings:
            if transport_regex.match(msg['to_addr']):
                self.dispatcher.publish_outbound_message(transport_name, msg)
                dispatched = True
        if dispatched is False:     #fallback
            transport_name = self.config.get('transport_fallback')
            if transport_name is None:
                log.error('Cannot route outbound, no transport fallback')
            self.dispatcher.publish_outbound_message(transport_name, msg)

    def dispatch_inbound_message(self, msg):
         names = self.config.get('exposed_names', [])
         for name in names:
             self.dispatcher.publish_inbound_message(name, msg)
 
    def dispatch_inbound_event(self, msg):
        names = self.config.get('exposed_names', [])
        for name in names:
            self.dispatcher.publish_inbound_event(name, msg)
