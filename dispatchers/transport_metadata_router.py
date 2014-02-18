from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import BaseDispatchRouter
from vumi import log


##TransportMetadataRouter is routing accoding to the presence
##of keys in the message's transport_metadata dict.
class TransportMetadataRouter(BaseDispatchRouter):

    def dispatch_outbound_message(self, msg):
        transport_mappings = self.config.get('transport_mappings', {})
        dispatched = False
        for metadata, transport_name in transport_mappings.iteritems():
            if (metadata in msg['transport_metadata']):
                self.dispatcher.publish_outbound_message(transport_name, msg)
                dispatched = True
        if dispatched is False:   #fallback
            transport_name = self.config.get('transport_fallback', None)
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
