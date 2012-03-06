# -*- tst-case-name tests.test_ttc_dispatcher

from twisted.python import log

from vumi.dispatchers.base import BaseDispatchRouter 

class ContentKeywordRouter(BaseDispatchRouter):
    """Router that dispatches based on msg content first word also named as the keyw
    ord in the sms context.
    
    :type keyword_mappings: dict
    :param keyword_mappings:
        Mapping from application transport names to keyword.
        If a message's content first word is matching a given keyword,
        the message is send to the application listenning on the given transport name.
    
    """
    
    def setup_routing(self):
        self.mappings = []
        for name, keyword in self.config['keyword_mappings'].items():
            self.mappings.append((name, keyword))
            
    def dispatch_inbound_message(self, msg):
        log.msg('Inbound message')
        keyword = msg['content'].split()[0]
        for name, application in self.mappings:
            if (keyword == application):
                log.msg('Message is routed to %s' % (name,))
                self.dispatcher.exposed_publisher[name].publish_message(msg)
    
    def dispatch_inbound_event(self, msg):
        log.msg("Event to root but not implemented")
        #raise NotImplementedError()
        
    def dispatch_outbound_message(self, msg):
        log.msg("Outbound message")
        name = self.config['transport_names'][0]
        self.dispatcher.transport_publisher[name].publish_message(msg)


class DummyDispatcher(object):

    class DummyPublisher(object):
        def __init__(self):
            self.msgs = []

        def publish_message(self, msg):
            self.msgs.append(msg)

    def __init__(self, config):
        self.transport_publisher = {}
        for transport in config['transport_names']:
            self.transport_publisher[transport] = self.DummyPublisher()
        self.exposed_publisher = {}
        self.exposed_event_publisher = {}
        for exposed in config['exposed_names']:
            self.exposed_publisher[exposed] = self.DummyPublisher()
            self.exposed_event_publisher[exposed] = self.DummyPublisher()
