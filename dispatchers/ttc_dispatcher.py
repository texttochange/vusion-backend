# -*- tst-case-name tests.test_ttc_dispatcher

import redis

from twisted.python import log

#from vumi.utils import get_deploy_int

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
        
        self.r_server = redis.Redis("localhost", db='0')
            
    def dispatch_inbound_message(self, msg):
        log.msg('Inbound message')
        keyword = msg['content'].split()[0]
        for name, application in self.mappings:
            if (keyword == application):
                log.msg('Message is routed to %s' % (name,))
                self.dispatcher.exposed_publisher[name].publish_message(msg)
    
    def dispatch_inbound_event(self, msg):
        log.msg("Inbound event")
        name = self.r_server.get(msg['user_message_id'])
        try:
            log.msg('Event is routed to %s' %(name,))
            self.dispatcher.exposed_publisher[name].publish_message(msg)
        except:
            log.msg("Error unable to dispatch event to %s" % (name,))
        
    def dispatch_outbound_message(self, msg):
        log.msg("Outbound message")
        name = self.config['transport_names'][0]
        self.dispatcher.transport_publisher[name].publish_message(msg)
        self.r_server.set(msg['message_id'], msg['transport_name'])


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
