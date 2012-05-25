# -*- tst-case-name tests.test_ttc_dispatcher

import redis
import functools

from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import SimpleDispatchRouter, BaseDispatchWorker
from vumi import log
from vumi.message import Message, TransportUserMessage

from vusion.message import DispatcherControl

def get_first_word(content, delimiter=' '):
    """
    splits a string to get the first word

    >>>get_first_word('KEYWORD rest of message')
    'KEYWORD'
    >>>

    """
    return (content or '').split(delimiter)[0]


class DynamicDispatchWorker(BaseDispatchWorker):
    """Dispatch worker able to create/remove publisher/consumer
    when receiving worker request on his controle queue

    """

    @inlineCallbacks
    def startWorker(self):
        log.debug('Starting Dynamic Dispatcher %s' % (self.config,))
        super(DynamicDispatchWorker, self).startWorker()
        yield self.setup_control()
        #yield self.setup_garbage()

    @inlineCallbacks
    def setup_control(self):
        self.control = yield self.consume(
            '%s.control' % self.config['dispatcher_name'],
            self.receive_control_message,
            message_class=DispatcherControl)

    @inlineCallbacks
    def setup_exposed(self, name):
        if not name in self.config['exposed_names']:
            self.config['exposed_names'].append(name)
            self.exposed_publisher[name] = yield self.publish_to(
                ('%s.inbound' % (name,)).encode('utf-8'))
            self.exposed_event_publisher[name] = yield self.publish_to(
                ('%s.event' % (name,)).encode('utf-8'))
            self.exposed_consumer[name] = yield self.consume(
                ('%s.outbound' % (name,)).encode('utf-8'),
                functools.partial(self.dispatch_outbound_message,
                                  name),
                message_class=TransportUserMessage)

    def remove_exposed(self, name):
        if name in self.config['exposed_names']:
            self.exposed_publisher.pop(name)
            self.exposed_event_publisher.pop(name)
            self.exposed_consumer.pop(name)
            self.config['exposed_names'].remove(name)

    #Need to check if the (name, rule) is not already there
    def append_mapping(self, exposed_name, rules):
        self.remove_non_present_mappings(exposed_name, rules)
        for rule in rules:
            if rule not in self._router.rules:
                self._router.rules.append(rule)

    def remove_non_present_mappings(self, exposed_name, rules):
        non_present_mappings = self.get_non_present_mapping(
            self.get_mapping(exposed_name),
            rules)
        for rule in non_present_mappings:
            self._router.rules.remove(rule)

    def get_mapping(self, name_to_get):
        return [rule for rule in self._router.rules
                if rule['app'] == name_to_get]

    def get_non_present_mapping(self, current_mappings, new_mappings):
        return [rule for rule in current_mappings
                if rule not in new_mappings]

    def clear_mapping(self, name_to_clear):
        self._router.rules = [rule
                              for rule
                              in self._router.rules
                              if name_to_clear != rule['app']]

    def receive_control_message(self, msg):
        log.debug('Received control message %s' % (msg,))
        if msg['action'] == 'add_exposed':
            self.setup_exposed(msg['exposed_name'])
            self.append_mapping(msg['exposed_name'], msg['rules'])
            return
        if msg['action'] == 'remove_exposed':
            self.remove_exposed(msg['exposed_name'])
            self.clear_mapping(msg['exposed_name'])
            return

##Deprecated use the Vumi ContentKeyword Router
class ContentKeywordRouter(SimpleDispatchRouter):
    """Router that dispatches based on msg content first word also named as the
     keyword in the sms context.

    :type keyword_mappings: dict
    :param keyword_mappings:
        Mapping from application's transport names to keyword.
        If a message's content first word is matching a given keyword,
        the message is send to the application listenning on the given
         transport name.

    :type transport_mappings: dict
    :param transport_mappings:
        Mapping from from_addr to transport's transport name.
        If a message's from_addr is matching a given from_addr,
        the message is send to the given transport.

    :type expire_routing_memory: int
    :param expire_routing_memory:
        Duration in second of storage of outbound message's id in redis.
        The stored id is used to route back the Event to
        the application worker.

    """

    def __init__(self, dispatcher, config):
        self.r_config = config.get('redis_config', {})
        self.r_prefix = config['dispatcher_name']
        self.r_server = redis.Redis(**self.r_config)
        self.keyword_mappings = []
        if 'keyword_mappings' in config:
            self.keyword_mappings = config['keyword_mappings'].items()
        self.transport_mappings = config['transport_mappings'].items()
        super(ContentKeywordRouter, self).__init__(dispatcher, config)

    def setup_routing(self):
        pass

    def get_message_key(self, message):
        return self.r_key('message', message)

    def r_key(self, *parts):
        return ':'.join([self.r_prefix] + map(str, parts))

    def publish_transport(self, name, msg):
        self.dispatcher.transport_publisher[name].publish_message(msg)

    def publish_exposed_inbound(self, name, msg):
        self.dispatcher.exposed_publisher[name].publish_message(msg)

    def publish_exposed_event(self, name, msg):
        self.dispatcher.exposed_event_publisher[name].publish_message(msg)

    def is_msg_matching_routing_rules(self, msg, routing_rules):
        return (get_first_word(msg['content']).lower() == routing_rules['keyword'].lower()
                and (
                    (not 'to_addr' in routing_rules)
                    or msg['to_addr'] == routing_rules['to_addr'])
                and (
                    (not 'prefix' in routing_rules)
                    or msg['from_addr'].startswith(routing_rules['prefix'])))

    def dispatch_inbound_message(self, msg):
        log.debug('Inbound message')
        msg_keyword = get_first_word(msg['content'])
        has_been_forwarded = False
        if (msg_keyword == ''):
            log.error('Message has no keyword')
            return has_been_forwarded
        for name, routing_rules in self.keyword_mappings:
            if (type(routing_rules) != dict):
                routing_rules = {'keyword': routing_rules}
            if self.is_msg_matching_routing_rules(msg, routing_rules):
                log.debug('Message is routed to %s' % (name,))
                self.publish_exposed_inbound(name, msg)
                has_been_forwarded = True
        return has_been_forwarded

    def dispatch_inbound_event(self, msg):
        log.debug("Inbound event")
        message_key = self.get_message_key(msg['user_message_id'])
        name = self.r_server.get(message_key)
        if (not name):
            log.error("Not route back tuple stored in Redis for %s"
                  % (msg['user_message_id'],))
        try:
            log.debug('Event is routed to %s' % (name,))
            self.publish_exposed_event(name, msg)
        except:
            log.error("No publishing route for %s" % (name,))

    @inlineCallbacks
    def dispatch_outbound_message(self, msg):
        log.debug("Outbound message")
        has_been_forwarded = False
        for (name, transport) in self.config['transport_mappings'].items():
            if (transport == msg['from_addr']):
                has_been_forwarded = True
                self.publish_transport(name, msg)
                message_key = self.get_message_key(msg['message_id'])
                self.r_server.set(message_key, msg['transport_name'])
                yield self.r_server.expire(
                    message_key,
                    int(self.config['expire_routing_memory']))
        if not has_been_forwarded:
            log.error("No transport for %s" % (msg['from_addr'],))
