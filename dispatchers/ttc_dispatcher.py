# -*- tst-case-name tests.test_ttc_dispatcher

import sys, traceback
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
        self.exposed_consumer = {}
        self.exposed_publisher = {}
        self.exposed_event_publisher = {}
        self.transport_consumer = {}
        self.transport_publisher = {}
        self.transport_event_consumer = {}
        super(DynamicDispatchWorker, self).startWorker()
        yield self.setup_control()

    #the stopping of a dispatcher is not implement yet in vumi
    @inlineCallbacks
    def stopWorker(self):
        log.debug('Stoping Dynamic Dispatcher')
        yield self.control.stop()
        super(DynamicDispatchWorker, self).stopWorker()

    @inlineCallbacks
    def setup_exposed_publishers(self):
        for exposed_name in self._exposed_names:
            if (exposed_name in self.exposed_publisher
                    and exposed_name in self.exposed_event_publisher):
                continue
            self.exposed_publisher[exposed_name] = yield self.publish_to(
                '%s.inbound' % (exposed_name,))
            self.exposed_event_publisher[exposed_name] = yield self.publish_to(
                '%s.event' % (exposed_name,))

    @inlineCallbacks
    def setup_exposed_consumers(self):
        for exposed_name in self._exposed_names:
            if exposed_name in self.exposed_consumer:
                continue
            self.exposed_consumer[exposed_name] = yield self.consume(
                '%s.outbound' % (exposed_name,),
                functools.partial(self.dispatch_outbound_message,
                                  exposed_name),
                message_class=TransportUserMessage)

    @inlineCallbacks
    def setup_control(self):
        self.control = yield self.consume(
            '%s.control' % self.config['dispatcher_name'],
            self.receive_control_message,
            message_class=DispatcherControl)

    @inlineCallbacks
    def setup_exposed(self, name):
        log.debug("Setup exposed %s" % (name,))
        if not name in self._exposed_names:
            self.exposed_publisher[name] = yield self.publish_to(
                ('%s.inbound' % (name,)).encode('utf-8'))
            self.exposed_event_publisher[name] = yield self.publish_to(
                ('%s.event' % (name,)).encode('utf-8'))
            self.exposed_consumer[name] = yield self.consume(
                ('%s.outbound' % (name,)).encode('utf-8'),
                functools.partial(self.dispatch_outbound_message,
                                  name),
                message_class=TransportUserMessage)
            self._exposed_names.append(name.encode('utf-8'))            

    @inlineCallbacks
    def remove_exposed(self, name):
        if name in self.config['exposed_names']:
            self.exposed_publisher.pop(name)
            self.exposed_event_publisher.pop(name)
            consumer = self.exposed_consumer.pop(name)
            yield consumer.stop()
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
        try:
            log.debug('Received control message %s' % (msg,))
            if msg['action'] == 'add_exposed':
                self.setup_exposed(msg['exposed_name'])
                self.append_mapping(msg['exposed_name'], msg['rules'])
                return
            if msg['action'] == 'remove_exposed':
                self.clear_mapping(msg['exposed_name'])
                self.remove_exposed(msg['exposed_name'])
                return
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "Error during consume control message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
