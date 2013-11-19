
from datetime import datetime

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportUserMessage, TransportEvent, Message
from vumi.tests.utils import FakeRedis, get_stubbed_worker
from vumi.dispatchers.tests.test_base import (DispatcherTestCase,
                                              TestBaseDispatchWorker)
from vumi.dispatchers.base import BaseDispatchWorker

from vusion.message import DispatcherControl
from dispatchers.ttc_dispatcher import DynamicDispatchWorker
from tests.utils import MessageMaker


class TestDynamicDispatcherWorker(TestCase, MessageMaker):

    @inlineCallbacks
    def setUp(self):
        yield self.get_worker()

    @inlineCallbacks
    def get_worker(self):
        config = {
            'dispatcher_name': 'vusion',
            'router_class': 'dispatchers.PriorityContentKeywordRouter',
            'exposed_names': ['app1', 'fallback_app'],
            'keyword_mappings': {'app1': 'keyword1'},
            'rules': [{
                'app': 'app1',
                'keyword': 'keyword2',
                'to_addr': '8181'}],
            'transport_names': ['transport1', 'forward_http'],
            'transport_mappings': {
                'http_forward': 'forward_http',
                'sms': {
                    '8181': 'transport1'}},
            'fallback_application': 'fallback_app',
            'expire_routing_memory': '1'
        }
        self.worker = get_stubbed_worker(DynamicDispatchWorker, config)
        self._amqp = self.worker._amqp_client.broker
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        self.clear_dispatched()
        yield self.worker.stopWorker()

    def dispatch(self, message, rkey=None, exchange='vumi'):
        if rkey is None:
            rkey = self.rkey('control')
        self._amqp.publish_message(exchange, rkey, message)
        return self._amqp.kick_delivery()

    def assert_messages(self, rkey, msgs):
        self.assertEqual(msgs, self._amqp.get_messages('vumi', rkey))

    def assert_no_messages(self, *rkeys):
        for rkey in rkeys:
            self.assertEqual([], self._amqp.get_messages('vumi', rkey))

    def clear_dispatched(self):
        self._amqp.dispatched = {}

    @inlineCallbacks
    def test_unmatching_routing(self):
        in_msg = self.mkmsg_in(content='keyword3')
        yield self.dispatch(in_msg, 'transport1.inbound')
        self.assert_messages('fallback_app.inbound', [in_msg])

    @inlineCallbacks
    def test_control_register_exposed(self):
        control_msg_add = self.mkmsg_dispatcher_control(
            action='add_exposed',
            exposed_name='app2',
            rules=[
                {'app': 'app2', 'keyword': 'keyword2'},
                {'app': 'app2', 'keyword': 'keyword3'},
            ])
        control_msg_remove = self.mkmsg_dispatcher_control(
            action='remove_exposed',
            exposed_name='app2'
        )
        in_msg = self.mkmsg_in(content='keyword2')
        out_msg = self.mkmsg_out(from_addr='8181',
                                 transport_type='sms')

        yield self.dispatch(in_msg, 'transport1.inbound')
        self.assert_no_messages('app2.inbound')

        self.clear_dispatched()

        yield self.dispatch(control_msg_add, 'vusion.control')
        yield self.dispatch(in_msg, 'transport1.inbound')
        self.assert_messages('app2.inbound', [in_msg])

        yield self.dispatch(out_msg, 'app2.outbound')
        self.assert_messages('transport1.outbound', [out_msg])

        self.clear_dispatched()

        yield self.dispatch(control_msg_remove, 'vusion.control')
        yield self.dispatch(in_msg, 'transport1.inbound')
        self.assertNotIn('app2', self.worker.exposed_consumer)
        self.assert_no_messages('app2.inbound')

    def test_append_mapping(self):
        add_mappings = [
            {'app': 'app2',
             'keyword': 'keyword2',
             'to_addr': '8181'},
            {'app': 'app2',
             'to_addr': 'keyword3'}]

        self.worker.append_mapping('app2', add_mappings)
        self.worker.append_mapping('app2', add_mappings)

        self.assertEqual(
            self.worker._router.rules,
            [{'app': 'app1', 'keyword': 'keyword2', 'to_addr': '8181'},
             {'app': 'app1', 'keyword': 'keyword1'},
             {'app': 'app2', 'keyword': 'keyword2', 'to_addr': '8181'},
             {'app': 'app2', 'to_addr': 'keyword3'}]
        )

        self.worker.append_mapping(
            'app2',
            [{'app': 'app2', 'keyword': 'keyword2', 'to_addr': '8181'}])

        self.assertEqual(
            self.worker._router.rules,
            [{'app': 'app1', 'keyword': 'keyword2', 'to_addr': '8181'},
             {'app': 'app1', 'keyword': 'keyword1'},
             {'app': 'app2', 'keyword': 'keyword2', 'to_addr': '8181'}])

    @inlineCallbacks
    def test_append_mapping_not_finished(self):
        add_mappings = [
                    {'app': 'app2',
                     'keyword': 'keyword2',
                     'to_addr': '8181'}]        
        self.worker.append_mapping('app2', add_mappings)
        in_msg = self.mkmsg_in(content='keyword2', to_addr='8181')
        
        yield self.dispatch(in_msg, 'transport1.inbound')
        self.assert_no_messages('app2.inbound')


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
