
from datetime import datetime

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportUserMessage, TransportEvent, Message
from vumi.tests.utils import FakeRedis, get_stubbed_worker
from vumi.dispatchers.tests.test_base import DispatcherTestCase, TestBaseDispatchWorker
from vumi.dispatchers.base import BaseDispatchWorker

from dispatchers import ContentKeywordRouter
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
            'router_class': 'dispatchers.ContentKeywordRouter',
            'exposed_names': ['app1'],
            'keyword_mappings': {
                'app1': 'keyword1'
                },
            'transport_names': ['transport1'],
            'transport_mappings': {
                'transport1': 'shortcode1'
                },
            'expire_routing_memory': '1'
        }
        self.worker = get_stubbed_worker(DynamicDispatchWorker, config)
        self._amqp = self.worker._amqp_client.broker
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
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
    def test_control_register_exposed(self):
        control_msg_add = self.mkmsg_control(message_type='add_exposed',
                                         exposed_name='app2',
                                         keyword_mappings=[
                                             ['app2', 'keyword2'],
                                             ['app2', 'keyword3']
                                         ])
        control_msg_remove = self.mkmsg_control(message_type='remove_exposed',
                                         exposed_name='app2'
                                         )
        in_msg = self.mkmsg_in(content='keyword2')
        out_msg = self.mkmsg_out(from_addr='shortcode1')
        
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
        self.assert_no_messages('app2.inbound')

    def test_append_mapping(self):
        add_mappings = [['app2', 'keyword2'],
                       ['app2', 'keyword3']]
        
        self.worker.append_mapping('app2', add_mappings)
        self.worker.append_mapping('app2', add_mappings)
        
        self.assertEqual(self.worker._router.keyword_mappings,
                         [('app1', 'keyword1'),
                          ('app2', 'keyword2'),
                          ('app2', 'keyword3')])
        
        self.worker.append_mapping('app2', [['app2', 'keyword2']])
        
        self.assertEqual(self.worker._router.keyword_mappings,
                         [('app1', 'keyword1'),
                          ('app2', 'keyword2')])


class TestContentKeywordRouter(DispatcherTestCase):

    dispatcher_class = BaseDispatchWorker
    transport_name = 'test_transport'

    @inlineCallbacks
    def setUp(self):
        yield super(TestContentKeywordRouter, self).setUp()
        self.config = {
            'dispatcher_name': 'keyword_dispatcher',
            'router_class': 'dispatchers.ContentKeywordRouter',
            'transport_names': ['transport1', 'transport2'],
            'transport_mappings': {
                'transport1': 'shortcode1',
                'transport2': 'shortcode2' 
                },
            'exposed_names': ['app1', 'app2', 'app3'],
            'keyword_mappings': {
                'app1': {
                    'keyword': 'KEYWORD1',
                    'to_addr': '8181',
                    'prefix': '+256'
                    },
                'app2': 'KEYWORD2',
                'app3': 'KEYWORD1'
                },
            'expire_routing_memory': '3'
            }
        self.fake_redis = FakeRedis()
        self.dispatcher = yield self.get_dispatcher(self.config)
        self.router = self.dispatcher._router
        self.router.r_server = self.fake_redis
        self.router.setup_routing()

    def tearDown(self):
        self.fake_redis.teardown()
        super(TestContentKeywordRouter, self).tearDown()

    def mkmsg_ack(self, event_type='ack', user_message_id='1',
                  send_message_id='abc', transport_name=None,
                  transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            event_type=event_type,
            user_message_id=user_message_id,
            sent_message_id=send_message_id,
            transport_name=transport_name,
            transport_metadata=transport_metadata,
            )
        return TransportEvent(**params)

    @inlineCallbacks
    def test_inbound_message_routing(self):
        msg = self.mkmsg_in(content='KEYWORD1 rest of a msg',
                            to_addr='8181',
                            from_addr='+256788601462')

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='inbound')

        app1_inbound_msg = self.get_dispatched_messages('app1',
                                                        direction='inbound')
        self.assertEqual(app1_inbound_msg, [msg])
        app2_inbound_msg = self.get_dispatched_messages('app2',
                                                        direction='inbound')
        self.assertEqual(app2_inbound_msg, [])
        app3_inbound_msg = self.get_dispatched_messages('app3',
                                                        direction='inbound')
        self.assertEqual(app3_inbound_msg, [msg])

    @inlineCallbacks
    def test_inbound_message_routing_empty_message_content(self):
        msg = self.mkmsg_in(content=None)

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='inbound')

        app1_inbound_msg = self.get_dispatched_messages('app1',
                                                        direction='inbound')
        self.assertEqual(app1_inbound_msg, [])
        app2_inbound_msg = self.get_dispatched_messages('app2',
                                                        direction='inbound')
        self.assertEqual(app2_inbound_msg, [])

    @inlineCallbacks
    def test_inbound_message_routing_not_casesensitive(self):
        msg = self.mkmsg_in(content='keyword1 rest of a msg',
                            to_addr='8181',
                            from_addr='+256788601462')

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='inbound')

        app1_inbound_msg = self.get_dispatched_messages('app1',
                                                        direction='inbound')
        self.assertEqual(app1_inbound_msg, [msg])

    @inlineCallbacks
    def test_inbound_event_routing_ok(self):
        msg = self.mkmsg_ack(user_message_id='1',
                             transport_name='transport1')
        self.router.r_server.set('keyword_dispatcher:message:1',
                                 'app2')

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='event')

        app2_event_msg = self.get_dispatched_messages('app2',
                                                      direction='event')
        self.assertEqual(app2_event_msg, [msg])
        app1_event_msg = self.get_dispatched_messages('app1',
                                                      direction='event')
        self.assertEqual(app1_event_msg, [])

    @inlineCallbacks
    def test_inbound_event_routing_failing_publisher_not_defined(self):
        msg = self.mkmsg_ack(transport_name='transport1')

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='event')

        app1_routed_msg = self.get_dispatched_messages('app1',
                                                       direction='event')
        self.assertEqual(app1_routed_msg, [])
        app2_routed_msg = self.get_dispatched_messages('app2',
                                                       direction='event')
        self.assertEqual(app2_routed_msg, [])

    @inlineCallbacks
    def test_inbound_event_routing_failing_no_routing_back_in_redis(self):
        msg = self.mkmsg_ack(transport_name='transport1')

        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='event')

        app1_routed_msg = self.get_dispatched_messages('app1',
                                                       direction='event')
        self.assertEqual(app1_routed_msg, [])
        app2_routed_msg = self.get_dispatched_messages('app2',
                                                       direction='event')
        self.assertEqual(app2_routed_msg, [])

    @inlineCallbacks
    def test_outbound_message_routing(self):
        msg = self.mkmsg_out(content="rest of msg",
                             from_addr='shortcode1',
                             transport_name='app2')

        yield self.dispatch(msg,
                            transport_name='app2',
                            direction='outbound')

        transport1_msgs = self.get_dispatched_messages('transport1',
                                                       direction='outbound')
        self.assertEqual(transport1_msgs, [msg])
        transport2_msgs = self.get_dispatched_messages('transport2',
                                                       direction='outbound')
        self.assertEqual(transport2_msgs, [])

        app2_route = self.fake_redis.get('keyword_dispatcher:message:1')
        self.assertEqual(app2_route, 'app2')

      
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


