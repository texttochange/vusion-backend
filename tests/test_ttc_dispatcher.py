

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.utils import FakeRedis
from vumi.dispatchers.tests.test_base import DispatcherTestCase
from vumi.dispatchers.base import BaseDispatchWorker

from dispatchers import ContentKeywordRouter

class TestContentKeywordRouter(DispatcherTestCase):

    dispatcher_class = BaseDispatchWorker
    transport_name = 'test_transport'

    @inlineCallbacks
    def setUp(self):
        yield super(TestContentKeywordRouter, self).setUp()
        self.config = {
            'dispatcher_name': 'keyword_dispatcher',
            'router_class': 'vumi.dispatchers.base.ContentKeywordRouter',
            'transport_names': ['transport1', 'transport2'],
            'transport_mappings': {
                'shortcode1': 'transport1',
                'shortcode2': 'transport2'
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
        msg = self.mkmsg_out(content="KEYWORD1 rest of msg",
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


