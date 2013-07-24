from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import (BaseDispatchWorker)
from vumi.tests.utils import FakeRedis
from vumi.dispatchers.tests.test_base import DispatcherTestCase

class TestPriorityContentKeywordRouter(DispatcherTestCase):
    
    dispatcher_class = BaseDispatchWorker
    transport_name = 'test_transport'
    
    @inlineCallbacks
    def setUp(self):
        yield super(TestPriorityContentKeywordRouter, self).setUp()
        self.config = {
            'dispatcher_name': 'keyword_dispatcher',
            'router_class': 'dispatchers.priority_router.PriorityContentKeywordRouter',
            'transport_names': [
                'transport1', 
                'transport2',
                'transport2-priority',
                'transport-http'],
            'transport_mappings':{
                'http_api': {
                    '.*': 'transport-http'},
                'sms': {
                    'shortcode1': 'transport1',
                    'shortcode2': {
                        'default': 'transport2',
                        'prioritized': 'transport2-priority'}}},
            'exposed_names': ['app1', 'app2', 'app3', 'fallback_app'],
            'rules': [{'app': 'app1',
                       'keyword': 'KEYWORD1',
                       'to_addr': '8181',
                       'prefix': '+256',
                       },
                      {'app': 'app2',
                       'keyword': 'KEYWORD2'}],
            'keyword_mappings': {
                'app2': 'KEYWORD3',
                'app3': 'KEYWORD1',
                },
            'fallback_application': 'fallback_app',
            'expire_routing_memory': '3',
            }
        self.fake_redis = FakeRedis()
        self.dispatcher = yield self.get_dispatcher(self.config)
        self.router = self.dispatcher._router
        self.router.r_server = self.fake_redis
        
    def tearDown(self):
        self.fake_redis.teardown()
        super(TestPriorityContentKeywordRouter, self).tearDown()
        
    @inlineCallbacks
    def test_outbound_message_routing_without_priority_transport(self):
        msg = self.mkmsg_out(content="hello outbound msg",
                             from_addr='shortcode1',
                             transport_name='app2')
        
        yield self.dispatch(msg,
                            transport_name='app2',
                            direction='outbound')
        
        transport1_msgs = self.get_dispatched_messages('transport1',
                                                      direction='outbound')
        self.assertEqual(transport1_msgs, [msg])
        
    @inlineCallbacks
    def test_outbound_message_routing_with_priority_transport_default(self):
        msg = self.mkmsg_out(content="hello outbound msg",
                             from_addr='shortcode2',
                             transport_name='app2')
        
        yield self.dispatch(msg,
                            transport_name='app2',
                            direction='outbound')
        
        transport2_msgs = self.get_dispatched_messages('transport2',
                                                      direction='outbound')
        self.assertEqual(transport2_msgs, [msg])
        
    @inlineCallbacks
    def test_outbound_message_routing_with_priority_defined(self):
        msg = self.mkmsg_out(content="hello outbound msg",
                             from_addr='shortcode2',
                             transport_name='app2',
                             transport_metadata={'priority':'prioritized'}) #priority 'prioritized' is defined
        
        yield self.dispatch(msg,
                            transport_name='app2',
                            direction='outbound')
        
        transport2_msgs = self.get_dispatched_messages('transport2-priority',
                                                      direction='outbound')
        self.assertEqual(transport2_msgs, [msg])
        
    @inlineCallbacks
    def test_outbound_message_routing_with_priority_undefined(self):
        msg = self.mkmsg_out(content="hello outbound msg",
                             from_addr='shortcode2',
                             transport_name='app2',
                             transport_metadata={'priority':'2'}) # priority 2 is undefined
        
        yield self.dispatch(msg,
                            transport_name='app2',
                            direction='outbound')
        
        transport2_msgs = self.get_dispatched_messages('transport2',
                                                      direction='outbound')
        self.assertEqual(transport2_msgs, [msg])
        
    @inlineCallbacks
    def test_outbound_message_routing_transport_type(self):
        msg = self.mkmsg_out(content="hello outbound msg",
                             from_addr='http://server.domain.ext/mo_message',
                             transport_name='app2',
                             transport_type='http_api')
        
        yield self.dispatch(msg,
                            transport_name='app2',
                            direction='outbound')
        
        transport1_msgs = self.get_dispatched_messages('transport-http',
                                                      direction='outbound')
        self.assertEqual(transport1_msgs, [msg])        
        
        
        
        
        
        