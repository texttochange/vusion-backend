#encoding: utf-8
from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import (BaseDispatchWorker)
from vumi.tests.utils import FakeRedis
from vumi.dispatchers.tests.test_base import DispatcherTestCase


class TestVusionMainRouter(DispatcherTestCase):
    
    dispatcher_class = BaseDispatchWorker
    transport_name = 'test_transport'
    
    @inlineCallbacks
    def setUp(self):
        yield super(TestVusionMainRouter, self).setUp()
        self.config = {
            'dispatcher_name': 'keyword_dispatcher',
            'router_class': 'dispatchers.VusionMainRouter',
            'transport_names': [
                'transport1', 
                'transport2',
                'transport2-priority',
                'transport-http'],
            'transport_mappings':{
                'http_forward': 'transport-http',
                'sms': {
                    'shortcode1': 'transport1',
                    'shortcode2': {
                        'default': 'transport2',
                        'prioritized': 'transport2-priority'}}},
            'exposed_names': ['app1', 'app2', 'app3', 'fallback_app'],
            'rules': [{'app': 'app1',
                       'keyword': 'espanol',
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
        super(TestVusionMainRouter, self).tearDown()
        
    @inlineCallbacks
    def test_outbound_message_routing(self):
        msg = self.mkmsg_out(content="hello outbound msg",
                             from_addr='shortcode1',
                             transport_name='app2',
                             transport_type='sms')
        
        yield self.dispatch(msg,
                            transport_name='app2',
                            direction='outbound')
        
        transport1_msgs = self.get_dispatched_messages('transport1',
                                                      direction='outbound')
        self.assertEqual(transport1_msgs, [msg])

    @inlineCallbacks
    def test_outbound_message_routing_transport_type(self):
        msg = self.mkmsg_out(content="hello outbound msg",
                             to_addr='http://server.domain.ext/mo_message',
                             from_addr='app2',
                             transport_name='app2',
                             transport_type='http_forward')
        
        yield self.dispatch(msg,
                            transport_name='app2',
                            direction='outbound')
        
        transport1_msgs = self.get_dispatched_messages('transport-http',
                                                      direction='outbound')
        self.assertEqual(transport1_msgs, [msg])

    @inlineCallbacks
    def test_inbound_message_not_accent_sensitive(self):
        msg = self.mkmsg_in(content=u'espaÑol join',
                             to_addr='8181',
                             from_addr='+256453')
        
        yield self.dispatch(msg,
                            transport_name='transport1',
                            direction='inbound')
        
        app1_msgs = self.get_dispatched_messages('app1', direction='inbound')
        self.assertEqual(app1_msgs, [msg])        
    