#encoding: utf-8
from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import (BaseDispatchWorker)
from vumi.tests.utils import FakeRedis
from vumi.tests.helpers import VumiTestCase
from vumi.dispatchers.tests.helpers import DispatcherHelper

class TestVusionMainRouter(VumiTestCase):
    
    @inlineCallbacks
    def setUp(self):
        self.disp_helper = self.add_helper(
            DispatcherHelper(BaseDispatchWorker))
        #yield super(TestVusionMainRouter, self).setUp()
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
        self.dispatcher = yield self.disp_helper.get_dispatcher(self.config)
        self.router = self.dispatcher._router
        yield self.router._redis_d
        self.add_cleanup(self.router.session_manager.stop)
        self.redis = self.router.redis
        yield self.redis._purge_all()

    def ch(self, connector_name):
        return self.disp_helper.get_connector_helper(connector_name)

    def send_inbound(self, connector_name, content, **kw):
        return self.ch(connector_name).make_dispatch_inbound(content, **kw)

    def send_outbound(self, connector_name, content, **kw):
        return self.ch(connector_name).make_dispatch_outbound(content, **kw)

    def assert_dispatched_inbound(self, connector_name, msgs):
        self.assertEqual(
            msgs,
            self.disp_helper.get_dispatched_inbound(connector_name))

    def assert_dispatched_outbound(self, connector_name, msgs):
        self.assertEqual(
            msgs,
            self.disp_helper.get_dispatched_outbound(connector_name))

    @inlineCallbacks
    def test_outbound_message_routing(self):
    
        #msg = self.mkmsg_out(content="hello outbound msg",
                             #from_addr='shortcode1',
                             #transport_name='app2',
                             #transport_type='sms')
        msg = yield self.send_outbound(
            'app2',
            'hello world',
            transport_type='sms',
            from_addr='shortcode1')
        self.assert_dispatched_outbound('transport1', [msg])
        #transport1_msgs = self.get_dispatched_messages('transport1',
                                                      #direction='outbound')
        #self.assertEqual(transport1_msgs, [msg])

    @inlineCallbacks
    def test_outbound_message_routing_transport_type(self):
        msg = yield self.send_outbound(
            'app2',
            'hello world',
            transport_type='http_forward',
            from_addr='app2',
            to_addr='http://server.domain.ext/mo_message')        
        self.assert_dispatched_outbound('transport-http', [msg])

    @inlineCallbacks
    def test_inbound_message_not_accent_sensitive(self):
        msg = yield self.send_inbound(
            'transport1',
            u'espaÑol join',
            to_addr='8181',
            from_addr='+256453')   
        self.assert_dispatched_inbound('app1', [msg])
