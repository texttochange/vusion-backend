from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import BaseDispatchWorker
from vumi.dispatchers.tests.test_base import DispatcherTestCase


class TestTransportMetadataRouter(DispatcherTestCase):
    
    dispatcher_class = BaseDispatchWorker
    transport_name = 'test_transport'
    
    @inlineCallbacks
    def setUp(self):
        yield super(TestTransportMetadataRouter, self).setUp()
        self.config = {
            'dispatcher_name': 'keyword_dispatcher',
            'router_class': 'dispatchers.TransportMetadataRouter',
            'exposed_names': ['app'],
            'transport_names': [
                'transport',
                'transport-priority'],
            'transport_mappings':{
                'priority': 'transport-priority'},
            'transport_fallback': 'transport'
            }
        self.dispatcher = yield self.get_dispatcher(self.config)
        self.router = self.dispatcher._router

    def tearDown(self):
        super(TestTransportMetadataRouter, self).tearDown()

    @inlineCallbacks
    def test_inbound_message(self):
        msg = self.mkmsg_in()
        yield self.dispatch(
            msg,
            transport_name='transport',
            direction='inbound')
        
        yield self.dispatch(
            msg,
            transport_name='transport-priority',
            direction='inbound')        
        
        app_msgs = self.get_dispatched_messages('app', direction='inbound')
        self.assertEqual(app_msgs, [msg, msg])

    @inlineCallbacks
    def test_inbound_event(self):
        msg = self.mkmsg_ack()
        yield self.dispatch(
            msg,
            transport_name='transport',
            direction='event')
        
        app_routing_event = self.get_dispatched_messages(
            'app', direction='event')
        self.assertEqual(app_routing_event, [msg])

    @inlineCallbacks
    def test_outbound_message_routing_with_priority_transport_default(self):
        msg = self.mkmsg_out(content="hello outbound msg",
                             from_addr='shortcode',
                             transport_name='app',
                             transport_type='sms')
        
        yield self.dispatch(msg,
                            transport_name='app',
                            direction='outbound')
        
        transport2_msgs = self.get_dispatched_messages(
            'transport', direction='outbound')
        self.assertEqual(transport2_msgs, [msg])
        
    @inlineCallbacks
    def test_outbound_message_routing_with_priority_defined(self):
        msg = self.mkmsg_out(content="hello outbound msg",
                             from_addr='shortcode',
                             transport_name='app',
                             transport_type='sms',
                             transport_metadata={'priority':'prioritized'}) #priority 'prioritized' is defined
        
        yield self.dispatch(msg,
                            transport_name='app',
                            direction='outbound')
        
        transport_priority_msgs = self.get_dispatched_messages(
            'transport-priority', direction='outbound')
        self.assertEqual(transport_priority_msgs, [msg])
        
    @inlineCallbacks
    def test_outbound_message_routing_with_priority_undefined(self):
        msg = self.mkmsg_out(content="hello outbound msg",
                             from_addr='shortcode',
                             transport_name='app',
                             transport_type='sms',
                             transport_metadata={'priority':'2'}) # priority 2 is undefined
        
        yield self.dispatch(msg,
                            transport_name='app',
                            direction='outbound')
        
        transport2_msgs = self.get_dispatched_messages(
            'transport-priority', direction='outbound')
        self.assertEqual(transport2_msgs, [msg])
        
        