from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import BaseDispatchWorker
from vumi.dispatchers.tests.test_base import DispatcherTestCase


class TestToAddrRouter(DispatcherTestCase):
    
    dispatcher_class = BaseDispatchWorker
    transport_name = 'test_transport'
    
    @inlineCallbacks
    def setUp(self):
        yield super(TestToAddrRouter, self).setUp()
        self.config = {
            'dispatcher_name': 'keyword_dispatcher',
            'router_class': 'dispatchers.ToAddrRouter',
            'exposed_names': ['app'],
            'transport_names': [
                'transport1',
                'transport2',
                'transport-default'],
            'country_code': '255',
            'transport_mappings':{
                'transport1': [
                    '[1-5]', 
                    '61'],
                'transport2': ['9']},
            'transport_fallback': 'transport-default'
            }
        self.dispatcher = yield self.get_dispatcher(self.config)
        self.router = self.dispatcher._router

    def tearDown(self):
        super(TestToAddrRouter, self).tearDown()

    @inlineCallbacks
    def test_inbound_message(self):
        msg = self.mkmsg_in()
        yield self.dispatch(
            msg,
            transport_name='transport1',
            direction='inbound')
        
        yield self.dispatch(
            msg,
            transport_name='transport-default',
            direction='inbound')        
        
        app_msgs = self.get_dispatched_messages('app', direction='inbound')
        self.assertEqual(app_msgs, [msg, msg])        

    @inlineCallbacks
    def test_inbound_event(self):
        msg1 = self.mkmsg_ack()
        yield self.dispatch(
            msg1,
            transport_name='transport1',
            direction='event')
        
        msg2 = self.mkmsg_ack()
        yield self.dispatch(
            msg2,
            transport_name='transport-default',
            direction='event')
        
        app_routing_event = self.get_dispatched_messages(
            'app', direction='event')
        self.assertEqual(app_routing_event, [msg1, msg2])

    @inlineCallbacks
    def test_outbound_message(self):
        msg1 = self.mkmsg_out(to_addr='+25523')
        yield self.dispatch(
            msg1,
            transport_name='app',
            direction='outbound')
        
        transport1_msgs = self.get_dispatched_messages(
            'transport1', direction='outbound')
        self.assertEqual(transport1_msgs, [msg1])
        
        msg2 = self.mkmsg_out(to_addr='+2559')
        yield self.dispatch(
            msg2,
            transport_name='app',
            direction='outbound')
        
        transport2_msgs = self.get_dispatched_messages(
            'transport2', direction='outbound')
        self.assertEqual(transport2_msgs, [msg2])

        msg3 = self.mkmsg_out(to_addr='+2558')
        yield self.dispatch(
            msg3,
            transport_name='app',
            direction='outbound')
        
        transport_default_msgs = self.get_dispatched_messages(
            'transport-default', direction='outbound')
        self.assertEqual(transport_default_msgs, [msg3])