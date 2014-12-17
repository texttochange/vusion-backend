from twisted.internet.defer import inlineCallbacks


from vumi.dispatchers.tests.helpers import DummyDispatcher
from vumi.tests.helpers import VumiTestCase, MessageHelper

from dispatchers import TransportMetadataRouter


class TestTransportMetadataRouter(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.config = {
            'exposed_names': ['app'],
            'transport_names': [
                'transport',
                'transport-priority'],
            'transport_mappings':{
                'priority': 'transport-priority'},
            'transport_fallback': 'transport'
            }
        self.dispatcher = DummyDispatcher(self.config)
        self.router = TransportMetadataRouter(self.dispatcher, self.config)
        yield self.router.setup_routing()
        self.msg_helper = self.add_helper(MessageHelper())

    def test_inbound_message(self):
        msg1 = self.msg_helper.make_inbound(
            "in", transport_name='transport')
        self.router.dispatch_inbound_message(msg1)

        msg2 = self.msg_helper.make_inbound(
            "in", transport_name='transport-priority')
        self.router.dispatch_inbound_message(msg2)

        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['app'].msgs, [msg1, msg2])

    def test_inbound_event(self):
        msg = self.msg_helper.make_ack(transport_name='transport')
        self.router.dispatch_inbound_event(msg)
        event_publishers = self.dispatcher.exposed_event_publisher
        self.assertEqual(event_publishers['app'].msgs, [msg])

    def test_outbound_message_routing_with_priority_transport_default(self):
        msg = self.msg_helper.make_outbound(
            content="hello outbound msg",
            from_addr='shortcode',
            transport_name='app',
            transport_type='sms')
        self.router.dispatch_outbound_message(msg)

        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport'].msgs, [msg])
        
    def test_outbound_message_routing_with_priority_defined(self):
        msg = self.msg_helper.make_outbound(
            content="hello outbound msg",
            from_addr='shortcode',
            transport_name='app',
            transport_type='sms',
            #priority 'prioritized' is defined
            transport_metadata={'priority':'prioritized'})
        self.router.dispatch_outbound_message(msg)

        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport-priority'].msgs, [msg])

    def test_outbound_message_routing_with_priority_undefined(self):
        msg = self.msg_helper.make_outbound(
            content="hello outbound msg",
            from_addr='shortcode',
            transport_name='app',
            transport_type='sms',
            # priority 2 is undefined
            transport_metadata={'priority':'2'})
        self.router.dispatch_outbound_message(msg)

        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport-priority'].msgs, [msg])
