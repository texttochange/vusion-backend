from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import BaseDispatchWorker
from vumi.dispatchers.tests.helpers import DispatcherHelper, DummyDispatcher
from vumi.tests.helpers import VumiTestCase, MessageHelper

from dispatchers.to_addr_router import ToAddrRouter


class TestToAddrRouter(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.config = {
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
        self.dispatcher = DummyDispatcher(self.config)
        self.router = ToAddrRouter(self.dispatcher, self.config)
        yield self.router.setup_routing()
        self.msg_helper = self.add_helper(MessageHelper())
 
    def test_inbound_message(self):
        msg = self.msg_helper.make_inbound(
            "1", transport_name='transport1')
        self.router.dispatch_inbound_message(msg)
        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['app'].msgs, [msg])

        publishers['app'].clear()
        msg = self.msg_helper.make_inbound(
            "1", transport_name='transport-default')
        self.router.dispatch_inbound_message(msg)
        self.assertEqual(publishers['app'].msgs, [msg])

    def test_inbound_event(self):
        msg1 = self.msg_helper.make_ack(transport_name='transport1')
        msg2 = self.msg_helper.make_ack(transport_name='transport-default')

        self.router.dispatch_inbound_event(msg1)
        self.router.dispatch_inbound_event(msg2)
        event_publishers = self.dispatcher.exposed_event_publisher

        self.assertEqual(event_publishers['app'].msgs, [msg1, msg2])

    def test_outbound_message(self):
        msg1 = self.msg_helper.make_outbound(
            "out", to_addr='+25523')
        self.router.dispatch_outbound_message(msg1)
        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport1'].msgs, [msg1])
      
        msg2 = self.msg_helper.make_outbound(
            "out", to_addr='+2559')
        self.router.dispatch_outbound_message(msg2)
        self.assertEqual(publishers['transport2'].msgs, [msg2])

        msg3 = self.msg_helper.make_outbound(
            "out", to_addr='+2558')
        self.router.dispatch_outbound_message(msg3)
        self.assertEqual(publishers['transport-default'].msgs, [msg3])


class TestToAddrRouterUrl(VumiTestCase):
        
    @inlineCallbacks
    def setUp(self):
        self.config = {
            'exposed_names': ['app'],
            'transport_names': [
                'transport1',
                'transport2',
                'transport-default'],
            'transport_mappings':{
                'transport1': [
                    'partner1.org', 
                    'mobile.partner1.org'],
                'transport2': [
                    '.*mobile.partner2.com']},
            'transport_fallback': 'transport-default'
            }
        self.dispatcher = DummyDispatcher(self.config)
        self.router = ToAddrRouter(self.dispatcher, self.config)
        yield self.router.setup_routing()
        self.msg_helper = self.add_helper(MessageHelper())

    def test_outbound_message(self):
        msg1 = self.msg_helper.make_outbound(
            "out", to_addr='partner1.org/api')
        self.router.dispatch_outbound_message(msg1)

        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport1'].msgs, [msg1])

        self.dispatcher.transport_publisher['transport1'].clear()

        msg2 = self.msg_helper.make_outbound(
            "out", to_addr='http://mobile.partner2.com')
        self.router.dispatch_outbound_message(msg2)
        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport2'].msgs, [msg2])
