from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial.unittest import TestCase

from middlewares.vusion_address_middleware import VusionAddressMiddleware
from tests.utils import MessageMaker


class VusionAddressTestCase(TestCase, MessageMaker):
    
    def setUp(self):
        dummy_worker = object()
        self.mw = VusionAddressMiddleware('mw1', {}, dummy_worker)
        self.mw.setup_middleware()

    def test_handle_inbound(self):
        msg_1 = self.mkmsg_in(from_addr="254888")
        msg_1 = self.mw.handle_inbound(msg_1 , 'dummy_endpoint')
        self.assertEqual(msg_1['from_addr'], '+254888')
        
        msg_2 = self.mkmsg_in(from_addr="+254888")
        msg_2 = self.mw.handle_inbound(msg_2 , 'dummy_endpoint')
        self.assertEqual(msg_2['from_addr'], '+254888')
    
        msg_3 = self.mkmsg_in(from_addr="00254888")
        msg_3 = self.mw.handle_inbound(msg_3 , 'dummy_endpoint')
        self.assertEqual(msg_3['from_addr'], '+254888')

    def test_handle_outbound(self):
        msg_1 = self.mkmsg_out(from_addr="254-8888", to_addr="+256")
        msg_1 = self.mw.handle_outbound(msg_1 , 'dummy_endpoint')
        self.assertEqual(msg_1['from_addr'], '8888')
        self.assertEqual(msg_1['to_addr'], '+256')

        msg_2 = self.mkmsg_out(from_addr="+318888")
        msg_2 = self.mw.handle_outbound(msg_2 , 'dummy_endpoint')
        self.assertEqual(msg_2['from_addr'], '+318888')


class VusionAddressRemovePlusTestCase(TestCase, MessageMaker):
    
    def setUp(self):
        dummy_worker = object()
        self.mw = VusionAddressMiddleware(
            'mw1',
            {'trim_plus_outbound': True},
            dummy_worker)
        self.mw.setup_middleware()

    def test_handle_outbound(self):
        msg_1 = self.mkmsg_out(to_addr="+256")
        msg_1 = self.mw.handle_outbound(msg_1 , 'dummy_endpoint')
        self.assertEqual(msg_1['to_addr'], '256')
        
        msg_2 = self.mkmsg_out(to_addr="256")
        msg_2 = self.mw.handle_outbound(msg_2 , 'dummy_endpoint')
        self.assertEqual(msg_2['to_addr'], '256')
    