from middlewares.vusion_address_middleware import VusionAddressMiddleware
from tests.utils import MessageMaker
from vumi.tests.helpers import VumiTestCase


class VusionAddressMiddlewareTestCase(VumiTestCase, MessageMaker):
    
    def setUp(self):
        dummy_worker = object()
        self.mw = VusionAddressMiddleware('mw1', {}, dummy_worker)
        self.mw.setup_middleware()

    def test_handle_inbound_from_addr(self):
        msg_1 = self.mkmsg_in(from_addr="254888")
        msg_1 = self.mw.handle_inbound(msg_1 , 'dummy_endpoint')
        self.assertEqual(msg_1['from_addr'], '+254888')
        
        msg_2 = self.mkmsg_in(from_addr="+254888")
        msg_2 = self.mw.handle_inbound(msg_2 , 'dummy_endpoint')
        self.assertEqual(msg_2['from_addr'], '+254888')
    
        msg_3 = self.mkmsg_in(from_addr="00254888")
        msg_3 = self.mw.handle_inbound(msg_3 , 'dummy_endpoint')
        self.assertEqual(msg_3['from_addr'], '+254888')
        
        msg_4 = self.mkmsg_in(from_addr="+ 254 888")
        msg_4 = self.mw.handle_inbound(msg_4 , 'dummy_endpoint')
        self.assertEqual(msg_4['from_addr'], '+254888')        
        
    def test_handle_inbound_to_addr(self):
        msg_1 = self.mkmsg_in(to_addr="254888")
        msg_1 = self.mw.handle_inbound(msg_1 , 'dummy_endpoint')
        self.assertEqual(msg_1['to_addr'], '254888')
        
        msg_2 = self.mkmsg_in(to_addr="+254888")
        msg_2 = self.mw.handle_inbound(msg_2 , 'dummy_endpoint')
        self.assertEqual(msg_2['to_addr'], '+254888')
    
        msg_3 = self.mkmsg_in(to_addr="00254888")
        msg_3 = self.mw.handle_inbound(msg_3 , 'dummy_endpoint')
        self.assertEqual(msg_3['to_addr'], '+254888')
        
        msg_2 = self.mkmsg_in(to_addr="+ 254 888")
        msg_2 = self.mw.handle_inbound(msg_2 , 'dummy_endpoint')
        self.assertEqual(msg_2['to_addr'], '+254888')        

    def test_handle_outbound(self):
        msg_1 = self.mkmsg_out(from_addr="254-8888", to_addr="+256")
        msg_1 = self.mw.handle_outbound(msg_1 , 'dummy_endpoint')
        self.assertEqual(msg_1['from_addr'], '8888')
        self.assertEqual(msg_1['to_addr'], '+256')

        msg_2 = self.mkmsg_out(from_addr="+318888")
        msg_2 = self.mw.handle_outbound(msg_2 , 'dummy_endpoint')
        self.assertEqual(msg_2['from_addr'], '+318888')


class VusionAddressRemovePlusTestCase(VumiTestCase, MessageMaker):
    
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


class VusionAddressRemoveInternationalPrefixTestCase(VumiTestCase, MessageMaker):
    
    def setUp(self):
        dummy_worker = object()
        self.mw = VusionAddressMiddleware(
            'mw1',
            {'international_prefix': '256',
             'trim_international_prefix_outbound': True},
            dummy_worker)
        self.mw.setup_middleware()

    def test_handle_outbound(self):
        msg_1 = self.mkmsg_out(to_addr="+2561111")
        msg_1 = self.mw.handle_outbound(msg_1 , 'dummy_endpoint')
        self.assertEqual(msg_1['to_addr'], '1111')
        
        msg_2 = self.mkmsg_out(to_addr="2561111")
        msg_2 = self.mw.handle_outbound(msg_2 , 'dummy_endpoint')
        self.assertEqual(msg_2['to_addr'], '1111')
    


class VusionAddressAddInternationalPrefix(VumiTestCase, MessageMaker):
    
    def setUp(self):
        dummy_worker = object()
        self.mw = VusionAddressMiddleware(
            'mw1',
            {'international_prefix': '254'},
            dummy_worker)
        self.mw.setup_middleware()

    def test_handle_inbound(self):
        msg_1 = self.mkmsg_in(from_addr="888")
        msg_1 = self.mw.handle_inbound(msg_1 , 'dummy_endpoint')
        self.assertEqual(msg_1['from_addr'], '+254888')
