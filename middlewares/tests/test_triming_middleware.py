from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial.unittest import TestCase

from middlewares.triming_middleware import TrimingMiddleware
from tests.utils import MessageMaker


class TrimingMiddlewareTestCase(TestCase, MessageMaker):
    
    def setUp(self):
        dummy_worker = object()
        self.mw = TrimingMiddleware('mw1', {'extra_trim':'"'}, dummy_worker)
        self.mw.setup_middleware()

    def test_handle_inbound_empty(self):
        msg = self.mkmsg_in(content=None)
        msg = self.mw.handle_inbound(msg , 'dummy_endpoint')
        self.assertEqual(
            msg['content'],
            '')

    def test_handle_inbound_space(self):
        msg = self.mkmsg_in(content=' to be trimed ')
        msg = self.mw.handle_inbound(msg , 'dummy_endpoint')
        self.assertEqual(
            msg['content'],
            'to be trimed')
        
    def test_handle_inbound_newline(self):
        msg = self.mkmsg_in(content='\nto be trimed\n')
        msg = self.mw.handle_inbound(msg , 'dummy_endpoint')
        self.assertEqual(
            msg['content'],
            'to be trimed')
    
    def test_handle_inbound_tab(self):
        msg = self.mkmsg_in(content='\tto be trimed\t')
        msg = self.mw.handle_inbound(msg , 'dummy_endpoint')
        self.assertEqual(
            msg['content'],
            'to be trimed')
        
    def test_handle_inbound_custom(self):
        msg = self.mkmsg_in(content='"mtoto" welcome"')
        msg = self.mw.handle_inbound(msg, 'dummy_endpoint')
        self.assertEqual(msg['content'], 'mtoto" welcome')
        
    def test_handle_inbound_custom_none(self):
        dummy_worker = object()
        mw = TrimingMiddleware('mw1', {'extra_trim':''}, dummy_worker)
        mw.setup_middleware()
        
        msg = self.mkmsg_in('"mtoto" ')
        msg = mw.handle_inbound(msg, 'dummy_endpoint')
        self.assertEqual(msg['content'], '"mtoto"')
        
        mw2 = TrimingMiddleware('mw2', {}, dummy_worker)
        mw2.setup_middleware()
        
        msg = self.mkmsg_in('"mtoto" ')
        msg = mw2.handle_inbound(msg, 'dummy_endpoint')
        self.assertEqual(msg['content'], '"mtoto"')
        
        mw3 = TrimingMiddleware('mw3', {'extra_trim': None}, dummy_worker)
        mw3.setup_middleware()
        
        msg = self.mkmsg_in('"mtoto" ')
        msg = mw3.handle_inbound(msg, 'dummy_endpoint')
        self.assertEqual(msg['content'], '"mtoto"')
            