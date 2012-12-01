from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial.unittest import TestCase

from vumi.middleware.tests.test_base import MiddlewareStack

from middlewares.triming_middleware import TrimingMiddleware
from tests.utils import MessageMaker


class TrimingMiddlewareTestCase(TestCase, MessageMaker):
    
    def setUp(self):
        dummy_worker = object()
        self.mw = TrimingMiddleware('mw1', {}, dummy_worker)
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