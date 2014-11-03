from twisted.internet.defer import inlineCallbacks

from vumi.transports import Transport
from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import VumiTestCase
from vumi.middleware import setup_middlewares_from_config
from vumi.message import TransportUserMessage

from middlewares.custom_middleware_stack import CustomMiddlewareStack, useCustomMiddleware

@useCustomMiddleware
class DummyTransport(Transport):

    outbound_msgs = []

    def handle_outbound_message(self, message):
        self.outbound_msgs.append(message)


class CustomMiddlewareStackTestCase(VumiTestCase):
    """
    This is a test for the base Transport class.

    Not to be confused with TransportTestCase above.
    """

    transport_class = DummyTransport

    TEST_MIDDLEWARE_CONFIG_BASIC = {
       "middleware": [
            {"mw1": "vumi.middleware.tests.utils.RecordingMiddleware"},
            {"mw2": "middlewares.tests.utils.StopPropagationMiddleware"},
            {"mw3": "vumi.middleware.tests.utils.RecordingMiddleware"},
            ],
        }

    TEST_MIDDLEWARE_CONFIG_STOPONLY = {
       "middleware": [
            {"mw1": "middlewares.tests.utils.StopPropagationMiddleware"},
            ],
        }    

    #@inlineCallbacks
    #def test_start_transport(self):
        #tx_helper = self.add_helper(TransportHelper(DummyTransport))
        #tr = yield tx_helper.get_transport({})
        #self.assertEqual('sphex', tr.transport_name)
        #self.assert_basic_rkeys(tr)

    #@inlineCallbacks
    #def test_middleware_for_inbound_stop_propagation(self):
        #tx_helper = self.add_helper(TransportHelper(DummyTransport))
        #transport = yield tx_helper.get_transport(self.TEST_MIDDLEWARE_CONFIG_BASIC)
        
        ##transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG_BASIC)
        ##orig_msg = self.mkmsg_in()
        ##orig_msg['timestamp'] = 0
        #yield tx_helper.make_dispatch_inbound('hello world', timestamp=0)
        #msgs = yield tx_helper.get_dispatched_inbound()
        #self.assertEqual(msgs, [])

    #@inlineCallbacks
    #def test_middleware_for_inbound_resume_propagation(self):
        #transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG_BASIC)
        #orig_msg = self.mkmsg_in()
        #orig_msg['timestamp'] = 0
        #middleware = transport._middlewares.middlewares[1]
        #yield transport._publish_message(orig_msg, from_middleware=middleware)
        #[msg] = self.get_dispatched_messages()
        #self.assertEqual(msg['record'], [
                   #['mw1', 'inbound', self.transport_name],
                   #])

    @inlineCallbacks
    def test_middleware_for_outbound_stop_propagation(self):
        tx_helper = self.add_helper(TransportHelper(DummyTransport))
        transport = yield tx_helper.get_transport(self.TEST_MIDDLEWARE_CONFIG_BASIC)
        
        yield tx_helper.make_dispatch_outbound('hello world', timestamp=0)
        [msgs_stopped] = transport.middlewares[1].outbound_msgs
        m, o, s = msgs_stopped['record'][0]
        self.assertEqual('mw1', m)
        self.assertEqual('outbound', o)
        self.assertEqual('sphex', s)
                        
    
    @inlineCallbacks
    def test_middleware_for_outbound_resume_propagation(self):
        tx_helper = self.add_helper(TransportHelper(DummyTransport))
        transport = yield tx_helper.get_transport(self.TEST_MIDDLEWARE_CONFIG_BASIC)
        msg = tx_helper.msg_helper.make_outbound('hello world')
        middleware = transport.middlewares[1]
        yield transport.connectors['sphex']._consume_message('outbound', msg, middleware)
        [msg_sent] = transport.outbound_msgs
        m, o, s = msg_sent['record'][0]
        self.assertEqual('mw3', m)
        self.assertEqual('outbound', o)
        self.assertEqual('sphex', s)        
        
       # middleware = transport.connectors
        #self.tx_helper
        #transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG_BASIC)
        #msgs = []
        #transport.handle_outbound_message = msgs.append
        #orig_msg = self.mkmsg_out()
        #orig_msg['timestamp'] = 0
        #middleware = transport._middlewares.middlewares[1]
        #yield transport._process_message(orig_msg, middleware)
        ##self.assertEqual(msgs, [])
        #[msg] = msgs
        #self.assertEqual(msg['record'], [
            #('mw3', 'outbound', self.transport_name),
        #])
    
    #@inlineCallbacks
    #def test_middleware_for_outbound_resume_propagation_stoponly(self):
        #transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG_STOPONLY)
        #msgs = []
        #transport.handle_outbound_message = msgs.append
        #orig_msg = self.mkmsg_out()
        #orig_msg['timestamp'] = 0
        #middleware = transport._middlewares.middlewares[0]
        #yield transport._process_message(orig_msg, middleware)
        ##self.assertEqual(msgs, [])
        #[msg] = msgs
        #self.assertEqual(msg, orig_msg)
