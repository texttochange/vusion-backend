from twisted.internet.defer import inlineCallbacks

from vumi.transports import Transport
from vumi.transports.tests.test_base import TransportTestCase
from vumi.middleware import setup_middlewares_from_config
from vumi.message import TransportUserMessage

from middlewares.custom_middleware_stack import CustomMiddlewareStack

class DummyTransport(Transport):
    
    @inlineCallbacks
    def setup_middleware(self):
           middlewares = yield setup_middlewares_from_config(self, self.config)
           self._middlewares = CustomMiddlewareStack(middlewares)
   
    # Overwriting the methode for middleware hook
    def _process_message(self, message, from_middleware=None):
           def _send_failure(f):
               self.send_failure(message, f.value, f.getTraceback())
               log.err(f)
               if self.SUPPRESS_FAILURE_EXCEPTIONS:
                   return None
               return f
           
           d = self._middlewares.apply_consume(
               "outbound", message, self.transport_name, from_middleware)
           d.addCallback(self.handle_outbound_message)
           d.addErrback(self._middlewares.process_control_flag)
           d.addErrback(_send_failure)
           return d
    
    def _publish_message(self, message, from_middleware=None):
        d = self._middlewares.apply_publish(
            "inbound", message, self.transport_name, from_middleware)
        d.addCallback(self.message_publisher.publish_message)
        d.addErrback(self._middlewares.process_control_flag)        
        return d        

    def publish_message(self, from_middleware=None, **kw):
        kw.setdefault('transport_name', self.transport_name)
        kw.setdefault('transport_metadata', {})
        msg = TransportUserMessage(**kw)
        return self._publish_message(msg, from_middleware)


class CustomMiddlewareStackTestCase(TransportTestCase):
    """
    This is a test for the base Transport class.

    Not to be confused with TransportTestCase above.
    """

    transport_name = 'carrier_pigeon'
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

    @inlineCallbacks
    def test_start_transport(self):
        tr = yield self.get_transport({})
        self.assertEqual(self.transport_name, tr.transport_name)
        self.assert_basic_rkeys(tr)

    @inlineCallbacks
    def test_middleware_for_inbound_stop_propagation(self):
        transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG_BASIC)
        orig_msg = self.mkmsg_in()
        orig_msg['timestamp'] = 0
        yield transport.publish_message(**orig_msg.payload)
        msgs = self.get_dispatched_messages()        
        self.assertEqual(msgs, [])

    @inlineCallbacks
    def test_middleware_for_inbound_resume_propagation(self):
        transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG_BASIC)
        orig_msg = self.mkmsg_in()
        orig_msg['timestamp'] = 0
        middleware = transport._middlewares.middlewares[1]
        yield transport._publish_message(orig_msg, from_middleware=middleware)
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['record'], [
                   ['mw1', 'inbound', self.transport_name],
                   ])

    @inlineCallbacks
    def test_middleware_for_outbound_stop_propagation(self):
        transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG_BASIC)
        msgs = []
        transport.handle_outbound_message = msgs.append
        orig_msg = self.mkmsg_out()
        orig_msg['timestamp'] = 0
        yield self.dispatch(orig_msg)
        self.assertEqual(msgs, [])

    @inlineCallbacks
    def test_middleware_for_outbound_resume_propagation(self):
        transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG_BASIC)
        msgs = []
        transport.handle_outbound_message = msgs.append
        orig_msg = self.mkmsg_out()
        orig_msg['timestamp'] = 0
        middleware = transport._middlewares.middlewares[1]
        yield transport._process_message(orig_msg, middleware)
        #self.assertEqual(msgs, [])
        [msg] = msgs
        self.assertEqual(msg['record'], [
            ('mw3', 'outbound', self.transport_name),
        ])
    
    @inlineCallbacks
    def test_middleware_for_outbound_resume_propagation_stoponly(self):
        transport = yield self.get_transport(self.TEST_MIDDLEWARE_CONFIG_STOPONLY)
        msgs = []
        transport.handle_outbound_message = msgs.append
        orig_msg = self.mkmsg_out()
        orig_msg['timestamp'] = 0
        middleware = transport._middlewares.middlewares[0]
        yield transport._process_message(orig_msg, middleware)
        #self.assertEqual(msgs, [])
        [msg] = msgs
        self.assertEqual(msg, orig_msg)    