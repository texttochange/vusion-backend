from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial.unittest import TestCase
from twisted.internet.defer import setDebugging
from twisted.internet import reactor

from middlewares.custom_middleware_stack import CustomMiddlewareStack, StopPropagation
from middlewares.window_manager_middleware import WindowManagerMiddleware
from components.tests.test_window_manager import FakeRedis
from tests.utils import MessageMaker

from middlewares.tests.test_custom_middleware_stack import DummyTransport

class ToyWorker(object):
    
    transport_name = 'transport'
    messages = []
        
    def _process_message(self, msg, from_middleware):
        self.messages.append(msg)


class WindowManagerTestCase(TestCase, MessageMaker):

    @inlineCallbacks
    def setUp(self):
        toy_worker = ToyWorker()
        self.transport_name = toy_worker.transport_name
        config = {'window_size': 2,
                  'flight_lifetime': 1,
                  'monitor_loop': 0.5}
        self.mw = WindowManagerMiddleware('mw1', config, toy_worker)
        yield self.mw.setup_middleware(FakeRedis())
        toy_worker._middlewares = CustomMiddlewareStack([self.mw])

    def tearDown(self):
        self.mw.teardown_middleware()

    @inlineCallbacks
    def test_handle_outbound(self):
        msg_1 = self.mkmsg_out(message_id='1')
        yield self.assertFailure(
            self.mw.handle_outbound(msg_1, self.transport_name),
            StopPropagation)

        msg_2 = self.mkmsg_out(message_id='2')
        yield self.assertFailure(
            self.mw.handle_outbound(msg_2, self.transport_name),
            StopPropagation)

        msg_3 = self.mkmsg_out(message_id='3')
        yield self.assertFailure(
            self.mw.handle_outbound(msg_3, self.transport_name),
            StopPropagation)

        count_waiting = yield self.mw.wm.count_waiting(self.transport_name)
        self.assertEqual(3, count_waiting)        

        yield self.mw.wm._monitor_windows(self.mw.send_outbound)
        self.assertEqual(1, self.mw.wm.count_waiting(self.transport_name))
        self.assertEqual(2, self.mw.wm.count_in_flight(self.transport_name))
        self.assertEqual(2, len(self.mw.worker.messages))

        #acknoledge one
        ack = self.mkmsg_ack(user_message_id="1")
        yield self.mw.handle_event(ack, self.transport_name)
        self.assertEqual(1, self.mw.wm.count_in_flight(self.transport_name))

        yield self.mw.wm._monitor_windows(self.mw.send_outbound)
        self.assertEqual(2, self.mw.wm.count_in_flight(self.transport_name))
