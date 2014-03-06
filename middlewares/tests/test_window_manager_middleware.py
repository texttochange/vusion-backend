from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial.unittest import TestCase
from twisted.internet.defer import setDebugging
from twisted.internet import reactor
from twisted.internet.task import Clock

from middlewares.custom_middleware_stack import CustomMiddlewareStack, StopPropagation
from middlewares.window_manager_middleware import WindowManagerMiddleware
from components.tests.test_window_manager import FakeRedis
from components.window_manager import WindowManager
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
        
        self.clock = Clock()
        def mock_clock_time(self):
            return self._clocktime
        self.patch(WindowManager, 'get_clocktime', mock_clock_time)
        self.mw.wm._clocktime = 0        
        

    def tearDown(self):
        self.mw.teardown_middleware()

    @inlineCallbacks
    def test_handle_outbound(self):
        msg_1 = self.mkmsg_out(message_id='1')
        yield self.assertFailure(
            self.mw.handle_outbound(msg_1, self.transport_name),
            StopPropagation)
        stored_msg_1 = yield self.mw.wm.get_data(self.transport_name, '1')
        self.assertEqual(msg_1.to_json(), stored_msg_1)

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
        #make sure it has been deleted
        stored_msg_1 = yield self.mw.wm.get_data(self.transport_name, '1')
        self.assertTrue(stored_msg_1 is None)

        yield self.mw.wm._monitor_windows(self.mw.send_outbound)
        self.assertEqual(2, self.mw.wm.count_in_flight(self.transport_name))

        #now they expire
        self.mw.wm._clocktime = 20
        yield self.mw.wm.clear_expired_flight_keys()
        #make sure it has been deleted
        stored_msg_2 = yield self.mw.wm.get_data(self.transport_name, '2')    
        # the expired message should be deleted
        self.assertTrue(stored_msg_2 is None)
        
        #the expired flight keys should be cleanedup
        self.assertEqual(
            [],
            self.mw.wm.get_expired_flight_keys(self.transport_name))
        
        self.assertEqual(
            0,
            self.mw.wm.count_in_flight(self.transport_name)) 
        
