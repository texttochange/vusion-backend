from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial.unittest import TestCase
from twisted.internet.defer import setDebugging
from twisted.internet import reactor
from twisted.internet.task import Clock

from vumi.transports import Transport
from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.helpers import PersistenceHelper, VumiTestCase

from middlewares.custom_middleware_stack import (
    CustomMiddlewareStack, StopPropagation, useCustomMiddleware)
from middlewares.window_manager_middleware import WindowManagerMiddleware
from components.window_manager import VusionWindowManager


@useCustomMiddleware
class DummyTransport(Transport):
    
    outbound_msgs = []
        
    def handle_outbound_message(self, message):
        self.outbound_msgs.append(message)


class WindowManagerTestCase(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(DummyTransport))
        self.transport = yield self.tx_helper.get_transport({})
        
        self.persistence_helper = self.add_helper(PersistenceHelper())
        redis = yield self.persistence_helper.get_redis_manager()
        
        self.transport_name = self.transport.transport_name
        config = {'window_size': 2,
                  'flight_lifetime': 1,
                  'monitor_loop': 0.5}
        self.mw = WindowManagerMiddleware('mw1', config, self.transport)

        yield self.mw.setup_middleware(redis)
                
        self.clock = Clock()
        self.patch(VusionWindowManager, 'get_clock', lambda _: self.clock)
        def mock_clock_time(self):
            return self._clocktime
        self.patch(VusionWindowManager, 'get_clocktime', mock_clock_time)
        self.mw.wm._clocktime = 0

    @inlineCallbacks
    def tearDown(self):
        yield self.mw.teardown_middleware()
        yield super(WindowManagerTestCase, self).tearDown()

    @inlineCallbacks
    def test_handle_outbound(self):
        msg_1 = self.tx_helper.make_outbound('hello world 1', message_id='1')
        yield self.assertFailure(
            self.mw.handle_outbound(msg_1, self.mw.queue_name),
            StopPropagation)
        stored_msg_1 = yield self.mw.wm.get_data(self.mw.queue_name, '1')
        self.assertEqual(msg_1.to_json(), stored_msg_1)

        msg_2 = self.tx_helper.make_outbound('hello world 2', message_id='2')
        yield self.assertFailure(
            self.mw.handle_outbound(msg_2, self.mw.queue_name),
            StopPropagation)

        msg_3 = self.tx_helper.make_outbound('hello world 3', message_id='3')
        yield self.assertFailure(
            self.mw.handle_outbound(msg_3, self.mw.queue_name),
            StopPropagation)

        count_waiting = yield self.mw.wm.count_waiting(self.mw.queue_name)
        self.assertEqual(3, count_waiting)        

        yield self.mw.wm._monitor_windows(self.mw.send_outbound)
        count_waiting = yield self.mw.wm.count_waiting(self.mw.queue_name)
        self.assertEqual(1, count_waiting)
        count_in_flight = yield self.mw.wm.count_in_flight(self.mw.queue_name)
        self.assertEqual(2, count_in_flight)
        self.assertEqual(2, len(self.mw.worker.outbound_msgs))

        #acknoledge one
        ack = self.tx_helper.make_ack(sent_message_id='1')
        yield self.mw.handle_event(ack, self.mw.queue_name)
        count_in_flight = yield self.mw.wm.count_in_flight(self.mw.queue_name)
        self.assertEqual(1, count_in_flight)
        #make sure it has been deleted
        stored_msg_1 = yield self.mw.wm.get_data(self.mw.queue_name, '1')
        self.assertTrue(stored_msg_1 is None)

        yield self.mw.wm._monitor_windows(self.mw.send_outbound)
        count_in_flight = yield self.mw.wm.count_in_flight(self.mw.queue_name)
        self.assertEqual(2, count_in_flight)

        #now they expire
        self.mw.wm._clocktime = 20
        yield self.mw.wm.clear_expired_flight_keys()
        #make sure it has been deleted
        stored_msg_2 = yield self.mw.wm.get_data(self.mw.queue_name, '2')    
        # the expired message should be deleted
        self.assertTrue(stored_msg_2 is None)
        
        #the expired flight keys should be cleanedup
        expired_flight_keys = yield self.mw.wm.get_expired_flight_keys(self.mw.queue_name)
        self.assertEqual([], expired_flight_keys)
        
        count_in_flight = yield self.mw.wm.count_in_flight(self.mw.queue_name)
        self.assertEqual(0, count_in_flight) 
        
