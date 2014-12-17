
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet.task import Clock

from vumi.transports import Transport
from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.helpers import VumiTestCase, PersistenceHelper

from middlewares import RateManagerMiddleware

class DummyTransport(Transport):

    outbound_msgs = []
    paused = False

    def handle_outbound_message(self, message):
        self.outbound_msgs.append(message)

    def pause_connectors(self):
        self.paused = True
        return super(Transport, self).pause_connectors()

    def unpause_connectors(self):
        self.paused = False
        return super(Transport, self).unpause_connectors()


def wait(secs):
    d = Deferred()
    reactor.callLater(secs, d.callback, None)
    return d


class RateManagerMiddlewareTestCase(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(DummyTransport))
        self.transport = yield self.tx_helper.get_transport({})
        
        self.persistence_helper = self.add_helper(PersistenceHelper())
        redis = yield self.persistence_helper.get_redis_manager()
        
        self.transport_name = self.transport.transport_name
        config = {'window_size': 1,
                  'per_seconds': 1,
                  'check_unpause_delay': 0.05}
        self.rm = RateManagerMiddleware('rm', config, self.transport)
 
        yield self.rm.setup_middleware()

    @inlineCallbacks
    def tearDown(self):
        yield self.rm.teardown_middleware()
        yield super(RateManagerMiddlewareTestCase, self).tearDown()

    @inlineCallbacks
    def test_handle_outbound(self):
        msg_1 = self.tx_helper.make_outbound('hello world 1', message_id='1')
        msg_1_after = yield self.rm.handle_outbound(msg_1, 'outbound')
        self.assertFalse(self.transport.paused)
        
        msg_2 = self.tx_helper.make_outbound('hello world 2', message_id='2')
        msg_2_after = yield self.rm.handle_outbound(msg_2, 'outbound')
        self.assertTrue(self.transport.paused)

        yield wait(1.5)
        self.assertFalse(self.transport.paused)