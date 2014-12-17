from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred

from vumi.tests.helpers import VumiTestCase, PersistenceHelper
from components.rate_manager import RateManager


def wait(secs):
    d = Deferred()
    reactor.callLater(secs, d.callback, None)
    return d


class RateManagerTestCase(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.redis = yield self.persistence_helper.get_redis_manager()
        yield self.redis._purge_all()
        self.manager = RateManager(self.redis, window_size=1, per_seconds=1)

    @inlineCallbacks
    def tearDown(self):
        yield self.redis._purge_all()
        yield self.redis._close()

    @inlineCallbacks
    def test_is_within_rate(self):
        allowed = yield self.manager.is_within_rate('1')
        self.assertTrue(allowed)
        allowed = yield self.manager.is_within_rate('2')
        self.assertFalse(allowed)
        
        #with prior version or redis 2.4 the expiring of key is not very accurate
        yield wait(2.0)
        
        allowed = yield self.manager.is_within_rate('3')
        self.assertTrue(allowed)