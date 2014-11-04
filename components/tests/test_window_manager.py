import fnmatch
from functools import wraps
from itertools import takewhile, dropwhile

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock

from vumi.tests.helpers import VumiTestCase
from components.window_manager import VusionWindowManager
    

class VusionWindowManagerTestCase(VumiTestCase):

    #@inlineCallbacks
    def setUp(self):
        pass
        #self._persist_setUp()
        #modify for backward compatibility
        #redis = FakeRedis()
        #self.window_id = 'window_id'

        ## Patch the clock so we can control time
        #self.clock = Clock()
        #self.patch(WindowManager, 'get_clock', lambda _: self.clock)

        #self.wm = VusionWindowManager(redis, window_size=10, flight_lifetime=10)
        #yield self.wm.create_window(self.window_id)
        #self.redis = self.wm.redis

    @inlineCallbacks
    def tearDown(self):
        #yield self.wm.stop()
        yield super(VusionWindowManagerTestCase, self).tearDown()

    def test_todo(self):
        self.fail()