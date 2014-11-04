# -*- test-case-name: components.tests.test_window_manager -*-
import json
import uuid

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import LoopingCall

from vumi.log import log
from vumi.components.window_manager import WindowManager


class VusionWindowManager(WindowManager):

    def __init__(self, redis, window_size=100, flight_lifetime=None,
                gc_interval=10, window_key=None, remove_expired = False):
        super(VusionWindowManager, self).__init__(
            redis, window_size, flight_lifetime, gc_interval);
        if window_key is not None:
            self.WINDOW_KEY = window_key
        self.remove_expired = remove_expired

    @inlineCallbacks
    def get_data(self, window_id, key):
        json_data = yield self.redis.get(self.window_key(window_id, key))
        if json_data is None:
            returnValue(None)
        returnValue(json.loads(json_data))

    @inlineCallbacks
    def clear_expired_flight_keys(self):
        windows = yield self.get_windows()
        for window_id in windows:
            expired_keys = yield self.get_expired_flight_keys(window_id)
            for key in expired_keys:
                if self.remove_expired is False:
                    yield self.redis.lrem(self.flight_key(window_id), key, 1)
                else:
                    yield self.remove_key(window_id, key)
