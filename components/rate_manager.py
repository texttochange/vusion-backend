from twisted.internet.defer import inlineCallbacks, returnValue


class RateManager(object):

    def __init__(self, redis, window_size=100, per_seconds=1):
        self.window_size = window_size
        self.per_seconds = per_seconds
        self.redis = redis

    def rate_key(self, key):
        return self.redis._key(key)

    def rate_keys(self):
        return self.redis._key('*')

    @inlineCallbacks
    def is_within_rate(self, message_id):
        allowed = yield self.has_room()
        yield self.redis.setex(
            self.rate_key(message_id),
            self.per_seconds,
            '')
        returnValue(allowed)

    @inlineCallbacks
    def has_room(self):
        inflight_keys = yield self.redis.keys(self.rate_keys())
        if self.window_size > len(inflight_keys):
            returnValue(True)
        returnValue(False)
