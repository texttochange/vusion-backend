from copy import deepcopy
from twisted.internet.defer import (
    inlineCallbacks, returnValue, DeferredList, succeed, Deferred)
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from vumi.middleware import BaseMiddleware
from vumi.message import TransportUserMessage
from vumi.log import log

from components.window_manager import WindowManager

from middlewares.custom_middleware_stack import StopPropagation

try:
    import txredis.client as txrc
    txr = txrc
except ImportError:
    import txredis.protocol as txrp
    txr = txrp

#TODO: remove at upgrade of Vumi
class VumiRedis(txr.Redis):
    """Wrapper around txredis to make it more suitable for our needs.

    Aside from the various API operations we need to implement to match the
    other redis client, we add a deferred that fires when we've finished
    connecting to the redis server. This avoids problems with trying to use a
    client that hasn't completely connected yet.

    TODO: We need to find a way to test this stuff

    """

    def __init__(self, *args, **kw):
        super(VumiRedis, self).__init__(*args, **kw)
        self.connected_d = Deferred()

    def connectionMade(self):
        d = super(VumiRedis, self).connectionMade()
        d.addCallback(lambda _: self)
        return d.chainDeferred(self.connected_d)

    def hget(self, key, field):
        d = super(VumiRedis, self).hget(key, field)
        d.addCallback(lambda r: r.get(field) if r else None)
        return d

    def lrem(self, key, value, num=0):
        return super(VumiRedis, self).lrem(key, value, count=num)

    # lpop() and rpop() are implemented in txredis 2.2.1 (which is in Ubuntu),
    # but not 2.2 (which is in pypi). Annoyingly, pop() in 2.2.1 calls lpop()
    # and rpop(), so we can't just delegate to that as we did before.

    def rpop(self, key):
        self._send('RPOP', key)
        return self.getResponse()

    def lpop(self, key):
        self._send('LPOP', key)
        return self.getResponse()

    def setex(self, key, seconds, value):
        return self.set(key, value, expire=seconds)

    # setnx() is implemented in txredis 2.2.1 (which is in Ubuntu), but not 2.2
    # (which is in pypi). Annoyingly, set() in 2.2.1 calls setnx(), so we can't
    # just delegate to that as we did before.

    def setnx(self, key, value):
        self._send('SETNX', key, value)
        return self.getResponse()

    def zadd(self, key, *args, **kwargs):
        if args:
            if len(args) % 2 != 0:
                raise ValueError("ZADD requires an equal number of "
                                 "values and scores")
        pieces = zip(args[::2], args[1::2])
        pieces.extend(kwargs.iteritems())
        orig_zadd = super(VumiRedis, self).zadd
        deferreds = [orig_zadd(key, member, score) for member, score in pieces]
        d = DeferredList(deferreds, fireOnOneErrback=True)
        d.addCallback(lambda results: sum([result for success, result
                                            in results if success]))
        return d

    def zrange(self, key, start, end, desc=False, withscores=False):
        return super(VumiRedis, self).zrange(key, start, end,
                                             withscores=withscores,
                                             reverse=desc)

    def zrangebyscore(self, key, min, max, start=None, num=None,
                     withscores=False, score_cast_func=float):
        d = super(VumiRedis, self).zrangebyscore(key, min, max,
                        offset=start, count=num, withscores=withscores)
        if withscores:
            d.addCallback(lambda r: [(v, score_cast_func(s)) for v, s in r])
        return d


class VumiRedisClientFactory(txr.RedisClientFactory):
    protocol = VumiRedis


class WindowManagerMiddleware(BaseMiddleware):

    queue_name = 'outbound'

    def get_redis(self, r_config):
        r_config = deepcopy(r_config)
        host = r_config.pop('host', 'locahost')
        port = r_config.pop('port', 6379)
        factory = VumiRedisClientFactory(**r_config)
        d = factory.deferred.addCallback(lambda r: r.connected_d)
        reactor.connectTCP(host, port, factory)
        return d

    def get_r_config(self):
        r_config = self.config.get('redis_config', {})
        if r_config != {} or not hasattr(self.worker, 'config'):
            return r_config
        if self.worker.config.has_key('redis'):
            return self.worker.config.get('redis', {})
        return self.worker.config.get('redis_config', {})

    @inlineCallbacks
    def setup_middleware(self, r_server=None):
        r_config = self.get_r_config()
        if r_server is None:
            r_server = yield self.get_redis(r_config)
        self.transport_name = self.worker.transport_name

        r_key = ':'.join(['middlewarewindows', self.transport_name])
        
        self.wm = WindowManager(
            r_server,
            window_size=self.config.get('window_size', 10),
            flight_lifetime=self.config.get('flight_lifetime', 1),
            gc_interval=self.config.get('gc_interval', 1),
            window_key=r_key,
            remove_expired=True)

        self.wm.monitor(
            self.send_outbound,
            self.config.get('monitor_loop', 1),
            False)
        
        if not (yield self.wm.window_exists(self.queue_name)):
            yield self.wm.create_window(self.queue_name)

    def teardown_middleware(self):
        self.wm.stop()

    @inlineCallbacks
    def handle_event(self, event, endpoint):
        log.msg("handle event %r" % event)
        #other event could maybe remove the key
        if event["event_type"] in ['ack', 'nack']:
            yield self.wm.remove_key(
                self.queue_name, event['user_message_id'])
        returnValue(event)

    @inlineCallbacks
    def handle_outbound(self, msg, endpoint):
        yield self.wm.add(
            self.queue_name, msg.to_json(), msg["message_id"])
        raise StopPropagation()

    @inlineCallbacks
    def send_outbound(self, window_id, key):
        data = yield self.wm.get_data(window_id, key)
        msg = TransportUserMessage.from_json(data)
        self.worker._process_message(msg, self)
