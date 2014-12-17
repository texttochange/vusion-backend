from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.persist.txredis_manager import TxRedisManager
from vumi.middleware import BaseMiddleware
from vumi.log import log

from components.rate_manager import RateManager


class RateManagerMiddleware(BaseMiddleware):

    RATE_MANAGER_KEY = 'rate_managers'
    clock = reactor

    @inlineCallbacks
    def setup_middleware(self):
        self._unpause_delayedCall = None
        self._unpause_check_delay = self.config.get('unpause_chech_delay', 0.5)

        window_size = self.config.get('window_size', 10)
        per_seconds = self.config.get('per_seconds', 1)
        r_config = self.config.get('redis_manager', {})
        key_prefix = r_config.get('key_prefix', self.RATE_MANAGER_KEY)
        r_config['key_prefix'] = ':'.join([key_prefix, self.worker.transport_name])
        log.msg("User redis manager config %r" % r_config)
        self.redis = yield TxRedisManager.from_config(r_config)
        self.sub_man = self.redis.sub_manager(self.RATE_MANAGER_KEY)
        self.manager = RateManager(self.sub_man, window_size, per_seconds)

    def teardown_middleware(self):
        if self._unpause_delayedCall is not None:
            self._unpause_delayedCall.cancel()
        yield self.redis._close()

    @inlineCallbacks
    def handle_outbound(self, msg, endpoint):
        is_within_rate = yield self.manager.is_within_rate(msg['message_id'])
        if not is_within_rate:
            log.msg("Pausing")
            self.worker.pause_connectors()
            self._unpause_delayedCall = self.clock.callLater(
                self._unpause_check_delay, self._check_unpause)
        returnValue(msg)

    @inlineCallbacks
    def _check_unpause(self):
        can_unpause = yield self.manager.has_room()
        if can_unpause:
            log.msg("Unpausing")
            self.worker.unpause_connectors()
            return
        self._unpause_delayedCall = self.clock.callLater(
            self._unpause_check_delay, self._check_unpause)
