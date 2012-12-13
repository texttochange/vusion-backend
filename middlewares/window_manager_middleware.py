import redis

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from vumi.middleware import BaseMiddleware
from vumi.message import TransportUserMessage
from vumi.log import log

from components.window_manager import WindowManager

from transports.push_tz_smpp import StopPropagation

class WindowManagerMiddleware(BaseMiddleware):

    @inlineCallbacks
    def setup_middleware(self, r_server=None):
        r_config = self.config.get('redis_config', {})
        if r_server is None:
            r_server = redis.Redis(**r_config)
        self.transport_name = self.worker.transport_name

        r_key = ':'.join(['middlewarewindows', self.transport_name])

        self.wm = WindowManager(
            r_server,
            window_size=self.config.get('window_size', 10),
            flight_lifetime=self.config.get('flight_lifetime', 1),
            gc_interval=self.config.get('gc_interval', 1),
            window_key=r_key)

        self.wm.monitor(
            self.send_outbound,
            self.config['monitor_loop'],
            False)
        
        if not (yield self.wm.window_exists(self.transport_name)):
            yield self.wm.create_window(self.transport_name)

    def teardown_middleware(self):
        self.wm.stop()

    @inlineCallbacks
    def handle_event(self, event, endpoint):
        log.msg("handle event %r" % event)
        if event["event_type"] in ['ack', 'nack']:
            yield self.wm.remove_key(
                self.transport_name,
                event['user_message_id'])
        returnValue(event)

    @inlineCallbacks
    def handle_outbound(self, msg, endpoint):
        yield self.wm.add(self.transport_name, msg.to_json(), msg["message_id"])
        raise StopPropagation()

    @inlineCallbacks
    def send_outbound(self, window_id, key):
        data = yield self.wm.get_data(window_id, key)
        msg = TransportUserMessage.from_json(data)
        # TODO store the endpoint in the stored data
        self.resume_handling('outbound', msg, self.transport_name)
        self.worker.handle_outbound_message(msg)

    def resume_handling(self, handle_name, message, endpoint):
        return self.worker._middlewares.resume_handling(
            self, handle_name, message, endpoint)