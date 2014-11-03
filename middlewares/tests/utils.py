from vumi.middleware.base import BaseMiddleware
from middlewares.custom_middleware_stack import StopPropagation


class StopPropagationMiddleware(BaseMiddleware):

    outbound_msgs = []

    def handle_inbound(self, message, endpoint):
        raise StopPropagation()

    def handle_outbound(self, message, endpoint):
        self.outbound_msgs.append(message)
        raise StopPropagation()

    def handle_event(self, message, endpoint):
        raise StopPropagation()

    def handle_failure(self, message, endpoint):
        raise StopPropagation()
