from vumi.middleware.base import BaseMiddleware
from middlewares.custom_middleware_stack import StopPropagation


class StopPropagationMiddleware(BaseMiddleware):

    def handle_inbound(self, message, endpoint):
        raise StopPropagation()

    def handle_outbound(self, message, endpoint):
        raise StopPropagation()

    def handle_event(self, message, endpoint):
        raise StopPropagation()

    def handle_failure(self, message, endpoint):
        raise StopPropagation()
