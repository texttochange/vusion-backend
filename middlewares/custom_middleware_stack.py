from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred

from vumi.middleware import MiddlewareStack, setup_middlewares_from_config


class MiddlewareControlFlag(Exception):
    pass


class StopPropagation(MiddlewareControlFlag):
    pass


class CustomMiddlewareStack(MiddlewareStack):

    def _get_middleware_index(self, middlewares, middleware):
        return middlewares.index(middleware)

    @inlineCallbacks
    def resume_handling(self, mw, handle_name, message, endpoint):
        mw_index = self._get_middleware_index(mw)
        #In case there are no other middleware after this one
        if mw_index + 1 == len(self.middlewares):
            returnValue(message)
        message = yield self._handle(self.middlewares, handle_name, message, endpoint, mw_index + 1)
        returnValue(message)

    @inlineCallbacks
    def _handle(self, middlewares, handler_name, message, endpoint, from_middleware=None):
        method_name = 'handle_%s' % (handler_name,)
        middlewares = list(middlewares)
        if len(middlewares) == 0:
            returnValue(message)
        if from_middleware is not None:
            from_index = 1 + self._get_middleware_index(middlewares, from_middleware)
        else:
            from_index = 0
        #In case there are no other middleware after this one
        if from_index == len(middlewares):
            returnValue(message)        
        for index, middleware in enumerate(middlewares[from_index:]):
            handler = getattr(middleware, method_name)
            message = yield self._handle_middleware(handler, message, endpoint, index)
            if message is None:
                raise MiddlewareError('Returned value of %s.%s should never ' \
                                'be None' % (middleware, method_name,))
        returnValue(message)

    def _handle_middleware(self, handler, message, endpoint, index):        
        def _handle_control_flag(f):
            if not isinstance(f.value, MiddlewareControlFlag):
                raise f
            if isinstance(f.value, StopPropagation):
                raise f
            raise MiddlewareError('Unknown Middleware Control Flag: %s'
                                  % (f.value,))                
        
        d = maybeDeferred(handler, message, endpoint)
        d.addErrback(_handle_control_flag)
        return d

    def process_control_flag(self, f):
        f.trap(StopPropagation)
        if isinstance(f.value, StopPropagation):
            return None

    def apply_consume(self, handler_name, message, endpoint, from_middleware=None):
        return self._handle(
            self.middlewares, handler_name, message, endpoint, from_middleware)
   
    def apply_publish(self, handler_name, message, endpoint, from_middleware=None):
        return self._handle(
            reversed(self.middlewares), handler_name, message, endpoint, from_middleware)