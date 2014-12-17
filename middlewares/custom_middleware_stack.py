from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred

from vumi.log import log
from vumi.message import TransportUserMessage
from vumi.transports import Transport
from vumi.middleware import MiddlewareStack, setup_middlewares_from_config
from vumi.connectors import ReceiveOutboundConnector

class MiddlewareControlFlag(Exception):
    pass


class StopPropagation(MiddlewareControlFlag):
    pass


class CustomMiddlewareStack(MiddlewareStack):

    def __init__(self, middlewares):
        super(CustomMiddlewareStack, self).__init__(middlewares)
        #transport._publish_message = self._transport_publish_message
        #transport.publish_message = self.transport_publish_message
    
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


class CustomReceiveOutboundConnector(ReceiveOutboundConnector):
    
    def __init__(self, worker, connector_name, prefetch_count=None,
                 middlewares=None):
        super(ReceiveOutboundConnector, self).__init__(
            worker, connector_name, prefetch_count, middlewares)
        self._middlewares = CustomMiddlewareStack(middlewares
                                                  if middlewares is not None else [])

    def _consume_message(self, mtype, msg, from_middleware=None):
        endpoint_name = msg.get_routing_endpoint()
        handler = self._endpoint_handlers[mtype].get(endpoint_name)
        if handler is None:
            handler = self._default_handlers.get(mtype)        
        
        d = self._middlewares.apply_consume(
            mtype, msg, self.name, from_middleware)
        d.addCallback(handler)
        d.addErrback(self._middlewares.process_control_flag)
        d.addErrback(self._ignore_message, msg)
        return d


def useCustomMiddleware(klass):
  
    def setup_ro_connector(self, connector_name, middleware=True):
        return self.setup_connector(
            CustomReceiveOutboundConnector, connector_name,
            middleware=middleware)
    klass.setup_ro_connector = setup_ro_connector
    
    #@inlineCallbacks
    #def setup_middleware(self):
        #middlewares = yield setup_middlewares_from_config(self, self.config)
        #self._middlewares = CustomMiddlewareStack(middlewares)
    #klass.setup_middleware = setup_middleware
  
    #def _process_message(self, message, from_middleware=None):
        #def _send_failure(f):
            #self.send_failure(message, f.value, f.getTraceback())
            #log.err(f)
            #if self.SUPPRESS_FAILURE_EXCEPTIONS:
                #return None
            #return f
        #d = self._middlewares.apply_consume(
            #"outbound", message, self.transport_name, from_middleware)
        #d.addCallback(self.handle_outbound_message)
        #d.addErrback(self._middlewares.process_control_flag)
        #d.addErrback(_send_failure)
        #return d
    #klass._process_message = _process_message    

    #def _publish_message(self, message, from_middleware=None):
        #d = self._middlewares.apply_publish(
            #"inbound", message, self.transport_name, from_middleware)
        #d.addCallback(self.message_publisher.publish_message)
        #d.addErrback(self._middlewares.process_control_flag)        
        #return d
    #klass._publish_message = _publish_message

    #def publish_message(self, **kw):
        #kw.setdefault('transport_name', self.transport_name)
        #kw.setdefault('transport_metadata', {})
        #msg = TransportUserMessage(**kw)
        #return self._publish_message(msg)
    #klass.publish_message = publish_message
    
    return klass
