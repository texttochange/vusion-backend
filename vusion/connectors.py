from twisted.internet.defer import gatherResults

from vumi.connectors import BaseConnector, ReceiveInboundConnector

from message import (
    WorkerControl, DispatcherControl, MultiWorkerControl, ExportWorkerControl)


class ReceiveWorkerControlConnector(ReceiveInboundConnector):
    
    def setup(self):
        super(ReceiveWorkerControlConnector, self).setup()
        control_d = self._setup_consumer(
            'control',
            WorkerControl,
            self.default_control_handler)
        return gatherResults([control_d])

    def default_control_handler(self, msg):
        log.warning("No control handler for %r: %r" % (self.name, msg))

    def set_control_handler(self, handler, endpoint_name=None):
        self._set_endpoint_handler('control', handler, endpoint_name)

    def set_default_control_handler(self, handler):
        self._set_default_endpoint_handler('control', handler)


class ReceiveMultiworkerControlConnector(BaseConnector):
    
    def setup(self):
        control_d = self._setup_consumer(
            'control',
            MultiWorkerControl,
            self.default_control_handler)
        return gatherResults([control_d])

    def default_control_handler(self, msg):
        log.warning("No control handler for %r: %r" % (self.name, msg))

    def set_control_handler(self, handler, endpoint_name=None):
        self._set_endpoint_handler('control', handler, endpoint_name)

    def set_default_control_handler(self, handler):
        self._set_default_endpoint_handler('control', handler)


class ReceiveDispatcherControlConnector(BaseConnector):
    
    def setup(self):
        control_d = self._setup_consumer(
            'control',
            ReceiveDispatcherControlConnector,
            self.default_control_handler)
        return gatherResults([control_d])

    def set_default_control_handler(self, handler):
        self._set_default_endpoint_handler('control', handler)


class SendControlConnector(BaseConnector):
    
    def setup(self):
        control_d = self._setup_publisher('control')
        return gatherResults([control_d])
    
    def publish_control(self, msg, endpoint_name=None):
        return self._publish_message('control', msg, endpoint_name)


class ReceiveExportWorkerControlConnector(BaseConnector):

    def setup(self):
        control_d = self._setup_consumer(
            'control',
            ExportWorkerControl,
            self.default_control_handler)
        return gatherResults([control_d])

    def default_control_handler(self, msg):
        log.warning("No control handler for %r: %r" % (self.name, msg))

    def set_control_handler(self, handler, endpoint_name=None):
        self._set_endpoint_handler('control', handler, endpoint_name)

    def set_default_control_handler(self, handler):
        self._set_default_endpoint_handler('control', handler)
