import re, sys, traceback
from xml.etree import ElementTree

from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full, normalize_msisdn
from vumi import log

class MovilgateHttpTransport(Transport):
    
    def mkres(self, cls, publish_func, path_key = None):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        if path_key is None:
            path = self.config['receive_path']
        else:
            path = "%s/%s" % (self.config['receive_path'], path_key)
        return (resource, path)
        
    @inlineCallbacks
    def setup_transport(self):
        self._resources = []
        log.msg("Setup mobivate transport %s" % self.config)
        resources = [
            self.mkres(MovilgateReceiveSMSResource,
                       self.publish_message)
        ]
        self.receipt_resource = yield self.start_web_resources(
            resources, self.config['receive_port'])

    def stopWorker(self):
        if hasattr(self, 'receipt_resource'):
            return self.receipt_resource.stopListening()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message to be processed %s" % repr(message))
        try:
            movilgate_parser = MovilgateXMLParser()
            mobilgate_msg = movilgate_parser.build({
            'proveedor': {
                'id': self.config['proveedor_id'],
                'password': self.config['proveedor_password']},
            'servicio': {'id': self.config['servicio_id']},
            'telephono': {
                'msisdn': message['to_addr'],
                'id_tran': '123'},
            'contenido': message['content']})
            
            response = yield http_request_full(
                self.config['url'],
                mobilgate_msg,
                {'User-Agent': ['Vusion Movilgate Transport'],
                 'Content-Type': ['application/xml; charset=UTF-8']})
            
            if response.code != 200:
                log.msg("Http Error %s: %s"
                        % (response.code, response.delivered_body))
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='http',
                    failure_code=response.code,
                    failure_reason=response.delivered_body)
                return
            
            resp = ElementTree.fromstring(response.delivered_body)
            status = resp.find('Transaccion').attrib['estado']
            if status != "0":
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='service',
                    failure_code=status,
                    failure_reason=resp.find('Texto').text)
                return
            
            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'])
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
            yield self.publish_delivery_report(
                user_message_id=message['message_id'],
                delivery_status='failed',
                failure_level='internal',
                failure_code=0,
                failure_reason=traceback.format_exc())


class MovilgateReceiveSMSResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_func):
        log.msg("Init ReceiveSMSResource %s" % (config))
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']

    @inlineCallbacks
    def do_render(self, request):
        try:
            raw_body = request.content.read()
            log.msg('got hit with %s' % raw_body)
            mo_request = ElementTree.fromstring(raw_body)
            contenido = mo_request.find('Contenido').text
            from_add = mo_request.find('Telefono').attrib['msisdn']
            yield self.publish_func(
                transport_type='sms',
                to_addr=self.config['default_origin'],
                from_addr=from_add,
                content=contenido,
                transport_metadata={})            
            request.setResponseCode(http.OK)
            request.setHeader('Content-Type', 'text/plain')
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.msg("Error processing the request: %s" % (request,))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET


class MovilgateXMLParser():

    def build(self, messagedict):
        messages = ElementTree.Element('MTRequest')
        proveedor = ElementTree.SubElement(messages, 'Proveedor')
        proveedor.set('Id', messagedict['proveedor']['id'])
        proveedor.set('Password', messagedict['proveedor']['password'])
        servicio = ElementTree.SubElement(messages, 'Servicio')
        servicio.set('Id', messagedict['servicio']['id'])
        servicio.set('ContentType', "0")
        servicio.set('CreateSession', "0")
        telephono = ElementTree.SubElement(messages, 'Telefono')
        telephono.set('msisdn', messagedict['telephono']['msisdn'])
        telephono.set('IdTran', messagedict['telephono']['id_tran'])
        contenido = ElementTree.SubElement(messages, 'Contenido')
        contenido.text = messagedict['contenido']
        return ElementTree.tostring(messages)
