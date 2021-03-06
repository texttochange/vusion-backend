import re, sys, traceback
from xml.etree import ElementTree
from xml.etree.ElementTree import ParseError

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

    mandatory_metadata_fields = ['servicio_id', 'telefono_id_tran']
    tranport_type = 'sms'

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
        log.msg("Setup mobivate transport %s" % self.config)
        super(MovilgateHttpTransport, self).setup_transport()
        self._resources = []
        resources = [
            self.mkres(MovilgateReceiveSMSResource,
                       self.publish_message)
        ]
        self.receipt_resource = yield self.start_web_resources(
            resources, self.config['receive_port'])
        self.movilgate_parser = MovilgateXMLParser()

    def teardown_transport(self):
        log.msg("Stop Movilgate Transport")
        if hasattr(self, 'receipt_resource'):
            return self.receipt_resource.stopListening()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message to be processed %s" % repr(message))
        try:
            servicio_id = message['transport_metadata'].get('servicio_id', "")
            if servicio_id != "":
                id_tran = message['transport_metadata'].get('telefono_id_tran', "0")
            else:
                id_tran = str(hash(message['to_addr']) % ((sys.maxsize + 1) * 2))
            mobilgate_msg = self.movilgate_parser.build({
            'proveedor': {
                'id': self.config['proveedor_id'],
                'password': self.config['proveedor_password']},
            'servicio': {
                'id': servicio_id,
                'short_number': message['from_addr']},
            'telephono': {
                'msisdn': message['to_addr'],
                'id_tran': id_tran},
            'contenido': message['content']})
            
            response = yield http_request_full(
                self.config['url'],
                mobilgate_msg,
                {'User-Agent': ['Vusion Movilgate Transport'],
                 'Content-Type': ['text/xml; charset=UTF-8']},
                method='POST')
                        
            if response.code != http.OK:
                if response.code == http.INTERNAL_SERVER_ERROR:
                    headers = dict(response.headers.getAllRawHeaders())
                    [reason_msg] = headers.get('X-Movilgate-Status', '')
                    reason = "SERVICE ERROR %s - %s" % (response.code, reason_msg)
                    yield self.publish_nack(message['message_id'], reason) 
                else:
                    reason = "HTTP ERROR %s - %s" % (
                        response.code, response.delivered_body)
                    yield self.publish_nack(message['message_id'], reason)
                log.msg("%s" % reason)
                return
            
            resp = ElementTree.fromstring(response.delivered_body)
            status = resp.find('Transaccion').attrib['estado']
            if status != "0":
                reason = "SERVICE ERROR %s - %s" % (
                    status, resp.find('Texto').text)
                log.error(reason)
                yield self.publish_nack(message['message_id'], reason)
                return
            
            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'])
        except Exception as ex:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "TRANSPORT ERROR: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
            reason = "TRANSPORT ERROR %s" % (ex.message)
            yield self.publish_nack(message['message_id'], reason)


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
            if raw_body in [None, "", "\n"]:
                #it's a ping from movilgate
                log.msg('PING from Movilgate')
                request.setResponseCode(http.OK)
                request.finish()
                return
            mo_request = ElementTree.fromstring(raw_body)
            contenido = mo_request.find('Contenido').text
            servicio_id = mo_request.find('Servicio').attrib['Id']
            from_add = mo_request.find('Telefono').attrib['msisdn']
            id_tran = mo_request.find('Telefono').attrib['IdTran']
            to_addr = servicio_id.split('.')[0]
            yield self.publish_func(
                transport_type='sms',
                to_addr=to_addr,
                from_addr=from_add,
                content=contenido,
                transport_metadata={
                    'telefono_id_tran': id_tran,
                    'servicio_id': servicio_id})
            request.setResponseCode(http.OK)
            request.setHeader('Content-Type', 'text/plain')
        except ParseError as ex:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.msg("Error parsing the request body %s" % raw_body)
        except:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.msg("Error processing the request: %s" % (request,))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
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
        if (messagedict['servicio']['id'] == ""):
            servicio.set('ShortNumber', messagedict['servicio']['short_number'])
        telephono = ElementTree.SubElement(messages, 'Telefono')
        telephono.set('msisdn', messagedict['telephono']['msisdn'])
        telephono.set('IdTran', messagedict['telephono']['id_tran'])
        contenido = ElementTree.SubElement(messages, 'Contenido')
        contenido.text = messagedict['contenido']
        return ElementTree.tostring(messages)
