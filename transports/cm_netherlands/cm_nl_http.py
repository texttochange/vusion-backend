import re, sys, traceback
from xml.etree import ElementTree
from urllib import urlencode, unquote
from urlparse import parse_qs

from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet import defer

from vumi.transports.base import Transport
from vumi.transports.httprpc import HttpRpcTransport
from vumi.utils import http_request_full, normalize_msisdn
from vumi import log

##This transport is supposed to send and receive sms in 2 different ways.
##For MT, it uses the CM API POST
##For MO, it uses the CM API GET
class CmHttpTransport(Transport):

    transport_type = 'sms'

    def mkres(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        return (resource, self.config['receive_path'])

    @inlineCallbacks
    def setup_transport(self):
        log.msg("Setup CM transport %s" % self.config)        
        super(CmHttpTransport, self).setup_transport()
        self._resources = []
        self.cmparser = CMXMLParser(self.config)
        resources = [
            self.mkres(ReceiveSMSResource,
                       self.publish_message,
                       self.config['receive_path'])
        ]
        self.receipt_resource = yield self.start_web_resources(
            resources, self.config['receive_port'])

    def teardown_transport(self):
        log.msg("Stop CM Transport")
        if hasattr(self, 'receipt_resource'):
            return self.receipt_resource.stopListening()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message to be processed %s" % repr(message))
        try:
            response = yield http_request_full(
                self.config['url'],
                self.cmparser.build({
                    'customer_id': self.config['customer_id'],
                    'login': self.config['login'],
                    'password': self.config['password'],
                    'from_addr': message['from_addr'],
                    'to_addr': message['to_addr'],
                    'content': message['content']}),
                {'User-Agent': ['Vumi CM YO Transport'],
                 'Content-Type': ['application/json;charset=UTF-8'], },
                'POST')

            if response.code == http.OK:
                if response.delivered_body in [None, '']:
                    yield self.publish_ack(message['message_id'],
                                           message['message_id'])
                else:
                    reason = "SERVICE ERROR - %s" % (response.delivered_body)
                    log.error(reason)
                    yield self.publish_nack(message['message_id'], reason)
            else:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
                log.error(reason)
                yield self.publish_nack(message['message_id'], reason)

        except Exception as ex:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "TRANSPORT ERROR: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
            reason = "TRANSPORT ERROR %s" % (ex.message)
            yield self.publish_nack(message['message_id'], reason)            


class ReceiveSMSResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_func):
        log.msg("Init ReceiveSMSResource %s" % (config))
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']

    def phone_format_from_cm(self, phone):
        regex = re.compile('^00')
        return re.sub(regex, '+', phone)

    @inlineCallbacks
    def do_render(self, request):
        log.msg('got hit with %s' % request.args)
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        try:
            yield self.publish_func(
                transport_name=self.transport_name,
                transport_type='sms',
                to_addr=request.args['originator'][0],
                from_addr=self.phone_format_from_cm(request.args['recipient'][0]),
                content=request.args['message'][0],
                transport_metadata={})
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.msg("Error processing the request: %s" % (request,))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET


class CMXMLParser():

    def __init__(self, config):
        self.minimum_number_of_message_part = config.get('minimum_number_of_message_part', '1')
        self.maximum_number_of_message_part = config.get('maximum_number_of_message_part', '3')

    def build(self, messagedict):
        messages = ElementTree.Element('MESSAGES')
        customer = ElementTree.SubElement(messages, 'CUSTOMER')
        customer.set('ID', messagedict['customer_id'])
        user = ElementTree.SubElement(messages, 'USER')
        user.set('LOGIN', messagedict['login'])
        user.set('PASSWORD', messagedict['password'])
        msg = ElementTree.SubElement(messages, 'MSG')
        origin = ElementTree.SubElement(msg, 'FROM')
        origin.text = messagedict['from_addr']
        body = ElementTree.SubElement(msg, 'BODY')
        body.set('HEADER', '')        
        body.set('TYPE', 'TEXT')
        body.text = messagedict['content']
        to = ElementTree.SubElement(msg, 'TO')
        to.text = messagedict['to_addr']
        minimum_number = ElementTree.SubElement(msg, 'MINIMUMNUMBEROFMESSAGEPARTS')
        minimum_number.text = self.minimum_number_of_message_part
        maximum_number = ElementTree.SubElement(msg, 'MAXIMUMNUMBEROFMESSAGEPARTS')
        maximum_number.text = self.maximum_number_of_message_part

        return ElementTree.tostring(messages)
