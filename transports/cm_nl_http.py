# -*- test-case-name: tests.test_yo_ug_http -*-

from urllib import urlencode, unquote
from urlparse import parse_qs

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full, normalize_msisdn
from twisted.internet import defer

from xml.etree import ElementTree

import re


##This transport is supposed to send and receive sms in 2 different ways.
##To send sms we use the CM API
##To receive sms we use the YO Interface to forward the sms
class CmTransport(Transport):

    def mkres(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        return (resource, self.config['receive_path'])

    @inlineCallbacks
    def setup_transport(self):
        self._resources = []
        log.msg("Setup yo transport %s" % self.config)
        resources = [
            self.mkres(ReceiveSMSResource,
                       self.publish_message,
                       self.config['receive_path'])
            ]
        self.receipt_resource = yield self.start_web_resources(
            resources,
            self.config['receive_port'])

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message to be processed %s" % repr(message))
        try:
            cmparser = CMXMLParser()
            response = yield http_request_full(
                self.config['url'],
                cmparser.build({
                    'customer_id': self.config['customer_id'],
                    'login': self.config['login'],
                    'password': self.config['password'],
                    'from_addr': message['from_addr'],
                    'to_addr': message['to_addr'],
                    'content': message['content']
                    }),
                {'User-Agent': ['Vumi CM YO Transport'],
                 'Content-Type': ['application/json;charset=UTF-8'], },
                'POST')
        except ConnectionRefusedError:
            log.msg("Connection failed sending message: %s" % message)
        except Exception as ex:
            log.msg("Unexpected error %s" % repr(ex))

        if response.code != 200:
            log.msg("Http Error %s: %s"
                    % (response.code, response.delivered_body))
            yield self.publish_delivery_report(
                user_message_id=message['message_id'],
                delivery_status='failed',
                failure_level='http',
                failure_code=response.code,
                failure_reason=response.delivered_body
                )
            return

        try:
            if response.delivered_body:
                log.msg("Cm Error: %s" % (response.delivered_body))
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='service',
                    failure_code=0,
                    failure_reason=response.delivered_body
                )
                return
            yield self.publish_delivery_report(
                user_message_id=message['message_id'],
                delivery_status='delivered',
                to_addr=message['to_addr']
            )
        except Exception as ex:
            log.msg("Unexpected error %s" % repr(ex))

    def stopWorker(self):
        log.msg("stop yo transport")
        if hasattr(self, 'receipt_resource'):
            return self.receipt_resource.stopListening()


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
                    transport_metadata={}
            )
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.msg("Error processing the request: %s" % (request,))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET


class CMXMLParser():

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
        body.set('TYPE', 'TEXT')
        body.set('HEADER', '')
        body.text = messagedict['content']
        to = ElementTree.SubElement(msg, 'TO')
        to.text = messagedict['to_addr']

        return ElementTree.tostring(messages)
