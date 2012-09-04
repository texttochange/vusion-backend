# -*- test-case-name: tests.test_yo_ug_http -*-

from urllib import urlencode, unquote
from urlparse import parse_qs
from xml.etree import ElementTree
import re

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.internet import defer
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full, normalize_msisdn

from vusion.utils import get_now_timestamp

##This transport is supposed to send and receive sms in 2 different ways.
##To send sms we use the CM API
##To receive sms we use the YO Interface to forward the sms
class PushTransport(Transport):

    def mkres(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        return (resource, self.config['receive_path'])

    @inlineCallbacks
    def setup_transport(self):
        self._resources = []
        log.msg("Setup yo transport %s" % self.config)
        resources = [
            self.mkres(PushReceiveSMSResource,
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
            push_parser = PushXMLParser()
            response = yield http_request_full(
                self.config['url'],
                push_parser.build_bulk_request({
                    'login': self.config['login'],
                    'password': self.config['password'],
                    'ref-id': get_now_timestamp(),
                    'delivery-notification-requested': self.config['delivery-notification-requested'],
                    'messages': [{
                        'id': message['message_id'],
                        'service-number': message['from_addr'],
                        'msisdn': message['to_addr'],
                        'content': message['content'],
                        'validity-period': self.config['validity-period'],
                        'priority': self.config['validity-period'],
                        }]
                    }),
                {'User-Agent': ['Vusion Push Transport'],
                 'Content-Type': ['application/xml;charset=UTF-8'], })
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


class PushReceiveSMSResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_func):
        log.msg("Init ReceiveSMSResource %s" % (config))
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']

    def phone_format_from_push(self, phone):
        regex = re.compile('^00')
        phone = re.sub(regex, '', phone)
        return ('+%s' % phone)

    @inlineCallbacks
    def do_render(self, request):
        log.msg('got hit with %s' % request.args)
        try:
            request.setResponseCode(http.OK)
            request.setHeader('Content-Type', 'text/plain')
            content = ElementTree.fromstring(request.content.read())
            message = content.find('message')
            yield self.publish_func(
                transport_name=self.transport_name,
                transport_type='sms',
                to_addr=message.attrib['service-number'],
                from_addr=self.phone_format_from_push(message.attrib['msisdn']),
                content=message.find('content').text,
                transport_metadata={}
            )
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.msg("Error processing the request: %s" % (request,))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET


class PushXMLParser():

    def build_bulk_request(self, message_dict):
        bulk_request = ElementTree.Element('bulk-request')
        bulk_request.set('login', message_dict['login'])
        bulk_request.set('password', message_dict['password'])
        bulk_request.set('ref-id', message_dict['ref-id'])
        bulk_request.set('delivery-notification-requested', message_dict['delivery-notification-requested'])
        bulk_request.set('version', '1.0')
        for message_dict in message_dict['messages']:
            message = ElementTree.SubElement(bulk_request, 'message')
            message.set('id', message_dict['id'])
            message.set('msisdn', message_dict['msisdn'])
            message.set('service-number', message_dict['service-number'])
            message.set('validity-period', message_dict['validity-period'])
            message.set('priority', message_dict['priority'])
            content = ElementTree.SubElement(message, 'content')
            content.set('type', 'text/plain')
            content.text = message_dict['content']
        return ElementTree.tostring(bulk_request)
