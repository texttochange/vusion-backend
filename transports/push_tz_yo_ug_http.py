# -*- test-case-name: tests.test_yo_ug_http -*-

from urllib import urlencode, unquote
from urlparse import parse_qs

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.web.xmlrpc import Proxy

from vumi.transports.base import Transport
from vumi.utils import http_request_full, normalize_msisdn, SimplishReceiver
from twisted.internet import defer

from xml.etree import ElementTree

import re

##This transport is supposed to send and receive sms in 2 different ways.
##To send sms we use the PUSH TZ API (xmlrpc)
##To receive sms we use the YO Interface to forward the sms
class PushYoTransport(Transport):

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
            response = yield rpc_request_full(
                self.config['url'],
                'EAPIGateway.SendSMS',
                {'Service': int(self.config['service_id']),
                 'Password': self.config['password'],
                 'Channel': int(self.config['channel']),
                 'Receipt': self.config['receipt'],
                 'Numbers': message['to_addr'],
                 'Source': message['from_addr'],
                 'SMSText': message['content'],
                 'MaxSegments': int(self.config['max_segments'])})
            
            if not 'Identifier' in response:
                log.msg("Push Error: %s" % (response))
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='service',
                    failure_code=0,
                    failure_reason=response['Error']
                )
                return
            
            yield self.publish_delivery_report(
                user_message_id=message['message_id'],
                delivery_status='delivered',
                to_addr=message['to_addr'],
                transport_metadata=response,
            )
        except Exception as ex:
            log.msg("Unexpected error %s" % repr(ex))
            yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='http',
                    failure_code=0,
                    failure_reason=repr(ex)
                )

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

    def phone_format_from_yo(self, phone):
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
                    to_addr=request.args['code'][0],
                    from_addr=self.phone_format_from_yo(request.args['sender'][0]),
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


def rpc_request_full(url, method, data=None):
    proxy = Proxy(url)
    d = proxy.callRemote(method, data)

    def handle_response(response):
        return response

    def handle_err(failure):
        failure.trap(ValueError)
        return {'Error': 'Failure during xml parsing'}
    
    d.addCallback(handle_response)
    d.addErrback(handle_err)

    return d
