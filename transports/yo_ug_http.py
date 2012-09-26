# -*- test-case-name: tests.test_yo_ug_http -*-

from urllib import urlencode, unquote
from urlparse import parse_qs
import re

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full, normalize_msisdn
from twisted.internet import defer
#defer.setDebugging(True)


class YoUgHttpTransport(Transport):

    def mkres(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        return (resource, self.config['receive_path'])

    def phone_format_to_yo(self, phone):
        regex = re.compile('^\+')
        return re.sub(regex, '', phone)

    @inlineCallbacks
    def setup_transport(self):
        self._resources = []
        log.msg("Setup yo transport %s" % self.config)
        resources = [self.mkres(YoReceiveSMSResource,
                                self.publish_message,
                                self.config['receive_path'])]
        self.receipt_resource = yield self.start_web_resources(
            resources, self.config['receive_port'])

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message to be processed %s" % repr(message))
        try:
            origin = (self.config['default_origin'] if not "customized_id" in message['transport_metadata'] else message['transport_metadata']['customized_id'])
            params = {
                'ybsacctno': self.config['ybsacctno'],
                'password': self.config['password'],
                'origin': origin, 
                'sms_content': message['content'],
                'destinations': self.phone_format_to_yo(message['to_addr']),
            }
            log.msg('Hitting %s with %s' % (self.config['url'], urlencode(params)))
            
            response = yield http_request_full(
                "%s?%s" % (self.config['url'], urlencode(params)),
                "",
                {'User-Agent': ['Vumi Yo Transport'],
                 'Content-Type': ['application/json;charset=UTF-8'], },
                'GET')
            
            if response.code != 200:
                log.msg("Http Error %s: %s"
                    % (response.code, response.delivered_body))
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    sent_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='http',
                    failure_code=response.code,
                    failure_reason=response.delivered_body)
                return

            response_attr = parse_qs(unquote(response.delivered_body))
            [ybs_status] = response_attr['ybs_autocreate_status']
            ybs_msg = response_attr['ybs_autocreate_message'][0] if 'ybs_autocreate_message' in response_attr else None
            if (ybs_status == 'ERROR'):
                log.msg("Yo Error %s: %s" % (response.code,
                                             response.delivered_body))
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    sent_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='service',
                    failure_code=ybs_status,
                    failure_reason=ybs_msg
                )
                return

            yield self.publish_delivery_report(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                delivery_status='delivered'
            )
        except Exception as ex:
            log.msg("Unexpected error %s" % repr(ex))

    def stopWorker(self):
        log.msg("stop yo transport")
        if hasattr(self, 'receipt_resource'):
            return self.receipt_resource.stopListening()


class YoReceiveSMSResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_func):
        log.msg("Init ReceiveSMSResource %s" % (config))
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']

    def phone_format_from_yo(self, phone):
        regex = re.compile('^[(00)(\+)]')
        regex_single = re.compile('^0')
        phone = re.sub(regex, '', phone)
        phone = re.sub(regex_single, '', phone)        
        return ('+%s' % phone)

    @inlineCallbacks
    def do_render(self, request):
        log.msg('got hit with %s' % request.args)
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        try:
            yield self.publish_func(
                transport_name=self.transport_name,
                transport_type='sms',
                to_addr=(request.args['code'][0] if request.args['code'][0]!='' else self.config['default_origin']),
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
