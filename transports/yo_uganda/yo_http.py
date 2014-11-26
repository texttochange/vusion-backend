# -*- test-case-name: tests.test_yo_ug_http -*-

import sys
import traceback
from urllib import urlencode, unquote
from urlparse import parse_qs
import re

from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full, normalize_msisdn
from vumi import log


class YoHttpTransport(Transport):

    tranport_type = 'sms'
    
    def mkres(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        return (resource, self.config['receive_path'])

    @inlineCallbacks
    def setup_transport(self):
        log.msg("Setup yo transport %s" % self.config)
        super(YoHttpTransport, self).setup_transport()
        self._resources = []
        resources = [self.mkres(YoReceiveSMSResource,
                                self.publish_message,
                                self.config['receive_path'])]
        self.web_resources = yield self.start_web_resources(
            resources, self.config['receive_port'])

    def teardown_transport(self):
        log.msg("STOP YO Transport")
        if hasattr(self, 'web_resources'):
            return self.web_resources.stopListening()
        
    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message %s" % repr(message))
        try:
            origin = (self.config['default_origin'] if not "customized_id" in message['transport_metadata'] else message['transport_metadata']['customized_id'])
            params = {
                'ybsacctno': self.config['ybsacctno'],
                'password': self.config['password'],
                'origin': origin,
                'sms_content': message['content'].encode('utf-8'),
                'destinations': message['to_addr'],
            }
            log.msg('Hitting %s with %s' % (self.config['url'], urlencode(params)))

            response = yield http_request_full(
                "%s?%s" % (self.config['url'], urlencode(params)),
                "",
                {'User-Agent': ['Vumi Yo Transport'],
                 'Content-Type': ['application/json;charset=UTF-8'], },
                'GET')

            if response.code != 200:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
                yield self.publish_nack(message['message_id'], reason)
                return

            response_attr = parse_qs(unquote(response.delivered_body))
            [ybs_status] = response_attr['ybs_autocreate_status']
            ybs_msg = response_attr['ybs_autocreate_message'][0] if 'ybs_autocreate_message' in response_attr else None
            if (ybs_status == 'ERROR'):
                reason = "SERVICE ERROR %s - %s" % (ybs_status, ybs_msg)
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
            pass



class YoReceiveSMSResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_func):
        log.msg("Init ReceiveSMSResource %s" % (config))
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']

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
                from_addr=request.args['sender'][0],
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
