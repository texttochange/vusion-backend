# -*- test-case-name: tests.test_mobifone_vietnam_http -*-

import sys
import traceback
from urllib import urlencode, unquote
from urlparse import parse_qs, parse_qsl
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


class MobifoneHttpTransport(Transport):

    tranport_type = 'sms'
    
    def mkres(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        return (resource, self.config['receive_path'])

    @inlineCallbacks
    def setup_transport(self):
        log.msg("Setup mobifone transport %s" % self.config)
        super(MobifoneHttpTransport, self).setup_transport()
        self._resources = []
        resources = [self.mkres(MobifoneReceiveSMSResource,
                                self.publish_message,
                                self.config['receive_path'])]
        self.web_resources = yield self.start_web_resources(
            resources, self.config['receive_port'])

    def teardown_transport(self):
        log.msg("STOP MOBIFONE Transport")
        if hasattr(self, 'web_resources'):
            return self.web_resources.stopListening()
        
    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message %s" % repr(message))
        try:
            params_credentials = {
                'msg_name': 'login',
                'account': self.config['mobiaccount'],
                'password': self.config['mobipassword'],
            }
            
            response_login = yield http_request_full(
                "%s?%s" % (self.config['outbound_url'], urlencode(params_credentials)),
                headers={'Content-Type': 'application/json'},
                method='POST') 
            
            log.msg("Response Login: (%s) %r" %
                    (response_login.code, response_login.delivered_body))        
        
            response_login_body = dict(parse_qsl(response_login.delivered_body))               
            
            params_send_sms = {}
            if any(response_login_body) == True:
                params_send_sms = {
                    'msg_name': 'send_sms',
                    'session_id': response_login_body['session_id'],
                    'msisdn': message['to_addr'],
                    'sms_template_code': '127701',
                    'param1': message['content'],
                    }
        
            response =yield http_request_full(
                "%s?%s" % (self.config['outbound_url'], urlencode(params_send_sms)),
                headers={'Content-Type': 'application/json'},
                method='POST')            
            
            log.msg("Response: (%s) %r" %
                    (response.code, response.delivered_body))

            if response.code != http.OK:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
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



class MobifoneReceiveSMSResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_func):
        log.msg("Init inbound ReceiveSMSResource %s" % (config))
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
                to_addr=self.config['default_origin'],
                from_addr=request.args['phone'][0],
                content=request.args['text'][0],
                transport_metadata={}
            )
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.msg("Error processing the request: %s" % (request,))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET
