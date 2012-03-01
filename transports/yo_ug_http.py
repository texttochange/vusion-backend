# -*- test-case-name: tests.test_yo_ug_http -*-

from urllib import urlencode

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full, normalize_msisdn
from twisted.internet import defer
defer.setDebugging(True)

class YoUgHttpTransport(Transport):

    def mkres(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        return (resource, self.config['receive_path'])

    
    @inlineCallbacks
    def setup_transport(self):
        self._resources = [];
        log.msg("Setup yo transport %s" % self.config)
        #self.config.setdefault('receive_path', '/yo')
        resources = [
            self.mkres(ReceiveSMSResource, self.publish_message, '/receive')
            ]
        self.receipt_resource = yield self.start_web_resources(
            resources, self.config['receive_port'])
        
        
    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message to be processed %s" % repr(message))
        params = {
            'ybsacctno': self.config['ybsacctno'],
            'password': self.config['password'],
            'origin': message['from_addr'],
            'sms_content': message['content'],
            'destinations': message['to_addr'],
        }
        log.msg('Hitting %s with %s' % (self.config['url'], urlencode(params)))
        try:
            response = yield http_request_full(
                "%s?%s" % (self.config['url'], urlencode(params)), 
                "",
                {'User-Agent': ['Vumi Yo Transport'],
                 'Content-Type': ['application/json;charset=UTF-8'],},
                'GET')
        except ConnectionRefusedError:
            log.msg("Connection failed sending message: %s" % message)
            #raise TemporaryFailure('connection refused')
        except Exception as ex:
            log.msg("Unexpected error %s" % repr(ex))
                
        if response.code != 200:
            log.msg("Http Error %s: %s" % (response.code, response.delivered_body))
            yield self.publish_delivery_report(
                user_message_id = message['message_id'],
                delivery_status = 'failed',
                failure_level = 'http',
                failure_code = response.code,
                failure_reason = response.delivered_body
                )
            return

        ybs_autocreate_status = response.delivered_body.split("&")[0].split('=')[1]
        if (ybs_autocreate_status=='ERROR'):
            log.msg("Yo Error %s: %s" % (response.code, response.delivered_body))
            yield self.publish_delivery_report(
                user_message_id = message['message_id'],
                delivery_status = 'failed',
                failure_level = 'service',
                failure_code = ybs_autocreate_status,
                failure_reason = response.delivered_body.split("&")[1].split('=')[1]
                )
            return

        log.msg("Sms received and accepted by Yo %s" % response.delivered_body)
        yield self.publish_ack(
            user_message_id=message['message_id'],
            sent_message_id="abc",
        )
    
    def stopWorker(self):
        log.msg("stop yo transport")
        if hasattr(self, 'receipt_resource'):
            return self.receipt_resource.stopListening()


class ReceiveSMSResource(Resource):
    isLeaf = True
    
    def __init__(self, config, publish_func):
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']
    
    @inlineCallbacks
    def do_render(self, request):
        log.msg('got hit with %s' % request.args)
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        yield self.publish_func(
             transport_name = self.transport_name,
                transport_type = 'sms',
                message_id = 'abc',
                to_addr = request.args['code'][0],
                from_addr = request.args['sender'][0],
                content = request.args['message'][0],
                transport_metadata = {}
        )
        request.finish()
        
    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET