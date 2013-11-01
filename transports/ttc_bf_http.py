import sys
import traceback

from urllib import urlencode

from twisted.internet.defer import inlineCallbacks
from twisted.web.resource import Resource
from twisted.web import http, server

from vumi.transports import Transport
from vumi import log
from vumi.utils import http_request_full


class TtcBfHttpTransport(Transport):
    
    def mk_resource_worker(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self.resources.append(resource)
        return (resource, self.config['receive_path'])

    @inlineCallbacks
    def setup_transport(self):
        self.resources = []
        log.msg("Setup TTC Burkina transport %s" %self.config)
        resources = [
            self.mk_resource_worker(
                TtcBfMoResource,
                self.publish_message,
                self.config['receive_path'])]
        self.resources = yield self.start_web_resources(
            resources, self.config['receive_port'])

    def stopWorker(self):
        log.msg("Stop TTC Burkina transport")
        if hasattr(self, 'resources'):
            return self.resources.stopListening()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message %s" % message)
        try:
            params = {
                'from_addr': self.config['default_shortcode'],
                'to_addr': message['to_addr'],
                'message': message['content'].encode('utf-8')}
            log.msg('Hitting %s as GET with %r' % (self.config['send_url'], urlencode(params)))
            
            response = yield http_request_full(
                "%s?%s" %(self.config['send_url'], urlencode(params)),
                None,
                {'User-Agent': ['Vumi TTC Burkina Transport']},
                'GET')
            
            if response.code == 200:
                yield self.publish_ack(
                    user_message_id=message['message_id'],
                    sent_message_id=message['message_id'])
                return
                
            response_body = response.delivered_body                
            if  response_body in [None, '']:
                log.err("HTTP TTC Burkina Error %s: %s" % (response.code, response_body))
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='http',
                    failure_code=str(response.code),
                    failure_reason=response_body)
                return

            log.err("TTC Burkina Error %s: %s" % (response.code, response_body))
            yield self.publish_delivery_report(
                user_message_id=message['message_id'],
                delivery_status='failed',
                failure_level='service',
                failure_code=str(response.code),
                failure_reason=response_body)

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.err("Unexpected error %r" % traceback.format_exception(exc_type, exc_value, exc_traceback))


class TtcBfMoResource(Resource):
    isLeaf = True
    
    def __init__(self, config, publish_func):
        log.msg("Init Mo Resource %s" % config)
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']
        self.shortcode = self.config['default_shortcode']

    @inlineCallbacks
    def do_render(self, request):
        log.msg("Got hit with %s" % request.args)
        try:
            yield self.publish_func(
                transport_name=self.transport_name,
                transport_type='sms',
                to_addr=self.shortcode,
                from_addr=request.args['from_addr'][0],
                content=request.args['message'][0])
            request.setResponseCode(http.OK)
            request.write('')
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            headers = dict(request.requestHeaders.getAllRawHeaders())
            log.msg("Error processing the request: %s with headers %s" % (request.args, repr(headers)))
            request.write(e.message)
        request.finish()

    def render(self, request):
        self.do_render(request)
        return server.NOT_DONE_YET
