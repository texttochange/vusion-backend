from urllib import urlencode, unquote
from urlparse import parse_qs
import re, sys, traceback

from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full, normalize_msisdn
from vumi import log

class MobivateHttpTransport(Transport):
    
    tranport_type = 'sms'
    
    def mkres(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        return (resource, "%s/%s" % (self.config['receive_path'], path_key))
        
    @inlineCallbacks
    def setup_transport(self):
        log.msg("Setup mobivate transport %s" % self.config)
        super(MobivateHttpTransport, self).setup_transport()
        self._resources = []
        resources = [
            self.mkres(MobivateReceiveSMSResource,
                       self.publish_message,
                       "SMSfromMobiles"),
            self.mkres(MobivateReceiveReciept,
                       self.publish_delivery_report,
                       "DeliveryReciept")
        ]
        self.receipt_resource = yield self.start_web_resources(
            resources, self.config['receive_port'])
 
    def teardown_transport(self):
        log.msg("Stop Mobivate Transport")
        if hasattr(self, 'receipt_resource'):
            return self.receipt_resource.stopListening() 

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message to be processed %s" % repr(message))
        try:
            params = {
                'USER_NAME': self.config['user_name'],
                'PASSWORD': self.config['password'],
                'ORIGINATOR': message['from_addr'],
                'MESSAGE_TEXT': message['content'],
                'RECIPIENT': message['to_addr'],
                'REFERENCE': message['message_id']
            }
            encoded_params = urlencode(params)
            log.msg('Hitting %s with %s' % (self.config['url'], encoded_params))

            response = yield http_request_full(
                "%s?%s" % (self.config['url'], encoded_params),
                headers={'User-Agent': ['Vumi Mobivate Transport'],
                         'Content-Type': ['application/json;charset=UTF-8'], },
                method='GET')

            if response.code == http.OK:
                response_content = response.delivered_body.split("\n")
                response_status = response_content[0]
                response_msg = response_content[1] if len(response_content) > 1 else ''
                if (response_status in ['0', '1']):
                    yield self.publish_ack(
                        user_message_id=message['message_id'],
                        sent_message_id=message['message_id'])
                else: 
                    reason = "SERVICE ERROR %s - %s" % (response_status, response_msg)
                    yield self.publish_nack(message['message_id'], reason)
            else:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
                yield self.publish_nack(message['message_id'], reason)
                
        except Exception as ex:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "TRANSPORT ERROR: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))            
            reason = "TRANSPORT ERROR %s" % (ex.message)
            yield self.publish_nack(message['message_id'], reason)
        
    
class MobivateReceiveSMSResource(Resource):
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
                to_addr=request.args['RECIPIENT'][0],
                from_addr=request.args['ORIGINATOR'][0],
                content=request.args['MESSAGE_TEXT'][0],
                transport_metadata={})
            request.write('0')
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.msg("Error processing the request: %s" % (request,))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET


class MobivateReceiveReciept(Resource):
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
                user_message_id=request.args['REFERENCE'][0],
                delivery_status='delivered')
            request.write('0')
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.msg("Error processing the request: %s" % (request,))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET
