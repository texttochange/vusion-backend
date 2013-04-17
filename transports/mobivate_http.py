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

class MobivateHttpTransport(Transport):
    
    def mkres(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        return (resource, "%s/%s" % (self.config['receive_path'], path_key))
        
    @inlineCallbacks
    def setup_transport(self):
        self._resources = []
        log.msg("Setup mobivate transport %s" % self.config)
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

    def phone_format(self, phone):
            regex = re.compile('^\+')
            return re.sub(regex, '', phone)
 
    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message to be processed %s" % repr(message))
        try:
            params = {
                'USER_NAME': self.config['user_name'],
                'PASSWORD': self.config['password'],
                'ORIGINATOR': message['from_addr'],
                'MESSAGE_TEXT': message['content'],
                'RECIPIENT': self.phone_format(message['to_addr']),
                'REFERENCE': message['message_id']
            }
            log.msg('Hitting %s with %s' % (self.config['url'], urlencode(params)))

            response = yield http_request_full(
                "%s?%s" % (self.config['url'], urlencode(params)),
                "",
                {'User-Agent': ['Vumi Mobivate Transport'],
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

            response_content = response.delivered_body.split("\n")
            response_status = response_content[0]
            if (not response_status in ['0', '1']):
                log.msg("Mobivate Error %s: %s" %
                        (response_status, response_content[1]))
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='service',
                    failure_code=response_status,
                    failure_reason=response_content[1])
                return

            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'])
        except Exception as ex:
            #add Nack or failure
            log.msg("Unexpected error %s" % repr(ex))

    def stopWorker(self):
        log.msg("stop Mobivate transport")
        if hasattr(self, 'receipt_resource'):
            return self.receipt_resource.stopListening()
    
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
