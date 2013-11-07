import urllib
import re
import sys
import traceback

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import http, server
from twisted.web.resource import Resource

from vumi.transports.base import Transport
from vumi import log
from vumi.utils import http_request_full


class MobtechMlHttpTransport(Transport):
    
    def make_resource_worker(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self.resources.append(resource)
        path = '%s/%s' % (self.config['receive_path'], path_key)
        return (resource, path)

    def mk_delivery_url(self, message):
        return 'http://%s:%s/%s/%s?messageid=%s&%s' % (self.config['domain'], 
                                                    self.config['receive_port'], 
                                                    self.config['receive_path'], 
                                                    self.config['delivery_receive_path'], 
                                                    message['message_id'],
                                                    self.config['delivery_url_params'])

    @inlineCallbacks
    def setup_transport(self):
        self.resources = []
        log.msg("Setup MobTech transport %s" %self.config)
        self.mt_response_regex = re.compile(self.config['mt_response_regex'])
        resources = [
            self.make_resource_worker(
                MobtechMlMoResource,
                self.publish_message,
                self.config['mo_receive_path']),
            self.make_resource_worker(
                MobtechMlDeliveryResource,
                self.publish_delivery_report,
                self.config['delivery_receive_path'])]
        self.resources = yield self.start_web_resources(
            resources, self.config['receive_port'])

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message %s" % repr(message))
        try:
            params = {
                'username': self.config['username'],
                'password': self.config['password'],
                'from': message['from_addr'],
                'to': message['to_addr'],
                'text': message['content'],
                'dlr-url': self.mk_delivery_url(message)}
            log.msg('Hitting %s as POST with %r' % (self.config['url'], repr(params)))
            
            response = yield http_request_full(
                self.config['url'],
                urllib.urlencode(params),
                {'User-Agent': ['Vumi Mobivate Transport'],
                 'Content-Type': ['application/x-www-form-urlencoded']},
                'POST')
            
            if response.code != 200:
                log.err("HTTP Error %s: %s" % (response.code, response.delivered_body))
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='http',
                    failure_code=str(response.code),
                    failure_reason=response.delivered_body)
                return

            match = re.match(self.mt_response_regex, response.delivered_body)
            response_body = match.groupdict()
            if (response_body['status'] != "0"):
                keys = response_body.keys()
                log.err("Mobtech Error %s: %s" % (response_body['status'], response_body['message']))
                yield self.publish_delivery_report(
                    user_message_id=message['message_id'],
                    delivery_status='failed',
                    failure_level='service',
                    failure_code=response_body['status'],
                    failure_reason=response_body['message'])
                return

            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'])
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.err("Unexpected error %r" % traceback.format_exception(exc_type, exc_value, exc_traceback))
            yield self.publish_delivery_report(
                user_message_id=message['message_id'],
                delivery_status='failed',
                failure_level='transport',
                failure_code='',
                failure_reason=response.delivered_body)

    def stopWorker(self):
        log.msg("Stop Mobtech transport")
        if hasattr(self, 'resources'):
            return self.resources.stopListening()


class MobtechMlMoResource(Resource):
    isLeaf = True
    
    def __init__(self, config, publish_func):
        log.msg("Init Mo Resource %s" % config)
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']

    @inlineCallbacks
    def do_render(self, request):
        log.msg("Got hit with %s" % request.args)
        try:
            yield self.publish_func(
                transport_name=self.transport_name,
                transport_type='sms',
                to_addr=request.args['to'][0],
                from_addr=request.args['from'][0],
                content=request.args['text'][0])
            request.setResponseCode(http.OK)
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            headers = dict(request.requestHeaders.getAllRawHeaders())
            log.msg("Error processing the request: %s with headers %s" % (request.args, repr(headers)))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return server.NOT_DONE_YET


class MobtechMlDeliveryResource(Resource):
    isLeaf = True
    
    def __init__(self, config, publish_func):
        log.msg("Init Delivery Resource %s" % config)
        self.config = config
        self.publish_func = publish_func
        self.delivery_regex = re.compile(self.config['delivery_regex'])
        self.stat_regex = re.compile(self.config['stat_regex'])

    @inlineCallbacks
    def do_render(self, request):
        log.msg("Got hit with %s" % request.args)
        try:
            raw_delivery = request.content.read()
            delivery_match = re.search(self.delivery_regex, raw_delivery)
            if delivery_match is None:
                log.err("Fail to read delivery report %s" % raw_delivery)
                return
            delivery_report = delivery_match.groupdict()
            if int(delivery_report['dlvrd']) >= 1:
                yield self.publish_func(
                    user_message_id=request.args['messageid'][0],
                    delivery_status='delivered')
            else:
                stat_match = re.search(self.stat_regex, raw_delivery)
                stat_report = 'XX'
                if stat_match is not None:
                    stat_report = stat_match.groupdict()
                    stat_report = stat_report['stat']
                yield self.publish_func(
                    user_message_id=request.args['messageid'][0],
                    delivery_status='failed',
                    failure_level='service',
                    failure_code='XX',
                    failure_reason= stat_report)
            request.setResponseCode(http.OK)        
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)            
            headers = dict(request.requestHeaders.getAllRawHeaders())
            log.msg("Error processing the request: %s with headers %s" % (request.args, repr(headers)))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return server.NOT_DONE_YET
