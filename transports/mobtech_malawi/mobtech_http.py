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
    
    transport_type = 'sms'
    
    def make_resource_worker(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        path = '%s/%s' % (self.config['receive_path'], path_key)
        return (resource, path)

    @inlineCallbacks
    def setup_transport(self):
        log.msg("Setup MobTech Transport %s" % self.config)
        super(MobtechMlHttpTransport, self).setup_transport()
        self._resources = []
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
        self.web_resource = yield self.start_web_resources(
            resources, self.config['receive_port'])

    def teardown_transport(self):
        log.msg("Stop Mobtech Transport")
        if hasattr(self, 'web_resource'):
            return self.web_resource.stopListening()

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
                'messageid': message['message_id']}
            log.msg('Hitting %s as POST with %r' % (self.config['url'], repr(params)))
            
            response = yield http_request_full(
                self.config['url'],
                urllib.urlencode(params),
                {'User-Agent': ['Vumi Mobivate Transport'],
                 'Content-Type': ['application/x-www-form-urlencoded']},
                'POST')
            
            if response.code != 200:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
                yield self.publish_nack(message['message_id'], reason)
                return
            
            match = re.match(self.mt_response_regex, response.delivered_body)
            response_body = match.groupdict()
            if (response_body['status'] != "0"):
                keys = response_body.keys()
                reason = "SERVICE ERROR %s - %s" % (response_body['status'], response_body['message'])
                #log.err("Mobtech Error %s: %s" % (response_body['status'], response_body['message']))
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
            request.write("OK")
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            headers = dict(request.requestHeaders.getAllRawHeaders())
            log.msg("Error processing the request: %s with headers %s" % (request.args, repr(headers)))
            request.write("NOT OK")
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
            raw_delivery = request.args['reply'][0]
            delivery_match = re.search(self.delivery_regex, raw_delivery)
            if delivery_match is None:
                log.err("Fail to read delivery report %s" % raw_delivery)
                raise Exception("No Delivery")
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
                if stat_report == 'DELIVRD':
                    yield self.publish_func(
                        user_message_id=request.args['messageid'][0],
                        delivery_status='delivered')
                else:
                    yield self.publish_func(
                        user_message_id=request.args['messageid'][0],
                        delivery_status='failed',
                        failure_level='service',
                        failure_code='XX',
                        failure_reason= stat_report)
            request.setResponseCode(http.OK)
            request.write("OK")
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)            
            headers = dict(request.requestHeaders.getAllRawHeaders())
            log.msg("Error processing the request: %s with headers %s" % (request.args, repr(headers)))
            request.write("NOT OK")
        request.finish()

    def render(self, request):
        self.do_render(request)
        return server.NOT_DONE_YET
