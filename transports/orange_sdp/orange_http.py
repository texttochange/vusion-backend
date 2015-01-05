import sys
import traceback
import json
import pytz
import re
from hashlib import md5
from datetime import datetime

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full
from vumi import log

from vusion.utils import get_shortcode_value


class OrangeSdpHttpTransport(Transport):

    transport_type = 'sms'

    def mkres(self, cls, publish_mo_func, publish_dlr_func, path_key = None):
        resource = cls(self.config, publish_mo_func, publish_dlr_func)
        if path_key is None:
            path = self.config['receive_path']
        else:
            path = "%s/%s" % (self.config['receive_path'], path_key)
        return (resource, path)

    @inlineCallbacks
    def setup_transport(self):
        log.msg("Setup Orange SPD Transport %s" % self.config)
        ## Make sure there is not tailing / in the url
        self.config['url'] = re.sub('/$','', self.config['url'])
        super(OrangeSdpHttpTransport, self).setup_transport()
        resources = [
            self.mkres(OrangeSdpMoResource,
                       self.publish_message,
                       self.publish_delivery_report)
        ]
        self.web_resources = yield self.start_web_resources(
            resources, self.config['receive_port'])
        yield self.set_mo_callback()

    def teardown_transport(self):
        log.msg("Stop Orange SPD Transport")
        self.web_resources.stopListening()

    ## return only milliseconds precision
    def get_timestamp(self):
        return datetime.now().strftime('%Y%m%d%H%M%S%f')[:17]

    def get_auth_header(self):
        timestamp = self.get_timestamp()
        tmp = "%s%s%s" % (self.config['service_provider_id'],
                          timestamp,
                          self.config['password'])
        return ('spId="%s",'
                'spPassword="%s",'
                'timeStamp="%s",'
                'productId="%s"') % (
                    self.config['service_provider_id'],
                    md5(tmp).hexdigest(),
                    timestamp,
                    self.config['product_id'])

    def from_vusion_code_2_shortcode(self, code):
        return get_shortcode_value(code)

    def from_vusion_code_2_orangecode(self, code):
        return "+%s" % re.sub('-', '', code).encode('ascii')

    def get_req_content(self, request):
        try:
            response_body = json.loads(response.content.read())
        except:
            return None

    @inlineCallbacks
    def set_mo_callback(self):
        log.msg("Registering mo callback...")
        data = {
            'notifyURL': '%s:%s%s' % (
                self.config['receive_domain'],
                self.config['receive_port'],
                self.config['receive_path']),
            'clientCorrelator': 'ttc_mo'}
        response = yield http_request_full(
            "%s/1/smsmessaging/inbound/subscriptions" % self.config['url'],
            headers={
                'User-Agent': ['Vusion OrangeSpd Transport'],
                'Content-Type': ['application/json;charset=UTF-8'],
                'Authorization': ['AUTH %s' % self.get_auth_header()]},
            method='POST',
            data=json.dumps(data))
        if response.code != http.CREATED:
            response_body = self.get_req_content(response)
            reason = "TRANSPORT FAILD SET MO CALLBACK %s - %s" % (
                response.code, (response_body or ''))
            log.error(reason)
            self.teardown_transport()
        log.msg("Callback Mo Registered!")

    @inlineCallbacks
    def set_dlr_callback(self, phone_number):
        log.msg("Registering DLR callback...")
        data = {
            'notifyURL': '%s:%s%s' % (
                self.config['receive_domain'],
                self.config['receive_port'],
                self.config['receive_path']),
            'clientCorrelator': 'ttc_dlr_%s' % phone_number}
        response = yield http_request_full(
            "%s/1/smsmessaging/outbound/%s/subscriptions" % (
                self.config['url'], phone_number),
            headers={
                'User-Agent': ['Vusion OrangeSpd Transport'],
                'Content-Type': ['application/json;charset=UTF-8'],
                'Authorization': ['AUTH %s' % self.get_auth_header()]},
            method='POST',
            data=json.dumps(data))
        if response.code != http.CREATED:
            response_body = self.get_req_content(response)
            reason = "TRANSPORT FAILD SET DLR CALLBACK %s - %s" % (
                response.code, (response_body or ''))
            log.error(reason)
        else:
            log.msg("Callback DLR Registered for %s!" % phone_number)

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message %r" % message)
        try:
            data = {
                'address': ['tel:%s' % message['to_addr']], 
                'message': message['content'], 
                'clientCorrelator': message['message_id'], 
                'senderName': self.from_vusion_code_2_shortcode(
                    message['from_addr'])}
            
            response = yield http_request_full(
                "%s/1/smsmessaging/outbound/tel:%s/requests" % (
                    self.config['url'],
                    self.from_vusion_code_2_orangecode(message['from_addr'])),
                headers={
                    'User-Agent': ['Vusion OrangeSpd Transport'],
                    'Content-Type': ['application/json;charset=UTF-8'],
                    'Authorization': ['AUTH %s' % self.get_auth_header()]},
                method='POST',
                data=json.dumps(data))
            
            if response.code != http.CREATED:
                reason = "HTTP/SERVICE ERROR %s - %s" % (
                    response.code, response.delivered_body)
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


class OrangeSdpMoResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_mo_func, publish_dlr_func):
        log.msg("Init OrangeSpdReveiveSmsResource")
        self.config = config
        self.publish_mo_func = publish_mo_func
        self.publish_dlr_func = publish_dlr_func
        self.transport_name = self.config['transport_name']

    def from_mo_data_2_from_addr(self, data):
        from_addr = data['inboundSMSMessageNotification']['inboundSMSMessage']['senderAddress']
        return re.sub(r'tel:', '', from_addr)

    def from_mo_data_2_to_addr(self, data):
        to_addr = data['inboundSMSMessageNotification']['inboundSMSMessage']['destinationAddress']
        return re.sub(r'\+', '', to_addr)

    def from_mo_data_2_content(self, data):
        return data['inboundSMSMessageNotification']['inboundSMSMessage']['message']

    def from_dlr_data_2_callback_data(self, data):
        return data['deliveryInfoNotification']['callbackData']

    @inlineCallbacks
    def do_render(self, request):
        try:
            raw_body = request.content.read()
            log.msg('got hit with %s' % raw_body)
            data = json.loads(raw_body)
            if 'inboundSMSMessageNotification' in data:
                yield self.publish_mo_func(
                    transport_name=self.transport_name,
                    transport_type='sms',
                    to_addr=self.from_mo_data_2_to_addr(data),
                    from_addr=self.from_mo_data_2_from_addr(data),
                    content=self.from_mo_data_2_content(data))
            elif 'deliveryInfoNotification' in data:
                yield self.publish_dlr_func(
                    delivery_status='delivered',
                    user_messsage_id=self.from_dlr_data_2_callback_data(data))
            else:
                pass
            request.setResponseCode(http.OK)
        except Exception as ex:
            reason = "Error processing the request: %s" % (ex.message,)
            log.msg(reason)
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET
