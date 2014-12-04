import sys
import traceback
import re
from xml.etree import ElementTree
from xml.etree.ElementTree import ParseError

from urllib import urlencode

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full
from vumi import log


class MahindraConvivaHttpTransport(Transport):

    transport_type = 'sms'

    def mkres(self, cls, publish_func, path_key = None):
        resource = cls(self.config, publish_func)
        if path_key is None:
            path = self.config['receive_path']
        else:
            path = "%s/%s" % (self.config['receive_path'], path_key)
        return (resource, path)

    @inlineCallbacks
    def setup_transport(self):
        log.msg("Setup MahindraConviva Transport %s" % self.config)
        super(MahindraConvivaHttpTransport, self).setup_transport()
        resources = [
            self.mkres(MahindraConvivaReceiveSmsResource,
                       self.publish_message)
        ]
        self.web_resources = yield self.start_web_resources(
            resources, self.config['receive_port'])

    def teardown_transport(self):
        log.msg("Stop MahindraConviva Transport")
        self.web_resources.stopListening()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message %r" % message)
        try:
            params = {
                'REQUESTTYPE': 'SMSSubmitReq',
                'USERNAME': self.config['user_name'],
                'PASSWORD': self.config['password'],
                'MOBILENO': message['to_addr'],
                'MESSAGE': message['content'],
                'ORIGIN_ADDR': self.config['default_shortcode'],
                'TYPE': '0'
            }
            encoded_params = urlencode(params)
            log.msg('Hitting %s with %s' % (self.config['outbound_url'], encoded_params))

            response = yield http_request_full(
                "%s?%s" % (self.config['outbound_url'], encoded_params),
                method='GET')

            log.msg("Response: (%s) %r" % (response.code, response.delivered_body))
            content = response.delivered_body.strip()

            if response.code != http.OK:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
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


class MahindraConvivaReceiveSmsResource(Resource):
    isLeaf = True
    
    response = ('<SMSDeliverRes>'
                '<StatusCode>%s</StatusCode>'
                '<StatusText>%s</StatusText>'
                '<Content>%s</Content>'
                '</SMSDeliverRes>')

    def __init__(self, config, publish_func):
        log.msg("Init MahindraConvivaReveiveSmsResource")
        self.config = config
        self.publish_func =publish_func
        self.transport_name = self.config['transport_name']

    @inlineCallbacks
    def do_render(self, request):
        try:
            raw_body = request.content.read()
            log.msg('got hit with %s' % raw_body)
            mo_request = ElementTree.fromstring(raw_body)
            from_addr = mo_request.find('./Sender/Number').text
            to_addr = mo_request.find('./Recipient/Address').text
            content = mo_request.find('./MsgDetails/ShortMessage').text
            yield self.publish_func(
                transport_name=self.transport_name,
                transport_type='sms',
                to_addr=to_addr,
                from_addr=from_addr,
                content=content)
            request.setResponseCode(http.OK)
            request.write(self.response % (http.OK, 'OK', 'GOT MO MESSAGE'))
        except ParseError as ex:
            reason = "Error parsing the request body %s" % raw_body
            log.msg(reason)
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            request.write(self.response % (
                http.INTERNAL_SERVER_ERROR,
                'FAIL',
                reason))
        except Exception as ex:
            reason = "Error processing the request: %s" % (ex.message,)
            log.msg(reason)
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            request.write(self.response % (
                http.INTERNAL_SERVER_ERROR,
                'FAIL',
                reason))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET