import sys
import traceback
import urlparse
import cgi
import re
from cStringIO import StringIO
from urllib import urlencode
from base64 import b64encode
from xml.etree import ElementTree
from xml.etree.ElementTree import ParseError

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full
from vumi import log


class CrmTextHttpTransport(Transport):

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
        log.msg("Setup CrmText Transport %s" % self.config)
        super(CrmTextHttpTransport, self).setup_transport()
        resources = [
            self.mkres(CrmTextReceiveSmsResource, self.publish_message)
        ]
        self.web_resources = yield self.start_web_resources(
            resources, self.config['receive_port'])
        self.set_callback()

    def teardown_transport(self):
        log.msg("Stop CRMText Transport")
        self.web_resources.stopListening()

    def get_auth_header(self): 
        auth = ('%s:%s:%s' % (
            self.config['user_id'],
            self.config['password'],
            self.config['keyword_store']))
        return b64encode(auth)

    @inlineCallbacks
    def set_callback(self):
        log.msg("Registering callback...")
        params = {
            'method': 'setcallback',
            'callback': "%s:%s%s" % (
                self.config['receive_domain'],
                self.config['receive_port'],
                self.config['receive_path'])}
        encoded_params = urlencode(params)
        response = yield http_request_full(
            "%s?%s" % (self.config['url'], encoded_params),
            headers={
                'User-Agent': ['Vusion CrmText Transport'],
                'Content-Type': ['application/xml;charset=UTF-8'],
                'Authorization': ['Basic %s' % self.get_auth_header()]},
            method='POST')
        if response.code != http.OK:
            response_msg = ElementTree.fromstring(response.delivered_body)
            error_message = response_msg.attrib['message']
            reason = "TRANSPORT FAILD SETCALLBACK %s - %s" % (response.code, error_message)
            log.error(reason)
            self.teardown_transport()
        log.msg("Callback registered!")

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message %r" % message)
        try:
            params = {
                'method': 'sendsmsmsg',
                'phone_number': message['to_addr'],
                'message': message['content']}
            encoded_params = urlencode(params)
            log.msg('Hitting %s with %s' % (self.config['url'], encoded_params))

            response = yield http_request_full(
                "%s?%s" % (self.config['url'], encoded_params),
                headers={
                    'User-Agent': ['Vumi CrmText Transport'],
                    'Content-Type': ['application/xml;charset=UTF-8'],
                    'Authorization': ['Basic %s' % self.get_auth_header()]},
                method='POST')

            if response.code != 200:
                reason = "HTTP ERROR %s - %s" % (response.code, response.delivered_body)
                yield self.publish_nack(message['message_id'], reason)
                return

            response_elt = ElementTree.fromstring(response.delivered_body)
            if response_elt.attrib['status'] != "200":
                reason = "SERVICE ERROR %s - %s" % (response_elt.attrib['status'], response_elt.attrib['message'], )
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


class CrmTextReceiveSmsResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_func):
        log.msg("Init CrmTextReceiveSmsResource")
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']

    @inlineCallbacks
    def do_render(self, request):
        try:
            raw_body = request.content.read()
            log.msg('got hit with %s' % raw_body)
            ctype, pdict = cgi.parse_header(request.getHeader('content-type'))
            log.msg('content type is %r' % pdict)
            parsed = cgi.parse_multipart(StringIO(raw_body), pdict)
            yield self.publish_func(
                transport_name=self.transport_name,
                transport_type='sms',
                to_addr=self.config['shortcode'],
                from_addr=re.match('[0-9]*', parsed['mobileNum'][0]).group(0),
                content=parsed['message'][0])
            request.setResponseCode(http.OK)
        except:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.msg("Error processing the request: %s" % (request,))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET
