import json, ast
from urllib import urlencode
from urlparse import parse_qsl
from base64 import b64encode
from io import StringIO

from twisted.web import http
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi import log
from vumi.utils import http_request_full
from vumi.config import ConfigDict, ConfigText
from vumi.transports.apposit import AppositTransport
from vumi.transports.httprpc import HttpRpcTransport
from vumi.transports.httprpc.tests.test_httprpc import JSONTransport


class MobifoneTransportConfig(HttpRpcTransport.CONFIG_CLASS):

    account = ConfigText('your http connection id', required=True, static=True)
    password = ConfigText('your http connection token', required=True, static=True)
    credentials = ConfigDict(
        "A dictionary where the `from_addr` is used for the key lookup and "
        "the returned value should be a dictionary containing the "
        "corresponding username, password and service id.",
        required=True, static=True)
    outbound_url = ConfigText(
        "The URL to send outbound messages to.", required=True, static=True)
    web_path = ConfigText("The path to listen for requests on.", static=True)
    outbound_url_api = ConfigText("The URL to send outbound messages to VOTO.", static=True)
    outbound_api_key = ConfigText("your VOTO http connection token key.", static=True)
    outbound_api_sender = ConfigText("your VOTO http connection sender id.", static=True)
    outbound_api_tree_id = ConfigText("your VOTO http connection first tree id.", static=True)


class MobifoneTransport(AppositTransport):
    
    CONFIG_CLASS = MobifoneTransportConfig    
    EXPECTED_FIELDS = frozenset(['from', 'to', 'message', 'isBinary', 'callbackType'])

    def validate_config(self):
        config = self.get_static_config()
        self.web_path = config.web_path
        return super(MobifoneTransport, self).validate_config()

    def get_field_values(self, request, EXPECTED_FIELDS,
                            ignored_fields=frozenset(['channelId','applicationTriggerUUID', 'accountUUID', 'applicationCallbackUrl', 'applicationUUID', 'receivedDateTime', 'messageId', 'applicationCallbackUUID', 'sessionId'])):
        values = {}
        errors = {}
        a = json.load(request.content)
        log.msg("inbound2 %s" % a)
        
        c = {u'isBinary': u'true'}
        a.update(c)        
        b = ast.literal_eval(json.dumps(a, ensure_ascii=False))
        for field in b:
            if field not in (EXPECTED_FIELDS | ignored_fields):
                if self._validation_mode == self.STRICT_MODE:
                    errors.setdefault('unexpected_parameter', []).append(field)
            else:
                values[field] = (
                    b.get(field))
        for field in EXPECTED_FIELDS:
            if field not in values:
                errors.setdefault('missing_parameter', []).append(field)
        return values, errors

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):        
        values, errors = self.get_field_values(request, self.EXPECTED_FIELDS)

        r = request.args.get('channel')[0]
        log.msg("inboundparam %s" % r)
        
        channel = 'SMS'
        if channel is not None and channel not in self.CHANNEL_LOOKUP.values():
            errors['unsupported_channel'] = channel

        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors),
                                      code=http.BAD_REQUEST)
            return

        log.msg("MobifoneTransport receiving inbound message from "
                  "%(from)s to %(to)s" % values)        
           
        yield self.publish_message(
            transport_name=self.transport_name,
            message_id=message_id,
            content=values['message'],
            from_addr=values['from'],
            to_addr=values['to'],
            provider='mobifone',
            transport_type=self.TRANSPORT_TYPE_LOOKUP[channel],
            transport_metadata={'mobifone': {'isBinary': values['isBinary']}})                   

        yield self.finish_request(
            message_id, json.dumps({'message_id': message_id}))

    @inlineCallbacks
    def handle_outbound_message(self, message):
        channel = self.CHANNEL_LOOKUP.get(message['transport_type'])
        if channel is None:
            reason = (self.UNSUPPORTED_TRANSPORT_TYPE_ERROR
                      % message['transport_type'])
            log.msg(reason)
            yield self.publish_nack(message['message_id'], reason)
            return

        # self.emit("Sending outbound message: %s" % (message,))
        log.msg("Sending outbound message: %s" % (message,))

        #build the params dict and ensure each param encoded correctly
        credentials = self.credentials.get(message['from_addr'], {})
                
        config = self.get_static_config()
                
        params_credentials = dict((k, v.encode(self.ENCODING)) for k, v in {
            'msg_name': 'login',
            'account': config.account,
            'password': config.password,
        }.iteritems())  
        
        log.msg("Making HTTP POST request: %s with body %s" %
                  (self.outbound_url, json.dumps(params_credentials, ensure_ascii=False)))

        response_login = yield http_request_full(
            "%s?%s" % (self.outbound_url, urlencode(params_credentials)),
            headers={'Content-Type': 'application/json'},
            method='POST') 
        
        log.msg("Response Login: (%s) %r" %
                          (response_login.code, response_login.delivered_body))        
        
        response_login_body = dict(parse_qsl(response_login.delivered_body))        
        
        #params_keep_session = dict((k, v.encode(self.ENCODING)) for k, v in {
            #'msg_name': 'keep_session',
            #'session_id': response_login_body['session_id'],
            #}.iteritems()) 
        
        #response_keep_session = yield http_request_full(
                    #"%s?%s" % (self.outbound_url, urlencode(params_keep_session)),
                    #headers={'Content-Type': 'application/json'},
                    #method='POST')
        
        #log.msg("Response session: (%s) %r" %
                                  #(response_keep_session.code, response_keep_session.delivered_body))        
        
        params_send_sms = {}
        if any(response_login_body) == True:
            params_send_sms = dict((k, v.encode(self.ENCODING)) for k, v in {
                    'msg_name': 'send_sms',
                    'session_id': response_login_body['session_id'],
                    'msisdn': message['to_addr'],
                    'sms_template_code': '127701',
                    'param1': message['content'],
                    }.iteritems())
        
        response =yield http_request_full(
            "%s?%s" % (self.outbound_url, urlencode(params_send_sms)),
            headers={'Content-Type': 'application/json'},
            method='POST')
          
        
        log.msg("Response: (%s) %r" %
                  (response.code, response.delivered_body))

        response_content = response.delivered_body
        if response.code == http.OK:
            yield self.publish_ack(user_message_id=message['message_id'],
                                   sent_message_id=message['message_id'])
        else:
            error = self.KNOWN_ERROR_RESPONSE_CODES.get(response_content)
            if error is not None:
                reason = "(%s) %s" % (response_content, error)
            else:
                reason = self.UNKNOWN_RESPONSE_CODE_ERROR % response_content
            log.msg(reason)
            yield self.publish_nack(message['message_id'], reason)
