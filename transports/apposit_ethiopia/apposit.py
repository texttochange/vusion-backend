import json, ast
from urllib import urlencode
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


class AppositV2TransportConfig(HttpRpcTransport.CONFIG_CLASS):

    app_id = ConfigText('your http connection id', required=True, static=True)
    token = ConfigText('your http connection token', required=True, static=True)
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


class AppositV2Transport(AppositTransport):
    
    CONFIG_CLASS = AppositV2TransportConfig    
    EXPECTED_FIELDS = frozenset(['from', 'to', 'message', 'isBinary', 'callbackType'])

    def validate_config(self):
        config = self.get_static_config()
        self.web_path = config.web_path
        return super(AppositV2Transport, self).validate_config()

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

        log.msg("AppositTransport receiving inbound message from "
                  "%(from)s to %(to)s" % values)
        if request.args.get('channel')[0] == "ivr":
            values['message'] = 'voicea'
            yield self.publish_message(
                transport_name=self.transport_name,
                message_id=message_id,
                content=values['message'],
                from_addr=values['from'],
                to_addr=values['to'],
                provider='apposit',
                transport_type=self.TRANSPORT_TYPE_LOOKUP[channel],
                transport_metadata={'apposit': {'isBinary': values['isBinary']}})
            
            params01 = dict((k, v.encode(self.ENCODING)) for k, v in {
                'phone': values['from'],
                'tree_id': config.outbound_api_tree_id,
                'voice_sender_id': config.outbound_api_sender,
                'schedule_type': 'now',                    
                }.iteritems())

            config = self.get_static_config()

            response = yield http_request_full(
                config.outbound_url_api,
                data=json.dumps(params01, ensure_ascii=False),
                method='POST',
                headers={'Content-Type': 'application/json; charset=UTF-8',
                'api_key': config.outbound_api_key,
                'H1':'V1'})            
            
            log.msg("Voto Push Response: (%s) %r" %
                        (response.code, response.delivered_body))
            
        else:
            yield self.publish_message(
                transport_name=self.transport_name,
                message_id=message_id,
                content=values['message'],
                from_addr=values['from'],
                to_addr=values['to'],
                provider='apposit',
                transport_type=self.TRANSPORT_TYPE_LOOKUP[channel],
                transport_metadata={'apposit': {'isBinary': values['isBinary']}})                   

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
        params = dict((k, v.encode(self.ENCODING)) for k, v in {
            'from': message['from_addr'],
            'to': message['to_addr'],
            'message': message['content'],
        }.iteritems())
        
        config = self.get_static_config()
        app_id = config.app_id
        token = config.token

        auth = b64encode("%s:%s" % (app_id, token))
        
        
        log.msg("Making HTTP POST request: %s with body %s" %
                  (self.outbound_url, json.dumps(params, ensure_ascii=False)))

        response = yield http_request_full(
            self.outbound_url,
            data=json.dumps(params, ensure_ascii=False),
            method='POST',
            headers={'Content-Type': 'application/json; charset=UTF-8',
            'Authorization': ['Basic %s' % auth],
            'H1':'V1'})

        
        log.msg("Response: (%s) %r" %
                  (response.code, response.delivered_body))

        response_content = response.delivered_body.strip()
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
