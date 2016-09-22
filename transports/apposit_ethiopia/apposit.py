import json
from urllib import urlencode
from base64 import b64encode

from twisted.web import http
from twisted.internet.defer import inlineCallbacks

from vumi import log
from vumi.utils import http_request_full
from vumi.config import ConfigDict, ConfigText
from vumi.transports.apposit import AppositTransport
from vumi.transports.httprpc import HttpRpcTransport


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


class AppositV2Transport(AppositTransport):
    
    CONFIG_CLASS = AppositV2TransportConfig
    
    def validate_config(self):
        config = self.get_static_config()
        self.web_path = config.web_path
        return super(AppositV2Transport, self).validate_config()
        
    @inlineCallbacks
    def handle_outbound_message(self, message):
        channel = self.CHANNEL_LOOKUP.get(message['transport_type'])
        if channel is None:
            reason = (self.UNSUPPORTED_TRANSPORT_TYPE_ERROR
                      % message['transport_type'])
            log.msg(reason)
            yield self.publish_nack(message['message_id'], reason)
            return

        #self.emit("Sending outbound message: %s" % (message,))
        log.msg("Sending outbound message: %s" % (message,))

        # build the params dict and ensure each param encoded correctly
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
                  (self.outbound_url, params))

        response = yield http_request_full(
            self.outbound_url,
            data=json.dumps(params),
            method='POST',
            headers={'Content-Type': 'application/json',
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
