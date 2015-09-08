import sys
import urllib
import traceback

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import http, server

from vumi import log
from vumi.transports.base import Transport
from vumi.config import ConfigText, ConfigInt
from vumi.utils import http_request_full

from transports.utils import MoResource, make_resource_worker


class TelkomselHttpTransportConfig(Transport.CONFIG_CLASS):

    mt_url = ConfigText(
        'The URL to send to MT.',
        required=True, static=True)
    mt_cpid = ConfigText(
        'The username for message authentication.',
        required=True, static=True)
    mt_pwd = ConfigText(
        'The password for message authentication.',
        required=True, static=True)
    mt_sid = ConfigText(
        'The service ID for service authentication.',
        required=True, static=True)
    mo_receive_path = ConfigText(
        'The path to listen to receive MO.',
        required=True, static=True)
    mo_receive_port = ConfigInt(
        'The port to listen to receive MO.',
        required=True, static=True)



class TelkomselHttpTransport(Transport):

    CONFIG_CLASS = TelkomselHttpTransportConfig

    transport_type = 'sms'

    success_responses = [
        '1:Success']

    fail_responses = {
        '0:1': 'Default error code',
        '0:2': 'MT rejected due to storage partition is full',
        '2': 'Authentication failed (binding failed) Permanent',
        '3:101': 'Charging timeout Temporary',
        '3:105': 'Invalid MSISDN (recipient)',
        '3:': 'Charging failed Permanent',
        '3:3:21': 'Not enough credit Temporary',
        '4:1': 'Invalid shortcode (sender)',
        '4:2:': 'Error Mandatory parameter is missing',
        '4:3': 'MT rejected due to long message restriction Permanent',
        '4:4:1': 'Multiple tariff is not allowed, but "tid" parameter is provided by CP',
        '4:4:2': 'The provided "tid" by CP is not allowed',
        '5:997': 'Invalid trx_id',
        '5:1': 'MT rejected due to subscription quota is finished',
        '5:2': 'MT rejected due to subscriber doesn\'t have this subscription',
        '5:3': 'MT rejected due to subscription is disabled',
        '5:4': 'Throttling error',
        '6': 'MT rejected due to quarantine',
        '7': 'Applicable for smart messaging and wap'
    }

    def setup_transport(self):
        log.msg("Setup Telkomsel Transport %s" % self.config)
        super(TelkomselHttpTransport, self).setup_transport()
        resources = [
            make_resource_worker(
                MoResource,
                self.handle_inbound_request,
                self.config['mo_receive_path']),
        ]
        self.web_resources = self.start_web_resources(
            resources, self.config['mo_receive_port'])

    def teardown_transport(self):
        log.msg("Stop Telkomsel Transport")
        self.web_resources.stopListening()

    @inlineCallbacks
    def handle_inbound_request(self, request):
        log.msg("Inbound request")
        yield self.publish_message(
            transport_name=self.transport_name,
            transport_type=self.transport_type,
            to_addr=request.args['adn'][0],
            from_addr=request.args['msisdn'][0],
            content=request.args['sms'][0])
        request.setResponseCode(http.ACCEPTED)
        request.write("OK")

    def extract_status_and_message(self, body):
        if body in self.success_responses:
            return 'success', None
        for error_code in self.fail_responses.iterkeys():
            if body.startswith(error_code):
                return 'fail', '%s - %s' % (body, self.fail_responses[error_code])
        return 'fail', '%s - %s' % (body, 'Unknown error')

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message %r" % message)
        config = self.get_static_config()
        try:
            data = {
                'cpid': config.mt_cpid,
                'pwd': config.mt_pwd,
                'sid': config.mt_sid,
                'sms': message['content'],
                'msisdn': message['to_addr']}
            data_encoded = urllib.urlencode(data)
            response = yield http_request_full(
                "%s?%s" % (config.mt_url, data_encoded),
                data=None,
                headers={'User-Agent': ['Vusion Telkomsel Transport']},
                method='GET')

            if response.code != http.ACCEPTED:
                reason = "HTTP ERROR %s - %s" % (
                    response.code, response.delivered_body)
                log.error(reason)
                yield self.publish_nack(message['message_id'], reason)
                return

            body = response.delivered_body.strip()
            status, description = self.extract_status_and_message(body)

            if status != 'success':
                reason = "SERVICE ERROR %s" % (description)
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
