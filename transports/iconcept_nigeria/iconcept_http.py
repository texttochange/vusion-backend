import sys
import traceback
import urllib
import xml.etree.ElementTree as ET

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.transports.base import Transport
from vumi.utils import http_request_full
from vumi import log
from vumi.persist.txredis_manager import TxRedisManager
from vumi.config import ConfigUrl, ConfigText, ConfigInt, ConfigDict


class IConceptHttpTransportConfig(Transport.CONFIG_CLASS):

    bulk_url = ConfigUrl(
        'The bulk API url to connect to.',
        required=True, static=True)
    bulk_user = ConfigText(
        'The id used to connect to the bulk API.',
        required=True, static=True)
    bulk_password = ConfigText(
        'The password used to connect to the bulk API.',
        required=True, static=True) 
    shortcode_url = ConfigUrl(
        'The shortcode API url to connect to.',
        required=True, static=True)
    shortcode_cid = ConfigText(
        'The id used to connect to the shortcode API.',
        required=True, static=True)
    shortcode_password = ConfigText(
        'The password used to connect to the shortcode API.',
        required=True, static=True)
    receive_port = ConfigInt(
        'The port to listen to receive MO.',
        required=True, static=True)
    receive_path = ConfigText(
        'The path to listen to receive MO.',
        required=True, static=True)
    redis_manager = ConfigDict(
        'How to connect to Redis.', default={}, static=True)


class IConceptHttpTransport(Transport):

    CONFIG_CLASS = IConceptHttpTransportConfig

    transport_type = 'sms'
    successfull_bodies = [
        '0',           ##'ALL_RECIPIENTS_PROCESSED' for the bulk api
        'Response message has been sent successfully' ## for the shortcode api
    ]

    bulk_api_errors = {
        '-1': 'SEND_ERROR',
        '-2': 'NOT_ENOUGH_CREDITS',
        '-3': 'NETWORK_NOTCOVERED',
        '-5': 'INVALID_USER_OR_PASS',
        '-6': 'MISSING_DESTINATION_ADDRESS',
        '-10': 'MISSING_USERNAME',
        '-11': 'MISSING_PASSWORD',
        '-13': 'INVALID_DESTINATION_ADDRESS',
        '-22': 'SYNTAX_ERROR',
        '-23': 'ERROR_PROCESSING',
        '-26': 'COMMUNICATION_ERROR',
        '-27': 'INVALID_SENDDATETIME',
        '-28': 'INVALID_DELIVERY_REPORT_PUSH_URL',
        '-30': 'INVALID_CLIENT_APPID',
        '-33': 'DUPLICATE_MESSAGEID',
        '-34': 'SENDER_NOT_ALLOWED',
        '-99': 'GENERAL_ERROR'}

    def from_status_to_error_message(self, status, api):
        if api == 'shotcode':
            return status
        if status in self.bulk_api_errors:
            return self.bulk_api_errors[status]
        return status

    def mkres(self, cls, publish_mo_func, path_key = None):
        config = self.get_static_config()
        resource = cls(publish_mo_func)
        if path_key is None:
            path = config.receive_path
        else:
            path = "%s/%s" % (config.receive_path, path_key)
        return (resource, path)

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        log.msg("Setup IConcept Transport %s" % config)
        super(IConceptHttpTransport, self).setup_transport()

        redis_prefix = '%s@%s' % (config.bulk_user, config.transport_name)
        self.redis = (yield TxRedisManager.from_config(
            config.redis_manager)).sub_manager(redis_prefix)

        resources = [
            self.mkres(IConceptMoResource, self.handle_inbound_message)
        ]
        self.web_resources = self.start_web_resources(
            resources, self.config['receive_port'])

    @inlineCallbacks
    def teardown_transport(self):
        log.msg("Stop IConcept Transport")
        self.web_resources.stopListening()
        yield self.redis._close()

    ##Pop the transaction id of this number
    @inlineCallbacks
    def has_transaction_id(self, phone):
        transaction_id = yield self.redis.get(phone)
        yield self.redis.delete(phone)
        returnValue(transaction_id)

    @inlineCallbacks
    def save_transaction_id(self, phone, transaction_id):
        yield self.redis.set(phone, transaction_id)

    @inlineCallbacks
    def handle_inbound_message(self, request):
        yield self.save_transaction_id(
            request.args['msisdn'][0],
            request.args['transaction_id'][0])
        yield self.publish_message(
            transport_name=self.transport_name,
            transport_type=self.transport_type,
            to_addr=request.args['shortcode'][0],
            from_addr=request.args['msisdn'][0],
            content=request.args['content'][0])

    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message %r" % message)
        config = self.get_static_config()
        try:
            transaction_id = yield self.has_transaction_id(message['to_addr'])
            if not transaction_id is None and len(message['content']) <= 160:
                api = 'shortcode'
                url = "%s" % config.shortcode_url.geturl()
                data = {
                    'cid': config.shortcode_cid,
                    'password': config.shortcode_password,
                    'from': message['from_addr'],
                    'content': message['content'],
                    'to': message['to_addr'],
                    'transaction_id': transaction_id}
            else:
                api = 'bulk'
                url = "%s" % config.bulk_url.geturl()
                data = {
                    'user': config.bulk_user,
                    'password': config.bulk_password,
                    'sender': message['from_addr'],
                    'SMSText': message['content'],
                    'GSM': message['to_addr']}
                if len(message['content']) > 160:
                    data.update({'type': 'longSMS'})

            data_encoded = urllib.urlencode(data)
            log.msg("Hitting %s with data %s" % (url, data_encoded))

            response = yield http_request_full(
                "%s?%s" % (url, data_encoded),
                data=None,
                headers={'User-Agent': ['Vusion IConcept Transport']},
                method='GET')

            if response.code != http.OK:
                reason = "HTTP ERROR on api %s - %s - %s" % (
                    api, response.code, response.delivered_body)
                log.error(reason)
                yield self.publish_nack(message['message_id'], reason)
                return

            body = response.delivered_body.strip()
            if api == 'bulk':
                tree = ET.fromstring(body)
                status = tree.find('result').find('status').text
            else:
                status = body

            if not status in self.successfull_bodies:
                error = self.from_status_to_error_message(status, api)
                reason = "SERVICE ERROR on api %s - %s" % (
                    api, error)
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


class IConceptMoResource(Resource):
    isLeaf = True

    def __init__(self, publish_mo_func):
        log.msg("Init IConceptMoResource")
        self.publish_mo_func = publish_mo_func

    @inlineCallbacks
    def do_render(self, request):
        try:
            yield self.publish_mo_func(request)
            request.setResponseCode(http.OK)
        except Exception as ex:
            reason = "Error processing the request: %s" % (ex.message,)
            log.msg(reason)
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error("%r" % traceback.format_exception(
                exc_type, exc_value, exc_traceback))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET
