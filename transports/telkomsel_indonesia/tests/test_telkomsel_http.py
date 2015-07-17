import urllib

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import VumiTestCase, MockHttpServer
from vumi.utils import http_request_full

from tests.utils import ObjectMaker

from transports import TelkomselHttpTransport


class TelkomselHttpTransportTestCase(VumiTestCase, ObjectMaker):

    @inlineCallbacks
    def setUp(self):
        self.telkomsel_calls = DeferredQueue()
        self.mock_telkomsel_server = MockHttpServer(self.handle_request)
        self.mock_telkomsel_server_response = []
        yield self.mock_telkomsel_server.start()

        self.config = {
            'mt_url': self.mock_telkomsel_server.url,
            'mt_cpid': 'username',
            'mt_pwd': 'password',
            'mt_sid': 'myservice',
            'mo_receive_path': '/telkomsel',
            'mo_receive_port': 9998}

        self.tx_helper = self.add_helper(
            TransportHelper(TelkomselHttpTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_telkomsel_server.stop()
        yield super(TelkomselHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.telkomsel_calls.put(request)
        (resp_code, resp_content) = self.mock_telkomsel_server_response.pop(0)
        request.setResponseCode(resp_code)
        return resp_content

    def get_mo_url(self, from_addr='628118003967'):
        data = {
            'trx_id': '140403164020@140404164020@2000SMS16304655',
            'adn': '2000',
            'msisdn': from_addr,
            'sms': 'Hello'}
        url = ("http://localhost:%s%s"
               % (self.config['mo_receive_port'], self.config['mo_receive_path']))
        url_full = "%s?%s" % (url, urllib.urlencode(data))
        return url_full 

    @inlineCallbacks
    def test_mo(self):
        response = yield http_request_full(
            self.get_mo_url(), method='GET')
        self.assertEqual(response.code, http.ACCEPTED)

        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual('Hello', user_msg['content'])
        self.assertEqual('628118003967', user_msg['from_addr'])
        self.assertEqual('2000', user_msg['to_addr'])

    @inlineCallbacks
    def test_mt_ok(self):
        self.mock_telkomsel_server_response.append(
            (http.ACCEPTED, '1:Success'))

        yield self.tx_helper.make_dispatch_outbound(
            'hello world', message_id='1', to_addr='628118003967')
        
        req = yield self.telkomsel_calls.get()
        self.assertEqual('hello world', req.args['sms'][0])
        self.assertEqual('628118003967', req.args['msisdn'][0])
        self.assertEqual('username', req.args['cpid'][0])
        self.assertEqual('password', req.args['pwd'][0])
        self.assertEqual('myservice', req.args['sid'][0])

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_mt_fail_http(self):
        self.mock_telkomsel_server_response.append(
                    (http.INTERNAL_SERVER_ERROR, 'something'))

        yield self.tx_helper.make_dispatch_outbound(
            'hello world', message_id='1', to_addr='628118003967')

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(
            event['nack_reason'],
            "HTTP ERROR 500 - something")

    @inlineCallbacks
    def test_mt_fail_service_known(self):
        self.mock_telkomsel_server_response.append(
            (http.ACCEPTED, '4:2:18'))

        yield self.tx_helper.make_dispatch_outbound(
            'hello world', message_id='1', to_addr='628118003967')

        req = yield self.telkomsel_calls.get()
        self.assertEqual('hello world', req.args['sms'][0])
        self.assertEqual('628118003967', req.args['msisdn'][0])
        self.assertEqual('username', req.args['cpid'][0])
        self.assertEqual('password', req.args['pwd'][0])
        self.assertEqual('myservice', req.args['sid'][0])

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(
            event['nack_reason'],
            "SERVICE ERROR 4:2:18 - Error Mandatory parameter is missing")

    @inlineCallbacks
    def test_mt_fail_service_unknown(self):
        self.mock_telkomsel_server_response.append(
            (http.ACCEPTED, '8:3:18'))

        yield self.tx_helper.make_dispatch_outbound(
            'hello world', message_id='1', to_addr='628118003967')

        req = yield self.telkomsel_calls.get()
        self.assertEqual('hello world', req.args['sms'][0])
        self.assertEqual('628118003967', req.args['msisdn'][0])
        self.assertEqual('username', req.args['cpid'][0])
        self.assertEqual('password', req.args['pwd'][0])
        self.assertEqual('myservice', req.args['sid'][0])

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(
            event['nack_reason'],
            "SERVICE ERROR 8:3:18 - Unknown error")
