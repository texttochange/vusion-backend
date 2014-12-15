from base64 import b64encode
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import VumiTestCase, MockHttpServer
from vumi.utils import http_request_full

from transports.crmtext_usa.crmtext_http import CrmTextHttpTransport


class CrmTextMessageMaker:

    def mk_setcallback_response_ok(self):
        return (
            '<response op="setcallback" status="200" message="ok">')

    def mk_mt_response_ok(self):
        return (
            '<response op="sendsmsmsg" status="200" message="ok">'
            '<message>'
            '<msgID>999111222</msgID>'
            '<message>John, your vehicle is due for service. Can I schedule an appt for you?</message>'
            '<timestamp>2014-08-11 14:23:36</timestamp>'
            '<txnid>0987654321</txnid>'
            '<msg_direction>MT</msg_direction>'
            '<user_id>1234</user_id>'
            '<customer>'
            '<custId>1234567891011</custId>'
            '<subacct>456</subacct>'
            '<timestamp>2014-08-11 09:19:12</timestamp>'
            '<optinStatus>3</optinStatus>'
            '<custName>John Queue</custName>'
            '<custMobile>9993338888</custMobile>'
            '</customer>'
            '</message>'
            '</response>')

    def mk_mt_response_fail(self, status='400', message='Customer is Opted-Out'):
        return (
            '<response op="sendsmsmsg" status="%s" message="%s">'
            '</response>') % (status, message)

    def mk_mo(self):
        return {
            'CustID': '112233',
            'Message': 'hello world',
            'Keyword': 'PIZZA123',
            'Mobile Number': '9992221234',
            'Opt-in Status': '3',
            'TimeStamp': '20140101 15:30:45',
            'SubAcct': '12345',
            'CustName': 'Joe Smith',
            'msgID': '123456789',
            'Subacct_Name': 'The Store Name'}

    def mk_mo_2(self):
        return (
            "------------------------------ba3884f919c6\r\n"
            "Content-Disposition: form-data; name=\"custID\"\r\n"
            "\r\n"
            "7829018\r\n"
            "------------------------------ba3884f919c6\r\n"
            'Content-Disposition: form-data; name="message"\r\n'
            '\r\n'
            'ttc hello world\r\n'
            '------------------------------ba3884f919c6\r\n'
            'Content-Disposition: form-data; name="keyword"\r\n'
            '\r\n'
            'TTC\r\n'
            '------------------------------ba3884f919c6\r\n'
            'Content-Disposition: form-data; name="mobileNum"\r\n'
            '\r\n'
            '9992221234 <tel:9992221234> \r\n'
            '------------------------------ba3884f919c6\r\n'
            'Content-Disposition: form-data; name="optInStatus"\r\n'
            '\r\n'
            'opt-in\r\n'
            '------------------------------ba3884f919c6\r\n'
            'Content-Disposition: form-data; name="timeStamp"\r\n'
            '\r\n'
            '2014-05-12 08:05:42\r\n'
            '------------------------------ba3884f919c6\r\n'
            'Content-Disposition: form-data; name="subacct"\r\n'
            '\r\n'
            '4021\r\n'
            '------------------------------ba3884f919c6\r\n'
            'Content-Disposition: form-data; name="custName"\r\n'
            '\r\n'
            '\r\n'
            '------------------------------ba3884f919c6\r\n'
            'Content-Disposition: form-data; name="msgID"'
            '\r\n'
            '9961254\r\n'
            '------------------------------ba3884f919c6\r\n'
            'Content-Disposition: form-data; name="subacct_name"\r\n'
            '\r\n'
            'TTC Mobile\r\n'
            '------------------------------ba3884f919c6--')


class CrmTextHttpTransportTestCase(VumiTestCase, CrmTextMessageMaker):

    @inlineCallbacks
    def setUp(self):
        self.crmtext_calls = DeferredQueue()
        self.mock_crmtext_server = MockHttpServer(self.handle_request)
        self.mock_crmtext_server_response = ''
        self.mock_crmtext_server_response_code = http.OK
        yield self.mock_crmtext_server.start()
        self.config = {
            'url': self.mock_crmtext_server.url,
            'user_id': 'mylogin',
            'password': 'mypassword',
            'keyword_store': 'TTC',
            'receive_domain': 'http://localhost',
            'receive_port': 9998,
            'receive_path': '/crmtext',
            'shortcode': '444999'}
        self.tx_helper = self.add_helper(TransportHelper(CrmTextHttpTransport))

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_crmtext_server.stop()
        yield super(CrmTextHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.crmtext_calls.put(request)
        request.setResponseCode(self.mock_crmtext_server_response_code)
        return self.mock_crmtext_server_response

    def assert_authentication(self, request):
        headers = request.getAllHeaders()
        self.assertTrue('authorization' in headers, "authorization header is missing")
        value = headers['authorization']

        auth = b64encode("%s:%s:%s" % (self.config['user_id'], self.config['password'], self.config['keyword_store']))
        self.assertEqual(value, "Basic %s" % auth)

    @inlineCallbacks
    def test_set_callback(self):
        self.mock_crmtext_server_response_code = http.OK
        self.mock_crmtext_server_response = self.mk_setcallback_response_ok()
        self.transport = yield self.tx_helper.get_transport(self.config)
        
        req = yield self.crmtext_calls.get()
        headers = dict(req.requestHeaders.getAllRawHeaders())
        self.assert_authentication(req)
        callback_url = 'http://localhost:9998/crmtext' 
        self.assertEqual(
            req.args,
            {'method': ['setcallback'],
             'callback': [callback_url]})

        events = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(len(events), 0)

    @inlineCallbacks
    def test_outbound_ok(self):
        self.transport = yield self.tx_helper.get_transport(self.config)        
        callback_req = yield self.crmtext_calls.get()
        
        self.mock_crmtext_server_response_code = http.OK
        self.mock_crmtext_server_response = self.mk_mt_response_ok()
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')

        req = yield self.crmtext_calls.get()
        headers = dict(req.requestHeaders.getAllRawHeaders())
        self.assert_authentication(req)
        self.assertEqual(
            req.args,
            { 'method': ['sendsmsmsg'],
              'message': ['hello world'],
              'phone_number': ['+41791234567']})

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_fail_service(self):
        self.mock_crmtext_server_response_code = http.OK
        self.mock_crmtext_server_response = self.mk_setcallback_response_ok()
        self.transport = yield self.tx_helper.get_transport(self.config)
        callback_req = yield self.crmtext_calls.get()
        
        self.mock_crmtext_server_response_code = http.OK
        self.mock_crmtext_server_response = self.mk_mt_response_fail()
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')

        req = yield self.crmtext_calls.get()
        headers = dict(req.requestHeaders.getAllRawHeaders())
        self.assert_authentication(req)
        self.assertEqual(
            req.args,
            { 'method': ['sendsmsmsg'],
              'message': ['hello world'],
              'phone_number': ['+41791234567']})

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], 'SERVICE ERROR 400 - Customer is Opted-Out')

    @inlineCallbacks
    def test_outbound_fail_transport(self):
        self.mock_crmtext_server_response_code = http.OK
        self.mock_crmtext_server_response = self.mk_setcallback_response_ok()
        self.transport = yield self.tx_helper.get_transport(self.config)
        callback_req = yield self.crmtext_calls.get()

        self.mock_crmtext_server.stop()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], 'TRANSPORT ERROR Connection refused')

    @inlineCallbacks
    def test_inbound(self):
        self.mock_crmtext_server_response_code = http.OK
        self.mock_crmtext_server_response = self.mk_setcallback_response_ok()        
        self.transport = yield self.tx_helper.get_transport(self.config)

        data = self.mk_mo_2()
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(
            url,
            headers={'Content-Type': ['multipart/form-data; boundary=----------------------------ba3884f919c6']},
            method='POST',
            data=data
        )

        self.assertEqual(response.code, http.OK)

        [user_msg] = yield self.tx_helper.get_dispatched_inbound()        
        self.assertEqual('ttc hello world', user_msg['content'])
        self.assertEqual('9992221234', user_msg['from_addr'])
