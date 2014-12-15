

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import VumiTestCase, MockHttpServer
from vumi.utils import http_request_full

from transports import MahindraConvivaHttpTransport

class MahindraConvivaMessageMaker:

    def mk_mo(self):
        return (
            '<SMSDeliverReq>'
                '<Sender>'
                    '<AddressType>4</AddressType>'
                    '<Number>9992221234</Number>'
                '</Sender>'
                '<Recipient>'
                    '<AddressType>3</AddressType>'
                    '<Address>1900</Address>' 
                '</Recipient>'
                '<MsgDetails>'
                    '<ShortMessage>hello world</ShortMessage> '
                    '<MsgType>0</MsgType>' 
                    '<MsgPriority>1</MsgPriority>'
                    '<MessageID>1078883649267533243</MessageID>'
                '</MsgDetails>'
            '</SMSDeliverReq>')

    def mk_mo_response(self):
        return (
            '<SMSDeliverRes>'
                '<StatusCode>200</StatusCode>'
                '<StatusText>OK</StatusText>'
                '<Content>GOT MO MESSAGE</Content>'
            '</SMSDeliverRes>')

    def mk_mt_response_success(self):
        return (
            'Your message has been submitted to Jataayu SMS gateway\n'
            'PushURL Application\n'
            'Build version 4.2.0:1    Feb 29 2012')

    def mk_mt_response_failure(self):
        return (
            '17\n'
            'Unknown Error\n'
            'Mesg ID:919886881284_1207261324111\n')


class MahindraConvivaHttpTransportTestCase(VumiTestCase,
                                           MahindraConvivaMessageMaker):

    @inlineCallbacks
    def setUp(self):
        self.msdp_calls = DeferredQueue()
        self.mock_msdp = MockHttpServer(self.handle_request)
        self.mock_msdp_response = ''
        self.mock_msdp_response_code = http.OK
        yield self.mock_msdp.start()
        self.config = {
            'outbound_url': self.mock_msdp.url,
            'user_name': 'myusername',
            'password': 'mypassword',
            'receive_port': 9998,
            'receive_path': '/mahindraconviva',
            'default_shortcode': '474'
        }
        self.tx_helper = self.add_helper(TransportHelper(
            MahindraConvivaHttpTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_msdp.stop()
        yield super(MahindraConvivaHttpTransportTestCase, self).tearDown()

    def handle_request(self, request):
        self.msdp_calls.put(request)
        request.setResponseCode(self.mock_msdp_response_code)
        return self.mock_msdp_response

    @inlineCallbacks
    def test_outbound_ok(self):
        self.mock_msdp_response_code = http.OK
        self.mock_msdp_response = self.mk_mt_response_success()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')

        req = yield self.msdp_calls.get()
        self.assertEqual(
            req.args,
            {'REQUESTTYPE': ['SMSSubmitReq'],
             'USERNAME': ['myusername'],
             'PASSWORD': ['mypassword'],
             'MOBILENO': ['+41791234567'],
             'MESSAGE': ['hello world'],
             'ORIGIN_ADDR': ['747'],
             'TYPE': ['0']})

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_outbound_fail_service(self):
        self.mock_msdp_response_code = http.OK
        self.mock_msdp_response = self.mk_mt_response_failure()

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1')

        req = yield self.msdp_calls.get()
        self.assertEqual(
            req.args,
            {'REQUESTTYPE': ['SMSSubmitReq'],
             'USERNAME': ['myusername'],
             'PASSWORD': ['mypassword'],
             'MOBILENO': ['+41791234567'],
             'MESSAGE': ['hello world'],
             'ORIGIN_ADDR': ['747'],
             'TYPE': ['0']})

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['nack_reason'], '1')

    @inlineCallbacks
    def test_inbound(self):        
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        response = yield http_request_full(
            url,
            headers={'Content-Type': ['application/xml']},
            method='POST',
            data=self.mk_mo()
        )

        self.assertEqual(response.code, http.OK)
        

        [user_msg] = yield self.tx_helper.get_dispatched_inbound()        
        self.assertEqual('hello world', user_msg['content'])
        self.assertEqual('9992221234', user_msg['from_addr'])