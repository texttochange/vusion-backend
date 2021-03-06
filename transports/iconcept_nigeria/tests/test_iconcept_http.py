import urllib

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.utils import VumiTestCase, MockHttpServer
from vumi.utils import http_request_full

from tests.utils import ObjectMaker

from transports.iconcept_nigeria.iconcept_http import IConceptHttpTransport


class IConceptHttpTransportTestCase(VumiTestCase, ObjectMaker):

    @inlineCallbacks
    def setUp(self):
        self.iconcept_bulk_calls = DeferredQueue()
        self.mock_iconcept_bulk_server = MockHttpServer(self.handle_bulk_request)
        self.mock_iconcept_bulk_server_response = []
        yield self.mock_iconcept_bulk_server.start()

        self.iconcept_shortcode_calls = DeferredQueue()
        self.mock_iconcept_shortcode_server = MockHttpServer(self.handle_shortcode_request)
        self.mock_iconcept_shortcode_server_response = []
        yield self.mock_iconcept_shortcode_server.start()

        self.config = {
            'bulk_url': self.mock_iconcept_bulk_server.url,
            'bulk_user': 'ttc_bulk_login',
            'bulk_password': 'ttc_bulk_pwd',
            'shortcode_url': self.mock_iconcept_shortcode_server.url,
            'shortcode_cid': 'ttc_shortcode_login',
            'shortcode_password': 'ttc_shortcode_pwd',
            'shortcode_mo_mt_ratio': 1,
            'receive_port': 9998,
            'receive_path': '/iconcept'}

        self.tx_helper = self.add_helper(
            TransportHelper(IConceptHttpTransport))
        self.transport = yield self.tx_helper.get_transport(self.config)

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_iconcept_bulk_server.stop()
        yield self.mock_iconcept_shortcode_server.stop()
        yield super(IConceptHttpTransportTestCase, self).tearDown()

    def mk_bulk_response(self, response_id, phone_number):
        return """<?xml version="1.0" encoding="UTF-8"?>\n
                     <results>\r\n
                        <result>
                           <status>%s</status>
                           <messageid>205042215022232190</messageid>
                           <destination>%s</destination>
                        </result>\r\n
                    </results>""" % (response_id, phone_number)

    def handle_bulk_request(self, request):
        self.iconcept_bulk_calls.put(request)
        (resp_code, resp_content) = self.mock_iconcept_bulk_server_response.pop(0)
        request.setResponseCode(resp_code)
        return resp_content

    def handle_shortcode_request(self, request):
        self.iconcept_shortcode_calls.put(request)
        (resp_code, resp_content) = self.mock_iconcept_shortcode_server_response.pop(0)
        request.setResponseCode(resp_code)
        return resp_content

    #@inlineCallbacks
    #def test_outbound_bulk_ok(self):
        #response = self.mk_bulk_response('0', '2561111')
        #self.mock_iconcept_bulk_server_response.append(
            #(http.OK, response))

        #yield self.tx_helper.make_dispatch_outbound(
            #"hello world", message_id='1',
            #to_addr='2561111', from_addr='8181')

        #req = yield self.iconcept_bulk_calls.get()
        #self.assertEqual('hello world', req.args['SMSText'][0])
        #self.assertEqual('2561111', req.args['GSM'][0])
        #self.assertEqual('8181', req.args['sender'][0])
        #self.assertEqual('ttc_bulk_login', req.args['user'][0])
        #self.assertEqual('ttc_bulk_pwd', req.args['password'][0])

        #[event] = yield self.tx_helper.get_dispatched_events()
        #self.assertEqual(event['event_type'], 'ack')
        #self.assertEqual(event['user_message_id'], '1')
        #self.assertEqual(event['sent_message_id'], '1')

    #@inlineCallbacks
    #def test_outbound_bulk_fail(self):
        #self.mock_iconcept_bulk_server_response.append(
            #(http.OK, self.mk_bulk_response('-3', '2561111')))

        #self.mock_iconcept_bulk_server_response.append(
            #(http.OK, 'NETWORK_NOTCOVERED'))

        #yield self.tx_helper.make_dispatch_outbound(
            #"hello world", message_id='1',
            #to_addr='2561111', from_addr='8181')

        #req = yield self.iconcept_bulk_calls.get()
        #self.assertEqual('hello world', req.args['SMSText'][0])
        #self.assertEqual('2561111', req.args['GSM'][0])
        #self.assertEqual('8181', req.args['sender'][0])

        #[event] = yield self.tx_helper.get_dispatched_events()
        #self.assertEqual(event['event_type'], 'nack')
        #self.assertEqual(event['user_message_id'], '1')
        #self.assertEqual(
            #event['nack_reason'],
            #"SERVICE ERROR on api bulk - NETWORK_NOTCOVERED")

    def get_mo_url(self, from_addr='2561111', to_addr='8181',
                   transaction_id='1234'):
        data = {
            'guid': 'a46f90ca-474b-41db-a26f-bc53752005fa',
            'operatorID': 'MTN',
            'shortcode': to_addr,
            'msisdn': from_addr,
            'log_date': '2011-12-28 19:03:38.450',
            'content': 'hello world',
            'keyword': 'hello',
            'transaction_id': transaction_id}
        url = ("http://localhost:%s%s"
               % (self.config['receive_port'], self.config['receive_path']))
        url_full = "%s?%s" % (url, urllib.urlencode(data))
        return url_full 

    @inlineCallbacks
    def test_mo(self):
        response = yield http_request_full(
            self.get_mo_url(transaction_id='1234'), method='GET')
        self.assertEqual(response.code, http.OK)

        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual('hello world', user_msg['content'])
        self.assertEqual('2561111', user_msg['from_addr'])
        self.assertEqual('8181', user_msg['to_addr'])
        self.assertEqual({'operator_id': 'MTN'}, user_msg['transport_metadata'])

    @inlineCallbacks
    def test_mt_shortcode_ok(self):
        self.mock_iconcept_shortcode_server_response.append(
            (http.OK, 'status:200;description:Your request has been processed.'))

        ### first get the a MO to initiate the shortcode api flow
        response = yield http_request_full(
            self.get_mo_url(transaction_id='1234'), method='GET')
        self.assertEqual(response.code, http.OK)

        [user_msg] = yield self.tx_helper.get_dispatched_inbound()
        self.assertEqual('hello world', user_msg['content'])
        self.assertEqual('2561111', user_msg['from_addr'])
        self.assertEqual('8181', user_msg['to_addr'])

        ## second use the transaction id to reply
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1',
            to_addr='2561111', from_addr='8181',
            transport_metadata={'operator_id': 'MTN'})

        req = yield self.iconcept_shortcode_calls.get()
        self.assertEqual('hello world', req.args['content'][0])
        self.assertEqual('2561111', req.args['to'][0])
        self.assertEqual('8181', req.args['from'][0])
        self.assertEqual('1234', req.args['transaction_id'][0])
        self.assertEqual('ttc_shortcode_login', req.args['cid'][0])
        self.assertEqual('ttc_shortcode_pwd', req.args['password'][0])

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(event['sent_message_id'], '1')

        ## third get the a MT via the shortcode api flow
        self.mock_iconcept_shortcode_server_response.append(
            (http.OK, 'status:200;description:Your request has been processed.'))

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='2',
            to_addr='2561111', from_addr='8181',
            transport_metadata={'operator_id': 'MTN'})

        req = yield self.iconcept_shortcode_calls.get()
        self.assertEqual('hello world', req.args['content'][0])
        self.assertEqual('2561111', req.args['to'][0])
        self.assertEqual('8181', req.args['from'][0])
        self.assertEqual('MTN', req.args['OperatorID'][0])
        self.assertEqual('ttc_shortcode_login', req.args['cid'][0])
        self.assertEqual('ttc_shortcode_pwd', req.args['password'][0])

        [event1, event2] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event2['event_type'], 'ack')
        self.assertEqual(event2['user_message_id'], '2')
        self.assertEqual(event2['sent_message_id'], '2')

        ### third get the a second MT via the bulk api flow
        #response = self.mk_bulk_response('0', '2561111')
        #self.mock_iconcept_bulk_server_response.append(
            #(http.OK, response))
        #yield self.tx_helper.make_dispatch_outbound(
            #"second message", message_id='1',
            #to_addr='2561111', from_addr='8181')

        #req = yield self.iconcept_bulk_calls.get()
        #self.assertEqual("second message", req.args['SMSText'][0])

    #@inlineCallbacks
    #def test_long_message_always_use_bulk_ok(self):
        #response = self.mk_bulk_response('0', '2561111')
        #self.mock_iconcept_bulk_server_response.append(
            #(http.OK, response))

        #response = yield http_request_full(
            #self.get_mo_url(transaction_id='1234'), method='GET')
        #self.assertEqual(response.code, http.OK)

        #[user_msg] = yield self.tx_helper.get_dispatched_inbound()
        #self.assertEqual('hello world', user_msg['content'])
        #self.assertEqual('2561111', user_msg['from_addr'])
        #self.assertEqual('8181', user_msg['to_addr'])

        #yield self.tx_helper.make_dispatch_outbound(
            #self.mk_content(161), message_id='1',
            #to_addr='2561111', from_addr='8181')

        #req = yield self.iconcept_bulk_calls.get()
        #self.assertEqual(161, len(req.args['SMSText'][0]))
        #self.assertEqual('2561111', req.args['GSM'][0])
        #self.assertEqual('8181', req.args['sender'][0])
        #self.assertEqual('ttc_bulk_login', req.args['user'][0])
        #self.assertEqual('ttc_bulk_pwd', req.args['password'][0])
        #self.assertEqual('longSMS', req.args['type'][0])

        #[event] = yield self.tx_helper.get_dispatched_events()
        #self.assertEqual(event['event_type'], 'ack')
        #self.assertEqual(event['user_message_id'], '1')
        #self.assertEqual(event['sent_message_id'], '1')

    @inlineCallbacks
    def test_mt_shortcode_fail(self):
        self.mock_iconcept_shortcode_server_response.append(
            (http.OK, 'status:101;description:The credentials you supplied are incorrect.'))

        #response = yield http_request_full(
            #self.get_mo_url(transaction_id='1234'), method='GET')

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1',
            to_addr='2561111', from_addr='8181',
            transport_metadata={'operator_id': 'MTN'})

        req = yield self.iconcept_shortcode_calls.get()
        self.assertEqual('hello world', req.args['content'][0])
        self.assertEqual('2561111', req.args['to'][0])
        self.assertEqual('8181', req.args['from'][0])
        #self.assertEqual('1234', req.args['transaction_id'][0])
        self.assertEqual('ttc_shortcode_login', req.args['cid'][0])
        self.assertEqual('ttc_shortcode_pwd', req.args['password'][0])

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(
            event['nack_reason'],
            "SERVICE ERROR on api shortcode - The credentials you supplied are incorrect.")

    @inlineCallbacks
    def test_mt_shortcode_fail_no_description(self):
        self.mock_iconcept_shortcode_server_response.append(
            (http.OK, 'status:101'))

        #response = yield http_request_full(
            #self.get_mo_url(transaction_id='1234'), method='GET')

        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1',
            to_addr='2561111', from_addr='8181',
            transport_metadata={'operator_id': 'MTN'})

        req = yield self.iconcept_shortcode_calls.get()
        self.assertEqual('hello world', req.args['content'][0])
        self.assertEqual('2561111', req.args['to'][0])
        self.assertEqual('8181', req.args['from'][0])
        #self.assertEqual('1234', req.args['transaction_id'][0])
        self.assertEqual('ttc_shortcode_login', req.args['cid'][0])
        self.assertEqual('ttc_shortcode_pwd', req.args['password'][0])

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(
            event['nack_reason'],
            "SERVICE ERROR on api shortcode - None")

    @inlineCallbacks
    def test_mt_shortcode_fail_no_operator_id(self):
        yield self.tx_helper.make_dispatch_outbound(
            "hello world", message_id='1',
            to_addr='2561111', from_addr='8181')

        [event] = yield self.tx_helper.get_dispatched_events()
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], '1')
        self.assertEqual(
            event['nack_reason'],
            "TRANSPORT ERROR Missing Operator Id")
