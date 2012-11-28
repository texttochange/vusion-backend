import redis
from time import sleep

from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from smpp.pdu_builder import SubmitSMResp, DeliverSM

from vumi.tests.utils import FakeRedis
from vumi.message import TransportUserMessage
from vumi.transports.smpp.clientserver.client import (
        EsmeTransceiver, ESME, KeyValueStore, EsmeCallbacks)
from vumi.transports.smpp.clientserver.tests.test_client import (
        KeyValueStoreTestCase)
from vumi.transports.smpp.transport import (SmppTransport,
                                            SmppTxTransport,
                                            SmppRxTransport)
from vumi.transports.smpp.service import SmppService
from vumi.transports.smpp.clientserver.config import ClientConfig
from vumi.transports.smpp.clientserver.tests.utils import SmscTestServer
from vumi.transports.tests.test_base import TransportTestCase

from transports.push_tz_smpp import PushTzSmppTransport

import redis
class RedisTestEsmeTransceiver(EsmeTransceiver):

    def send_pdu(self, pdu):
        pass  # don't actually send anything


class RedisTestSmppTransport(SmppTransport):

    def send_smpp(self, message):
        to_addr = message['to_addr']
        text = message['content']
        sequence_number = self.esme_client.submit_sm(
                short_message=text.encode('utf-8'),
                destination_addr=str(to_addr),
                source_addr="1234567890",
                )
        return sequence_number


class MockSmppTransport(PushTzSmppTransport):
    def _setup_message_consumer(self):
        super(MockSmppTransport, self)._setup_message_consumer()
        self._block_till_bind.callback(None)


def mk_expected_pdu(direction, sequence_number, command_id, **extras):
    headers = {
        'command_status': 'ESME_ROK',
        'sequence_number': sequence_number,
        'command_id': command_id,
        }
    headers.update(extras)
    return {"direction": direction, "pdu": {"header": headers}}


class EsmeToSmscTestCasePlus(TransportTestCase):

    transport_name = "esme_testing_transport"
    transport_class = MockSmppTransport

    def assert_pdu_header(self, expected, actual, field):
        self.assertEqual(expected['pdu']['header'][field],
                         actual['pdu']['header'][field])

    def assert_server_pdu(self, expected, actual):
        self.assertEqual(expected['direction'], actual['direction'])
        self.assert_pdu_header(expected, actual, 'sequence_number')
        self.assert_pdu_header(expected, actual, 'command_status')
        self.assert_pdu_header(expected, actual, 'command_id')

    @inlineCallbacks
    def setUp(self):
        yield super(EsmeToSmscTestCasePlus, self).setUp()
        self.config = {
            "system_id": "VumiTestSMSC",
            "password": "password",
            "host": "localhost",
            "port": 0,
            "redis": {},
            "transport_name": self.transport_name,
            "transport_type": "smpp",
        }
        self.service = SmppService(None, config=self.config)
        yield self.service.startWorker()
        self.service.factory.protocol = SmscTestServer
        self.config['port'] = self.service.listening.getHost().port
        self.transport = yield self.get_transport(self.config, start=False)
        self.transport.r_server = FakeRedis()
        self.expected_delivery_status = 'delivered'

    @inlineCallbacks
    def startTransport(self):
        self.transport._block_till_bind = defer.Deferred()
        yield self.transport.startWorker()

    @inlineCallbacks
    def tearDown(self):
        yield super(EsmeToSmscTestCasePlus, self).tearDown()
        self.transport.r_server.teardown()
        self.transport.factory.stopTrying()
        self.transport.factory.esme.transport.loseConnection()
        yield self.service.listening.stopListening()
        yield self.service.listening.loseConnection()

    @inlineCallbacks
    def test_handshake_submit_and_deliver(self):

        # 1111111111111111111111111111111111111111111111111
        expected_pdus_1 = [
            mk_expected_pdu("inbound", 1, "bind_transceiver"),
            mk_expected_pdu("outbound", 1, "bind_transceiver_resp"),
            mk_expected_pdu("inbound", 2, "enquire_link"),
            mk_expected_pdu("outbound", 2, "enquire_link_resp"),
        ]

        # 2222222222222222222222222222222222222222222222222
        expected_pdus_2 = [
            mk_expected_pdu("inbound", 3, "submit_sm"),
            mk_expected_pdu("outbound", 3, "submit_sm_resp"),
            # the delivery report
            mk_expected_pdu("outbound", 1, "deliver_sm"),
            mk_expected_pdu("inbound", 1, "deliver_sm_resp"),
        ]

        # 3333333333333333333333333333333333333333333333333
        expected_pdus_3 = [
            # a sms delivered by the smsc
            mk_expected_pdu("outbound", 555, "deliver_sm"),
            mk_expected_pdu("inbound", 555, "deliver_sm_resp"),
        ]

        # 4444444444444444444444444444444444444444444444444444444
        expected_pdus_4 = [
            # a sms delivered by the smsc
            #mk_expected_pdu("inbound", 4, "submit_sm"),
            #mk_expected_pdu("outbound", 4, "submit_sm_resp"),
            # the delivery report
            mk_expected_pdu("outbound", 666, "deliver_sm", **{'optional_parameters': {'tag': 'network_error_code', 'value': '04000'}}),
            mk_expected_pdu("inbound", 666, "deliver_sm_resp"),
        ]        

        ## Startup
        yield self.startTransport()
        yield self.transport._block_till_bind

        # First we make sure the Client binds to the Server
        # and enquire_link pdu's are exchanged as expected
        pdu_queue = self.service.factory.smsc.pdu_queue

        for expected_message in expected_pdus_1:
            actual_message = yield pdu_queue.get()
            self.assert_server_pdu(expected_message, actual_message)

        # Next the Client submits a SMS to the Server
        # and recieves an ack and a delivery_report

        msg = TransportUserMessage(
                to_addr="2772222222",
                from_addr="2772000000",
                content='hello world',
                transport_name=self.transport_name,
                transport_type='smpp',
                transport_metadata={},
                rkey='%s.outbound' % self.transport_name,
                timestamp='0',
                )
        yield self.dispatch(msg)

        for expected_message in expected_pdus_2:
            actual_message = yield pdu_queue.get()
            self.assert_server_pdu(expected_message, actual_message)

        # We need the user_message_id to check the ack
        user_message_id = msg.payload["message_id"]

        dispatched_events = self.get_dispatched_events()
        ack = dispatched_events[0].payload
        delv = dispatched_events[1].payload

        self.assertEqual(ack['message_type'], 'event')
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['transport_name'], self.transport_name)
        self.assertEqual(ack['user_message_id'], user_message_id)

        # We need the sent_message_id to check the delivery_report
        sent_message_id = ack['sent_message_id']

        self.assertEqual(delv['message_type'], 'event')
        self.assertEqual(delv['event_type'], 'delivery_report')
        self.assertEqual(delv['transport_name'], self.transport_name)
        self.assertEqual(delv['user_message_id'], user_message_id)
        self.assertEqual(delv['delivery_status'],
                         self.expected_delivery_status)

        # Finally the Server delivers a SMS to the Client

        pdu = DeliverSM(555,
                short_message="SMS from server",
                destination_addr="2772222222",
                source_addr="2772000000",
                )
        self.service.factory.smsc.send_pdu(pdu)

        for expected_message in expected_pdus_3:
            actual_message = yield pdu_queue.get()
            self.assert_server_pdu(expected_message, actual_message)

        dispatched_messages = self.get_dispatched_messages()
        mess = dispatched_messages[0].payload

        self.assertEqual(mess['message_type'], 'user_message')
        self.assertEqual(mess['transport_name'], self.transport_name)
        self.assertEqual(mess['content'], "SMS from server")
        self.assertEqual(mess['from_addr'], "+2772000000")

        dispatched_failures = self.get_dispatched_failures()
        self.assertEqual(dispatched_failures, [])
        
        self.clear_dispatched_messages()
        ## + adding
        pdu = DeliverSM(555,
                short_message="SMS from server",
                destination_addr="2772222222",
                source_addr="+2772000000",
                )
        self.service.factory.smsc.send_pdu(pdu)

        for expected_message in expected_pdus_3:
            actual_message = yield pdu_queue.get()
            self.assert_server_pdu(expected_message, actual_message)

        dispatched_messages = self.get_dispatched_messages()
        mess = dispatched_messages[0].payload

        self.assertEqual(mess['message_type'], 'user_message')
        self.assertEqual(mess['transport_name'], self.transport_name)
        self.assertEqual(mess['content'], "SMS from server")
        self.assertEqual(mess['from_addr'], "+2772000000")

        dispatched_failures = self.get_dispatched_failures()
        self.assertEqual(dispatched_failures, [])
        
        self.clear_dispatched_messages()        
        ## handle delivery error message from push
        #msg = TransportUserMessage(
            #to_addr="+++2772222222",
            #from_addr="2772000000",
            #content='This message will never arrived',
            #transport_name=self.transport_name,
            #transport_type='smpp',
            #transport_metadata={},
            #rkey='%s.outbound' % self.transport_name,
            #timestamp='0',
        #)
        #yield self.dispatch(msg)
    
        #for expected_message in expected_pdus_4:
            #actual_message = yield pdu_queue.get()
            #self.assert_server_pdu(expected_message, actual_message)
            
        ## We need the user_message_id to check the ack
        #user_message_id = msg.payload["message_id"]
    
        #dispatched_events = self.get_dispatched_events()
        ##self.assertEqual(2, len(dispatched_events))
        #ack = dispatched_events[2].payload
        #delv = dispatched_events[3].payload
        
        #self.assertEqual(ack['message_type'], 'event')
        #self.assertEqual(ack['event_type'], 'ack')
        #self.assertEqual(ack['transport_name'], self.transport_name)
        #self.assertEqual(ack['user_message_id'], user_message_id)
        
        ## We need the sent_message_id to check the delivery_report
        #sent_message_id = ack['sent_message_id']
        
        #self.assertEqual(delv['message_type'], 'event')
        #self.assertEqual(delv['event_type'], 'delivery_report')
        #self.assertEqual(delv['transport_name'], self.transport_name)
        #self.assertEqual(delv['user_message_id'], user_message_id)
        #self.assertEqual(delv['delivery_status'],
                         #self.expected_delivery_status)        
        
        
        pdu = DeliverSM(666,
                short_message=None,
                destination_addr="2772222222",
                source_addr="2772000000",
                )
        pdu._PDU__add_optional_parameter('network_error_code', '040000')
        self.service.factory.smsc.send_pdu(pdu)

        for expected_message in expected_pdus_4:
            actual_message = yield pdu_queue.get()
            self.assert_server_pdu(expected_message, actual_message)

        dispatched_messages = self.get_dispatched_messages()
        self.assertEqual(0, len(dispatched_messages))

        #self.assertEqual(mess['message_type'], 'user_message')
        #self.assertEqual(mess['transport_name'], self.transport_name)
        #self.assertEqual(mess['content'], "SMS from server")
        #self.assertEqual(mess['from_addr'], "+2772000000")

        dispatched_failures = self.get_dispatched_failures()
        self.assertEqual(dispatched_failures, [])        

    def send_out_of_order_multipart(self, smsc, to_addr, from_addr):
        destination_addr = to_addr
        source_addr = from_addr

        sequence_number = 1
        short_message1 = "\x05\x00\x03\xff\x03\x01back"
        pdu1 = DeliverSM(sequence_number,
                short_message=short_message1,
                destination_addr=destination_addr,
                source_addr=source_addr)

        sequence_number = 2
        short_message2 = "\x05\x00\x03\xff\x03\x02 at"
        pdu2 = DeliverSM(sequence_number,
                short_message=short_message2,
                destination_addr=destination_addr,
                source_addr=source_addr)

        sequence_number = 3
        short_message3 = "\x05\x00\x03\xff\x03\x03 you"
        pdu3 = DeliverSM(sequence_number,
                short_message=short_message3,
                destination_addr=destination_addr,
                source_addr=source_addr)

        smsc.send_pdu(pdu2)
        smsc.send_pdu(pdu3)
        smsc.send_pdu(pdu1)

    @inlineCallbacks
    def test_submit_and_deliver(self):

        self._block_till_bind = defer.Deferred()

        # Startup
        yield self.startTransport()
        yield self.transport._block_till_bind

        # Next the Client submits a SMS to the Server
        # and recieves an ack and a delivery_report

        msg = TransportUserMessage(
                to_addr="2772222222",
                from_addr="2772000000",
                content='hello world',
                transport_name=self.transport_name,
                transport_type='smpp',
                transport_metadata={},
                rkey='%s.outbound' % self.transport_name,
                timestamp='0',
                )
        yield self.dispatch(msg)

        # We need the user_message_id to check the ack
        user_message_id = msg.payload["message_id"]

        wait_for_events = self._amqp.wait_messages(
                "vumi",
                "%s.event" % self.transport_name,
                2,
                )
        yield wait_for_events

        dispatched_events = self.get_dispatched_events()
        ack = dispatched_events[0].payload
        delv = dispatched_events[1].payload

        self.assertEqual(ack['message_type'], 'event')
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['transport_name'], self.transport_name)
        self.assertEqual(ack['user_message_id'], user_message_id)

        # We need the sent_message_id to check the delivery_report
        sent_message_id = ack['sent_message_id']

        self.assertEqual(delv['message_type'], 'event')
        self.assertEqual(delv['event_type'], 'delivery_report')
        self.assertEqual(delv['transport_name'], self.transport_name)
        self.assertEqual(delv['user_message_id'], user_message_id)
        self.assertEqual(delv['delivery_status'],
                         self.expected_delivery_status)

        # Finally the Server delivers a SMS to the Client

        pdu = DeliverSM(555,
                short_message="SMS from server",
                destination_addr="2772222222",
                source_addr="2772000000",
                )
        self.service.factory.smsc.send_pdu(pdu)

        # Have the server fire of an out-of-order multipart sms
        self.send_out_of_order_multipart(self.service.factory.smsc,
                                         to_addr="2772222222",
                                         from_addr="2772000000")

        wait_for_inbound = self._amqp.wait_messages(
                "vumi",
                "%s.inbound" % self.transport_name,
                2,
                )
        yield wait_for_inbound

        dispatched_messages = self.get_dispatched_messages()
        mess = dispatched_messages[0].payload
        multipart = dispatched_messages[1].payload

        self.assertEqual(mess['message_type'], 'user_message')
        self.assertEqual(mess['transport_name'], self.transport_name)
        self.assertEqual(mess['content'], "SMS from server")

        # Check the incomming multipart is re-assembled correctly
        self.assertEqual(multipart['message_type'], 'user_message')
        self.assertEqual(multipart['transport_name'], self.transport_name)
        self.assertEqual(multipart['content'], "back at you")

        dispatched_failures = self.get_dispatched_failures()
        self.assertEqual(dispatched_failures, [])

    @inlineCallbacks
    def test_submit_and_deliver_with_missing_id_lookup(self):

        def r_failing_get(third_party_id):
            return None
        self.transport.r_get_id_for_third_party_id = r_failing_get

        self._block_till_bind = defer.Deferred()

        # Startup
        yield self.startTransport()
        yield self.transport._block_till_bind

        # Next the Client submits a SMS to the Server
        # and recieves an ack and a delivery_report

        msg = TransportUserMessage(
                to_addr="2772222222",
                from_addr="2772000000",
                content='hello world',
                transport_name=self.transport_name,
                transport_type='smpp',
                transport_metadata={},
                rkey='%s.outbound' % self.transport_name,
                timestamp='0',
                )
        yield self.dispatch(msg)

        # We need the user_message_id to check the ack
        user_message_id = msg.payload["message_id"]

        wait_for_events = self._amqp.wait_messages(
                "vumi",
                "%s.event" % self.transport_name,
                2,
                )
        yield wait_for_events

        dispatched_events = self.get_dispatched_events()
        ack = dispatched_events[0].payload
        delv = dispatched_events[1].payload

        self.assertEqual(ack['message_type'], 'event')
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['transport_name'], self.transport_name)
        self.assertEqual(ack['user_message_id'], user_message_id)

        # We need the sent_message_id to check the delivery_report
        sent_message_id = ack['sent_message_id']

        self.assertEqual(delv['message_type'], 'event')
        self.assertEqual(delv['event_type'], 'delivery_report')
        self.assertEqual(delv['transport_name'], self.transport_name)
        self.assertEqual(delv['user_message_id'], None)
        self.assertEqual(delv['transport_metadata']['message']['id'],
                                                    sent_message_id)
        self.assertEqual(delv['delivery_status'],
                         self.expected_delivery_status)

