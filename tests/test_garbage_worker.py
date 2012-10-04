
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

import pymongo

from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker, UTCNearNow

from tests.utils import MessageMaker
from tests.utils import ObjectMaker
from vusion import GarbageWorker


class GarabageWorkerTestCase(TestCase, MessageMaker, ObjectMaker):

    @inlineCallbacks
    def setUp(self):
        self.config = {
            'database_name': 'test',
            'application_name': 'garbage',
            'transport_name': 'garbage',
            'mongodb_host': 'localhost',
            'mongodb_port': 27017}

        connection = pymongo.Connection('localhost', 27017)
        db = connection[self.config['database_name']]
        self.unmatchable_replies_collection = db['unmatchable_reply']
        self.unmatchable_replies_collection.drop()
        self.templates_collection = db['templates']
        self.templates_collection.drop()
        self.shortcodes_collection = db['shortcodes']
        self.shortcodes_collection.drop()

        self.worker = get_stubbed_worker(GarbageWorker,
                                         config=self.config)
        self.broker = self.worker._amqp_client.broker
        self.worker.startService()
        yield self.worker.startWorker()

    def tearDown(self):
        self.broker.dispatched = {}
        self.unmatchable_replies_collection.drop()
        self.shortcodes_collection.drop()
        self.templates_collection.drop()

    @inlineCallbacks
    def send(self, msg):
        self.broker.publish_message(
            'vumi', '%s.inbound' % self.config['transport_name'], msg)
        yield self.broker.kick_delivery()

    @inlineCallbacks
    def test_receive_user_message_without_error_template(self):
        msg = self.mkmsg_in(to_addr='8282')
        self.shortcodes_collection.save(self.mkobj_shortcode(code = '8282'))

        yield self.send(msg)

        self.assertTrue(self.unmatchable_replies_collection.find_one())
        messages = self.broker.get_messages('vumi', 'garbage.outbound')
        self.assertEqual(len(messages), 0)

    @inlineCallbacks
    def test_receive_user_message_with_one_error_template_matching_to_addr(self):
        msg = self.mkmsg_in(content='Gen 2', to_addr='8181')
        template_id = self.templates_collection.save(
            self.mkobj_template_unmatching_keyword()
        )
        self.shortcodes_collection.save(self.mkobj_shortcode(template_id))

        yield self.send(msg)

        self.assertTrue(self.unmatchable_replies_collection.find_one())
        messages = self.broker.get_messages('vumi', 'garbage.outbound')
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['content'],
                         'Gen does not match any keyword.')
        self.assertEqual(messages[0]['to_addr'], msg['from_addr'])
    
    @inlineCallbacks
    def test_receive_user_message_with_two_error_template_matching_to_addr(self):
        msg = self.mkmsg_in(content='Gen 2', to_addr='8282')
        template_1_id = self.templates_collection.save(
            self.mkobj_template_unmatching_keyword()
        )
        shortcode_1 = self.mkobj_shortcode(template_1_id, '8181')
        self.shortcodes_collection.save(shortcode_1)
        
        template_2_id = self.templates_collection.save(
            self.mkobj_template_unmatching_keyword(message="KEYWORD is not good.")
        )
        shortcode_2 = self.mkobj_shortcode(template_2_id, '8282')
        self.shortcodes_collection.save(shortcode_2)

        yield self.send(msg)

        self.assertTrue(self.unmatchable_replies_collection.find_one())
        messages = self.broker.get_messages('vumi', 'garbage.outbound')
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['content'],
                         'Gen is not good.')
        self.assertEqual(messages[0]['to_addr'], msg['from_addr'])
