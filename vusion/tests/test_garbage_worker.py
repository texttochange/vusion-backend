from pymongo import MongoClient

from datetime import datetime

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportUserMessage
from vumi.tests.utils import VumiTestCase, UTCNearNow
from vumi.application.tests.helpers import ApplicationHelper

from tests.utils import MessageMaker, ObjectMaker
from vusion.persist import (UnmatchableReplyManager, ShortcodeManager,
                            GarbageCreditLogManager, TemplateManager)
from vusion import GarbageWorker


class GarabageWorkerTestCase(VumiTestCase, MessageMaker, ObjectMaker):

    @inlineCallbacks
    def setUp(self):
        self.config = {
            'database_name': 'garbage_test',
            'mongodb_host': 'localhost',
            'mongodb_port': 27017,
            'mongodb_safe': True}
        
        self.app_helper = self.add_helper(ApplicationHelper(GarbageWorker))
        self.worker = yield self.app_helper.get_application(self.config)

        mongo_client = MongoClient('localhost', 27017, w=1)
        mongo_client.write_concern['j'] = True
        db = mongo_client[self.config['database_name']]
        self.collections = {}
        self.collections['unmatchable_reply'] = UnmatchableReplyManager(
            db, 'unmatchable_reply')
        self.collections['template'] = TemplateManager(
            db, 'templates')
        self.collections['shortcode'] = ShortcodeManager(
            db,'shortcodes')
        self.collections['credit_log'] = GarbageCreditLogManager(
            db, 'credit_logs')

    @inlineCallbacks
    def tearDown(self):
        self.clearData()
        yield super(GarabageWorkerTestCase, self).tearDown()

    def clearData(self):
        for collection in self.collections.itervalues():
            collection.drop()

    @inlineCallbacks
    def test_receive_user_message_without_error_template(self):
        now = datetime.now()

        msg = self.mkmsg_in(to_addr='8282')
        self.collections['shortcode'].save(
            self.mkobj_shortcode(code='8282', international_prefix='256'))

        yield self.app_helper.dispatch_inbound(msg)

        self.assertEqual(1, self.collections['unmatchable_reply'].count())
        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 0)
        self.assertEqual(1, self.collections['credit_log'].get_count(now, code='256-8282'))

    @inlineCallbacks
    def test_receive_user_message_with_one_error_template_matching_to_addr(self):
        now = datetime.now()
        participant_transport_metadata = {'some_key': 'some_value'}
        msg = self.mkmsg_in(content='Gen 2', to_addr='8282', transport_metadata=participant_transport_metadata)
        template_id = self.collections['template'].save(
            self.mkobj_template_unmatching_keyword()
        )
        self.collections['shortcode'].save(
            self.mkobj_shortcode(code='8282', international_prefix='256', error_template=template_id))

        yield self.app_helper.dispatch_inbound(msg)

        self.assertTrue(2, self.collections['unmatchable_reply'].count())
        self.assertEqual(2, self.collections['credit_log'].get_count(now, code='256-8282'))
        messages = yield self.app_helper.wait_for_dispatched_outbound(1)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['content'],
                         'Gen does not match any keyword.')
        self.assertEqual(messages[0]['to_addr'], msg['from_addr'])
        self.assertEqual(messages[0]['from_addr'], '256-8282')
        self.assertEqual(messages[0]['transport_metadata'], participant_transport_metadata)

    @inlineCallbacks
    def test_receive_user_message_with_two_error_template_matching_to_addr(self):
        msg = self.mkmsg_in(content='Gen 2', to_addr='8282')
        template_1_id = self.collections['template'].save(
            self.mkobj_template_unmatching_keyword())
        shortcode_1 = self.mkobj_shortcode(error_template=template_1_id, code='8181')
        self.collections['shortcode'].save(shortcode_1)

        template_2_id = self.collections['template'].save(
            self.mkobj_template_unmatching_keyword(message="KEYWORD is not good."))
        shortcode_2 = self.mkobj_shortcode(error_template=template_2_id, code='8282')
        self.collections['shortcode'].save(shortcode_2)

        yield self.app_helper.dispatch_inbound(msg)

        self.assertEqual(2, self.collections['unmatchable_reply'].count())
        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['content'],
                         'Gen is not good.')
        self.assertEqual(messages[0]['to_addr'], msg['from_addr'])
