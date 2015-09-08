from pymongo import MongoClient
from bson import ObjectId

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper
from vusion.persist import UnattachedMessageManager, UnattachedMessage


class testUnattachedMessage(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_program_db'

        c = MongoClient(w=1)
        db = c.test_program_db
        self.manager = UnattachedMessageManager(db, 'unattached_messages')

        #parameters:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)
        self.property_helper['timezone'] = 'Africa/Kampala'
        self.manager.set_property_helper(self.property_helper)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.manager.drop()

    def test_get_unattached_message(self):
        unattached = self.mkobj_unattach_message()
        unattached_id = self.manager.save(unattached)

        saved_unattached = self.manager.get_unattached_message(unattached_id)
        self.assertTrue(isinstance(saved_unattached, UnattachedMessage))

    def test_get_unattached_message_not_present(self):
        saved_unattached = self.manager.get_unattached_message(ObjectId())
        self.assertEqual(None, saved_unattached)

    def test_get_unattached_messages(self):
        unattached_future = self.mkobj_unattach_message(fixed_time='3200-01-01T10:10:10')
        unattached_id_future = self.manager.save(unattached_future)

        unattached_future = self.mkobj_unattach_message(fixed_time='1200-01-01T10:10:10')
        self.manager.save(unattached_future)

        unattacheds = self.manager.get_unattached_messages()

        self.assertEqual(1, unattacheds.count())
        unattached = unattacheds.next()
        self.assertEqual(unattached_id_future, unattached['_id'])

    def test_get_unattached_message_selector_tag(self):
        unattached_geek = self.mkobj_unattach_message(
            send_to_type='match',
            send_to_match_operator='all',
            send_to_match_conditions=['geek'])
        unattached_geek_id = self.manager.save(unattached_geek)

        unattached = self.mkobj_unattach_message()
        self.manager.save(unattached)

        unattacheds = self.manager.get_unattached_messages_selector_tag('geek')
        self.assertEqual(1, unattacheds.count())
        unattached = unattacheds.next()
        self.assertEqual(unattached_geek_id, unattached['_id'])
