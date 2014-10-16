from bson import ObjectId
from redis import Redis

from twisted.trial.unittest import TestCase

from vusion.component import FlyingMessageManager


class FlyingMessageTestCase(TestCase):
    
    def setUp(self):
        self.redis = Redis()
        self.prefix_key = 'unittest:testprogram'
        
        self.fm = FlyingMessageManager(
            self.prefix_key,
            self.redis)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        keys = self.redis.keys("%s:*" % self.prefix_key)
        for key in keys:
            self.redis.delete(key)

    def test_append_get(self):
        history_id = ObjectId()
        self.fm.append_message_data('1', history_id, 3, 'ack')
        saved_history_id, credit, status = self.fm.get_message_data('1')
        self.assertEqual(history_id, saved_history_id)
        self.assertEqual(credit, 3)
        self.assertEqual(status, 'ack')

    def test_append_get_not_present(self):
        saved_history_id, credit = self.fm.get_message_data('1')
        self.assertTrue(saved_history_id is None)
        self.assertTrue(credit == 0)