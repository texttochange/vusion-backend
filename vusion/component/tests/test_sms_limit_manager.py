import redis
import pymongo
from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.component.sms_limit_manager import SmsLimitManager

class SmsLimitManagerTestCase(TestCase, ObjectMaker):
        
    def setUp(self):
        # setUp redis
        self.redis = redis.StrictRedis()
        self.slm_redis_key = 'test:slm'
        # setUp mongodb
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        db = c.test_program_db
        self.history_collection = db.history

        self.clearData()

        #parameters:
        self.limit_type = 'none'
        self.limit_number = None
        self.limit_from_date = None
        self.limit_to_date = None
        
        self.slm = SmsLimitManager(self.slm_redis_key, self.redis,
                                   self.history_collection, self.limit_type, 
                                   self.limit_number, self.limit_from_date,
                                   self.limit_to_date)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.history_collection.drop()
        self.redis.delete("test:slm:smslimit:count")        

    def test_no_limit(self):
        now = datetime.now()
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))
        self.slm.set_limit('none')
        self.assertTrue(self.slm.is_allowed('test'))

    def test_outgoing_limit_history(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))

        self.slm.set_limit(
            'outgoing', 
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))

        # first message should be granted
        self.assertTrue(self.slm.is_allowed(message_credits=1))

        # second message should not
        self.assertFalse(self.slm.is_allowed(message_credits=1))
        self.assertFalse(self.slm.is_allowed(message_credits=1))

        # until the limit is increased
        self.slm.set_limit(
            'outgoing', 
            limit_number=4, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))
        self.assertTrue(self.slm.is_allowed(message_credits=1))

    def test_sync_history_outgoing_only(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)
        
        self.slm.set_limit(
            'outgoing-only', 
            limit_number=4, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))        
   
        ## Count dialogue history
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1,
            interaction_id=1,
            direction='outgoing',
            timestamp=time_to_vusion_format(now)))
        self.assertEqual(self.slm.sync_data(), 1)
   
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1,
            interaction_id=1,
            direction='outgoing',
            timestamp=time_to_vusion_format(now), 
            message_credits=2))        
        self.assertEqual(self.slm.sync_data(), 3)
        
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, 
            interaction_id=1, 
            direction="incoming", 
            timestamp=time_to_vusion_format(now), 
            message_credits=2))
        self.assertEqual(self.slm.sync_data(), 3)        
        
        ## Count unattached
        self.history_collection.save(self.mkobj_history_unattach(
            unattach_id=1,
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.slm.sync_data(), 5)

        ## Count request
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='outgoing',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.slm.sync_data(), 7)
        
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='incoming',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.slm.sync_data(), 7)

        ## Do not count marker
        self.history_collection.save(self.mkobj_history_one_way_marker(
            dialogue_id=1,
            interaction_id=1,
            timestamp=time_to_vusion_format(now)))
        self.assertEqual(self.slm.sync_data(), 7)

    def test_sync_history_outgoing_incoming(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)
        
        self.slm.set_limit(
            'outgoing-incoming', 
            limit_number=4, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))        
        
        ## Count dialogue history
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1,
            interaction_id=1,
            direction='outgoing',
            timestamp=time_to_vusion_format(now)))
        self.assertEqual(self.slm.sync_data(), 1)
                   
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, 
            interaction_id=1, 
            direction="incoming", 
            timestamp=time_to_vusion_format(now), 
            message_credits=2))
        self.assertEqual(self.slm.sync_data(), 3)        
                    
        ## Count request
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='outgoing',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.slm.sync_data(), 5)
            
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='incoming',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.slm.sync_data(), 7)
        
        ## Do not count marker
        self.history_collection.save(self.mkobj_history_one_way_marker(
            dialogue_id=1,
            interaction_id=1,
            timestamp=time_to_vusion_format(now)))
        self.assertEqual(self.slm.sync_data(), 7)

    def test_set_wildcard(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.slm.set_limit(
            'outgoing', 
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))

        # first message should be granted
        wildcard = self.slm.is_allowed_set_wildcard(
            message_credits=1,
            recipient_count=2,
            source_type='unattach-id',
            source_id='1')
        self.assertTrue(wildcard)
        
        ## only the wildcard source type and are allowed
        self.assertFalse(self.slm.is_allowed(message_credits=1))
        self.assertFalse(self.slm.is_allowed(message_credits=1, 
                                             source_type='unattach-id',
                                             source_id='2'))
        self.assertTrue(self.slm.is_allowed(message_credits=1, 
                                            source_type='unattach-id',
                                            source_id='1'))
        self.assertTrue(self.slm.is_allowed(message_credits=1, 
                                            source_type='unattach-id',
                                            source_id='1'))