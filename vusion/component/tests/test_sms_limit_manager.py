import pymongo
from redis import Redis
from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.persist import UnattachSchedule
from vusion.component.sms_limit_manager import SmsLimitManager

class SmsLimitManagerTestCase(TestCase, ObjectMaker):
        
    def setUp(self):
        # setUp redis
        self.redis = Redis()
        self.slm_redis_key = 'test:slm'
        # setUp mongodb
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        db = c.test_program_db
        self.history_collection = db.history
        self.schedules_collection = db.schedules
        self.clearData()

        #parameters:
        self.limit_type = 'none'
        self.limit_number = None
        self.limit_from_date = None
        self.limit_to_date = None
        
        self.slm = SmsLimitManager(self.slm_redis_key, self.redis,
                                   self.history_collection, self.schedules_collection,
                                   self.limit_type, self.limit_number,
                                   self.limit_from_date, self.limit_to_date)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.history_collection.drop()
        self.schedules_collection.drop()
        keys = self.redis.keys("test:slm:*")
        for key in keys:
            self.redis.delete(key)        

    def test_no_limit(self):
        now = datetime.now()
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))
        self.slm.set_limit('none')
        self.assertTrue(self.slm.is_allowed('test', now))

    def test_outgoing_limit_history(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))

        self.slm.set_limit(
            'outgoing-only', 
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))

        # first message should be granted
        self.assertTrue(self.slm.is_allowed(message_credits=1, local_time=now))

        # second message should not
        self.assertFalse(self.slm.is_allowed(message_credits=1, local_time=now))
        self.assertFalse(self.slm.is_allowed(message_credits=1, local_time=now))

        # until the limit is increased
        self.slm.set_limit(
            'outgoing', 
            limit_number=4, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))
        self.assertTrue(self.slm.is_allowed(message_credits=1, local_time=now))

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
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 1)
   
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1,
            interaction_id=1,
            direction='outgoing',
            timestamp=time_to_vusion_format(now), 
            message_credits=2))        
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 3)
        
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, 
            interaction_id=1, 
            direction="incoming", 
            timestamp=time_to_vusion_format(now), 
            message_credits=2))
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 3)        
        
        ## Count unattached
        self.history_collection.save(self.mkobj_history_unattach(
            unattach_id=1,
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 5)

        ## Count request
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='outgoing',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 7)
                
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='incoming',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 7)

        ## Do not count marker
        self.history_collection.save(self.mkobj_history_one_way_marker(
            dialogue_id=1,
            interaction_id=1,
            timestamp=time_to_vusion_format(now)))
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 7)

        ## Count previous version of history model without field message-credits
        self.history_collection.save({
            'object-type': 'unattach-history',
            'timestamp': time_to_vusion_format(now),
            'message-direction': 'outgoing',
            'message-status': 'delivered'})
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 8)
        
        

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
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 1)
                   
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, 
            interaction_id=1, 
            direction="incoming", 
            timestamp=time_to_vusion_format(now), 
            message_credits=2))
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 3)        
                    
        ## Count request
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='outgoing',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 5)
            
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='incoming',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 7)
        
        ## Do not count marker
        self.history_collection.save(self.mkobj_history_one_way_marker(
            dialogue_id=1,
            interaction_id=1,
            timestamp=time_to_vusion_format(now)))
        self.assertEqual(self.slm.get_used_credit_counter_mongo(), 7)

    def test_set_whitecard_unattach_schedule(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.slm.set_limit(
            'outgoing',
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))

        schedule_first = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+1'))
        schedule_second = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+2'))
        # the frist shedule has already beend removed from the collection
        self.schedules_collection.save(schedule_second.get_as_dict())

        # first message should be granted
        self.assertTrue(
            self.slm.is_allowed(message_credits=1, local_time=now, schedule=schedule_first))
        
        # the whitecard didn't book the allowed space, so another message can still be send
        self.assertTrue(self.slm.is_allowed(message_credits=1, local_time=now))
        
        # At this point the manager start to reject message 
        self.assertFalse(self.slm.is_allowed(message_credits=1, local_time=now))
        
        # Except the one having a whitecard
        self.assertTrue(self.slm.is_allowed(message_credits=1, local_time=now, schedule=schedule_second))

    def test_set_blackcard_unattach_schedule(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.slm.set_limit(
            'outgoing',
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))
        
        schedule_first = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+1'))
        schedule_second = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+2'))
        self.schedules_collection.save(schedule_first.get_as_dict())
        self.schedules_collection.save(schedule_second.get_as_dict())
        
        # first message should not be granted as the total credit required is 4
        self.assertFalse(
            self.slm.is_allowed(message_credits=2, local_time=now, schedule=schedule_first))

        # other message are still allowed
        self.assertTrue(self.slm.is_allowed(message_credits=1, local_time=now))
        # Still same origin unattach message are rejected
        self.assertFalse(
            self.slm.is_allowed(message_credits=2, local_time=now, schedule=schedule_second))

        self.assertTrue(self.slm.is_allowed(message_credits=1, local_time=now))
        # At this point the manager start to reject message 
        self.assertFalse(self.slm.is_allowed(message_credits=1, local_time=now))

    def test_is_timeframed(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        more_past = past - timedelta(days=1)
        future = now + timedelta(days=1)
        more_future = future + timedelta(days=1)
        
        self.slm.set_limit(
            'outgoing-only', 
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))
        
        self.assertTrue(self.slm.is_allowed(message_credits=1, local_time=now))
        self.assertFalse(self.slm.is_allowed(message_credits=1, local_time=more_future))
        self.assertFalse(self.slm.is_allowed(message_credits=1, local_time=more_past))