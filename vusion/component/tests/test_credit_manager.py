import pymongo, json
from redis import Redis
from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.persist import UnattachSchedule
from vusion.component import CreditManager, CreditStatus, DialogueWorkerPropertyHelper

class CreditManagerTestCase(TestCase, ObjectMaker):
        
    def setUp(self):
        # setUp redis
        self.redis = Redis()
        self.cm_redis_key = 'unittest'
        # setUp mongodb
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        db = c.test_program_db
        self.history_collection = db.history
        self.schedules_collection = db.schedules
        self.clearData()

        #parameters:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)
        self.property_helper['timezone'] = 'Africa/Kampala'
        self.property_helper['credit-type'] = 'none'
        self.property_helper['credit-number'] = None
        self.property_helper['credit-from-date'] = None
        self.property_helper['credit-to-date'] = None
        
        self.cm = CreditManager(self.cm_redis_key, self.redis,
                                self.history_collection, self.schedules_collection,
                                self.property_helper)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.history_collection.drop()
        self.schedules_collection.drop()
        keys = self.redis.keys("%s:*" % self.cm_redis_key)
        for key in keys:
            self.redis.delete(key)        
            
    def assertCounter(self, expected):
        counter = self.redis.get("%s:creditmanager:count" % self.cm_redis_key)
        self.assertEqual(counter, expected)

    def test_no_limit(self):
        now = datetime.now()
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))
        self.property_helper['credit-type'] = 'none'
        self.cm.set_limit()
        self.assertTrue(self.cm.is_allowed('test'))

    def test_outgoing_limit_history(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))

        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '2'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)

        self.cm.set_limit()
        self.assertCounter('1')
        
        # first message should be granted
        self.assertTrue(self.cm.is_allowed(message_credits=1))
        self.assertCounter('2')
        
        # let add this last message to collection
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))        

        # second message should not
        self.assertFalse(self.cm.is_allowed(message_credits=1))
        self.assertFalse(self.cm.is_allowed(message_credits=1))
        self.assertCounter('2')

        # until the limit is increased
        self.property_helper['credit-number'] = '4'
        self.cm.set_limit()
        self.assertTrue(self.cm.is_allowed(message_credits=1))
        self.assertCounter('3')

    def test_sync_history_outgoing_only(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)
                
        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '4'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)
        self.cm.set_limit()        
   
        ## Count dialogue history
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1,
            interaction_id=1,
            direction='outgoing',
            timestamp=time_to_vusion_format(now)))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 1)
   
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1,
            interaction_id=1,
            direction='outgoing',
            timestamp=time_to_vusion_format(now), 
            message_credits=2))        
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 3)
        
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, 
            interaction_id=1, 
            direction="incoming", 
            timestamp=time_to_vusion_format(now), 
            message_credits=2))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 3)        
        
        ## Count unattached
        self.history_collection.save(self.mkobj_history_unattach(
            unattach_id=1,
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 5)

        ## Count request
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='outgoing',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 7)
                
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='incoming',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 7)

        ## Do not count marker
        self.history_collection.save(self.mkobj_history_one_way_marker(
            dialogue_id=1,
            interaction_id=1,
            timestamp=time_to_vusion_format(now)))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 7)

        ## Count previous version of history model without field message-credits
        self.history_collection.save({
            'object-type': 'unattach-history',
            'timestamp': time_to_vusion_format(now),
            'message-direction': 'outgoing',
            'message-status': 'delivered'})
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 8)

    def test_sync_history_outgoing_incoming(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)
        
        self.property_helper['credit-type'] = 'outgoing-incoming'
        self.property_helper['credit-number'] = '4'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)
        self.cm.set_limit()
        
        ## Count dialogue history
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1,
            interaction_id=1,
            direction='outgoing',
            timestamp=time_to_vusion_format(now)))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 1)
                   
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, 
            interaction_id=1, 
            direction="incoming", 
            timestamp=time_to_vusion_format(now), 
            message_credits=2))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 3)        
                    
        ## Count request
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='outgoing',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 5)
            
        self.history_collection.save(self.mkobj_history_request(
            request_id=1,
            message_direction='incoming',
            timestamp=time_to_vusion_format(now),
            message_credits=2))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 7)
        
        ## Do not count marker
        self.history_collection.save(self.mkobj_history_one_way_marker(
            dialogue_id=1,
            interaction_id=1,
            timestamp=time_to_vusion_format(now)))
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 7)

    def test_set_whitecard_unattach_schedule(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '2'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)
        self.cm.set_limit()

        schedule_first = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+1', unattach_id='1'))
        schedule_second = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+2', unattach_id='1'))
        # the frist shedule has already beend removed from the collection
        self.schedules_collection.save(schedule_second.get_as_dict())

        # first message should be granted
        self.assertTrue(
            self.cm.is_allowed(message_credits=1, schedule=schedule_first))
        
        # the whitecard didn't book the allowed space, so another message can still be send
        self.assertTrue(self.cm.is_allowed(message_credits=1))
        
        # At this point the manager start to reject message 
        self.assertFalse(self.cm.is_allowed(message_credits=1))
        
        # Except the one having a whitecard
        self.assertTrue(self.cm.is_allowed(message_credits=1, schedule=schedule_second))
        
        card = self.redis.get("%s:creditmanager:card:unattach-schedule:1" % self.cm_redis_key)
        self.assertEqual(card, 'white')
        

    def test_set_blackcard_unattach_schedule(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '2'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)
        self.cm.set_limit()
        
        schedule_first = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+1', unattach_id='1'))
        schedule_second = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+2', unattach_id='1'))
        self.schedules_collection.save(schedule_first.get_as_dict())
        self.schedules_collection.save(schedule_second.get_as_dict())
        
        # first message should not be granted as the total credit required is 4
        self.assertFalse(
            self.cm.is_allowed(message_credits=2, schedule=schedule_first))
        # Other message are still allowed
        self.assertTrue(
            self.cm.is_allowed(message_credits=1))
        # Still same origin unattach message are rejected
        self.assertFalse(
            self.cm.is_allowed(message_credits=2, schedule=schedule_second))

        self.assertTrue(self.cm.is_allowed(message_credits=1))
        # At this point the manager start to reject message 
        self.assertFalse(self.cm.is_allowed(message_credits=1))

        # The separate message should have a blackcard
        blackcard = self.redis.get("%s:creditmanager:card:unattach-schedule:1" % self.cm_redis_key)
        self.assertEqual(blackcard, 'black')

    def test_is_timeframed(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        more_past = past - timedelta(days=1)
        future = now + timedelta(days=1)
        more_future = future + timedelta(days=1)
        
        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '2'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past.date())
        self.property_helper['credit-to-date'] = time_to_vusion_format(future.date())
        self.cm.set_limit()
        
        self.assertTrue(self.cm.is_allowed(message_credits=1))
        self.property_helper.get_local_time = lambda v: time_to_vusion_format(more_future)
        self.assertFalse(self.cm.is_allowed(message_credits=1))
        self.property_helper.get_local_time = lambda v: time_to_vusion_format(more_past)
        self.assertFalse(self.cm.is_allowed(message_credits=1))
        
        ## current day of the to-date limit is allowed
        same_date_future = future + timedelta(minutes=1)
        self.property_helper.get_local_time = lambda v: time_to_vusion_format(same_date_future)
        self.assertTrue(self.cm.is_allowed(message_credits=1))

    def test_check_status_none(self):
        status = self.cm.check_status()
        self.assertIsInstance(status, CreditStatus)
        self.assertEqual(status['status'], 'none')
        redis_status = CreditStatus(**json.loads(self.redis.get("%s:creditmanager:status" % self.cm_redis_key)))
        self.assertEqual(status, redis_status)

    def test_check_status_no_credit_timeframe(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        more_past = past - timedelta(days=1)
        future = now + timedelta(days=1)
        more_future = future + timedelta(days=1)
        even_more_future = more_future + timedelta(days=1)
        
        ## no-credit-timeframe status
        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '2'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)
        self.cm.set_limit()
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'ok')
        redis_status = CreditStatus(**json.loads(self.redis.get("%s:creditmanager:status" % self.cm_redis_key)))
        self.assertEqual(status, redis_status)        
        
        self.property_helper['credit-from-date'] = time_to_vusion_format(future)
        self.property_helper['credit-to-date'] = time_to_vusion_format(more_future)
        self.property_helper.get_local_time = lambda v: time_to_vusion_format(now)        
        self.cm.set_limit()
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(now))

        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)
        self.property_helper.get_local_time = lambda v: time_to_vusion_format(now)        
        self.cm.set_limit()
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'ok')
        
        self.property_helper['credit-from-date'] = time_to_vusion_format(more_past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(past)
        self.property_helper.get_local_time = lambda v: time_to_vusion_format(now)        
        self.cm.set_limit()
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(now))
        
        ## even more future keep the time since the status changed
        self.property_helper.get_local_time = lambda v: time_to_vusion_format(future)        
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(now))
        
    def test_check_status_no_credit_timeframe(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        more_past = past - timedelta(days=1)
        future = now + timedelta(days=1)
        more_future = future + timedelta(days=1)
        even_more_future = more_future + timedelta(days=1)

        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '0'
        self.property_helper['credit-from-date'] = time_to_vusion_format(more_past.date())
        self.property_helper['credit-to-date'] = time_to_vusion_format(future.date())
        self.cm.set_limit()
        
        self.property_helper.get_local_time = lambda v: time_to_vusion_format(now)
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit')
        self.assertEqual(status['since'], time_to_vusion_format(now))
        
        self.property_helper.get_local_time = lambda v: time_to_vusion_format(now)
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit')
        self.assertEqual(status['since'], time_to_vusion_format(now))
        
        self.property_helper.get_local_time = lambda v: time_to_vusion_format(now)
        self.property_helper['credit-to-date'] = time_to_vusion_format(past.date())
        self.cm.set_limit()        
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(now))
