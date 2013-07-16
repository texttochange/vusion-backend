import pymongo, json
from redis import Redis
from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.persist import UnattachSchedule
from vusion.component import CreditManager, CreditStatus

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
        self.limit_type = 'none'
        self.limit_number = None
        self.limit_from_date = None
        self.limit_to_date = None
        
        self.cm = CreditManager(self.cm_redis_key, self.redis,
                                self.history_collection, self.schedules_collection,
                                self.limit_type, self.limit_number,
                                self.limit_from_date, self.limit_to_date)

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
        self.cm.set_limit('none')
        self.assertTrue(self.cm.is_allowed('test', now))

    def test_outgoing_limit_history(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))

        self.cm.set_limit(
            'outgoing-only', 
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))
        self.assertCounter('1')
        
        # first message should be granted
        self.assertTrue(self.cm.is_allowed(message_credits=1, local_time=now))
        self.assertCounter('2')
        
        # let add this last message to collection
        self.history_collection.save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))        

        # second message should not
        self.assertFalse(self.cm.is_allowed(message_credits=1, local_time=now))
        self.assertFalse(self.cm.is_allowed(message_credits=1, local_time=now))
        self.assertCounter('2')

        # until the limit is increased
        self.cm.set_limit(
            'outgoing', 
            limit_number=4, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))
        self.assertTrue(self.cm.is_allowed(message_credits=1, local_time=now))
        self.assertCounter('3')

    def test_sync_history_outgoing_only(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)
        
        self.cm.set_limit(
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
        
        self.cm.set_limit(
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

        self.cm.set_limit(
            'outgoing',
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))

        schedule_first = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+1', unattach_id='1'))
        schedule_second = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+2', unattach_id='1'))
        # the frist shedule has already beend removed from the collection
        self.schedules_collection.save(schedule_second.get_as_dict())

        # first message should be granted
        self.assertTrue(
            self.cm.is_allowed(message_credits=1, local_time=now, schedule=schedule_first))
        
        # the whitecard didn't book the allowed space, so another message can still be send
        self.assertTrue(self.cm.is_allowed(message_credits=1, local_time=now))
        
        # At this point the manager start to reject message 
        self.assertFalse(self.cm.is_allowed(message_credits=1, local_time=now))
        
        # Except the one having a whitecard
        self.assertTrue(self.cm.is_allowed(message_credits=1, local_time=now, schedule=schedule_second))
        
        blackcard = self.redis.get("%s:creditmanager:card:unattach-schedule:1" % self.cm_redis_key)
        self.assertEqual(blackcard, 'white')
        

    def test_set_blackcard_unattach_schedule(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.cm.set_limit(
            'outgoing',
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))
        
        schedule_first = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+1', unattach_id='1'))
        schedule_second = UnattachSchedule(
            **self.mkobj_schedule_unattach(participant_phone='+2', unattach_id='1'))
        self.schedules_collection.save(schedule_first.get_as_dict())
        self.schedules_collection.save(schedule_second.get_as_dict())
        
        # first message should not be granted as the total credit required is 4
        self.assertFalse(
            self.cm.is_allowed(message_credits=2, local_time=now, schedule=schedule_first))
        # Other message are still allowed
        self.assertTrue(
            self.cm.is_allowed(message_credits=1, local_time=now))
        # Still same origin unattach message are rejected
        self.assertFalse(
            self.cm.is_allowed(message_credits=2, local_time=now, schedule=schedule_second))

        self.assertTrue(self.cm.is_allowed(message_credits=1, local_time=now))
        # At this point the manager start to reject message 
        self.assertFalse(self.cm.is_allowed(message_credits=1, local_time=now))

        # The separate message should have a blackcard
        blackcard = self.redis.get("%s:creditmanager:card:unattach-schedule:1" % self.cm_redis_key)
        self.assertEqual(blackcard, 'black')

    def test_is_timeframed(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        more_past = past - timedelta(days=1)
        future = now + timedelta(days=1)
        more_future = future + timedelta(days=1)
        
        self.cm.set_limit(
            'outgoing-only', 
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))
        
        self.assertTrue(self.cm.is_allowed(message_credits=1, local_time=now))
        self.assertFalse(self.cm.is_allowed(message_credits=1, local_time=more_future))
        self.assertFalse(self.cm.is_allowed(message_credits=1, local_time=more_past))

    def test_check_status(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        more_past = past - timedelta(days=1)
        future = now + timedelta(days=1)
        more_future = future + timedelta(days=1)
        even_more_future = more_future + timedelta(days=1)

        status = self.cm.check_status(now)
        self.assertIsInstance(status, CreditStatus)
        self.assertEqual(status['status'], 'none')
        redis_status = CreditStatus(**json.loads(self.redis.get("%s:creditmanager:status" % self.cm_redis_key)))
        self.assertEqual(status, redis_status)
        
        self.cm.set_limit(
            'outgoing-only', 
            limit_number=2, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))
        status = self.cm.check_status(now)
        self.assertEqual(status['status'], 'ok')
        redis_status = CreditStatus(**json.loads(self.redis.get("%s:creditmanager:status" % self.cm_redis_key)))
        self.assertEqual(status, redis_status)        
        
        status = self.cm.check_status(more_past)
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(more_past))

        status = self.cm.check_status(now)
        self.assertEqual(status['status'], 'ok')
        
        status = self.cm.check_status(more_future)
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(more_future))
        
        ## even more future keep the time since the status changed
        status = self.cm.check_status(even_more_future)
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(more_future))
        
        self.cm.set_limit(
            'outgoing-only', 
            limit_number=0, 
            from_date=time_to_vusion_format(past),
            to_date=time_to_vusion_format(future))
        
        status = self.cm.check_status(now)
        self.assertEqual(status['status'], 'no-credit')
        self.assertEqual(status['since'], time_to_vusion_format(now))
        
        status = self.cm.check_status(future)
        self.assertEqual(status['status'], 'no-credit')
        self.assertEqual(status['since'], time_to_vusion_format(now))
        
        status = self.cm.check_status(more_future)
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(more_future))
