import json
from pymongo import MongoClient
from redis import Redis
from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker, DumbLogManager
from vusion.utils import (time_to_vusion_format, time_from_vusion_format,
                          time_to_vusion_format_date)
from vusion.persist import (HistoryManager, ScheduleManager, UnattachSchedule,
                            ProgramCreditLogManager, Participant)
from vusion.component import (CreditManager, CreditStatus, 
                              DialogueWorkerPropertyHelper)


class CreditManagerTestCase(TestCase, ObjectMaker):

    def setUp(self):
        # setUp redis
        self.redis = Redis()
        self.cm_redis_key = 'unittest'
        # setUp mongodb
        self.vusion_database_name = 'test_vusion_db'
        self.database_name = 'test_program_db'
        c = MongoClient(w=1)
        db = c[self.database_name]
        self.collections = {}
        self.collections['history'] = HistoryManager(db, 'history', self.cm_redis_key, self.redis)
        self.collections['schedules'] = ScheduleManager(db, 'schedules')
        db = c[self.vusion_database_name]
        self.collections['credit_logs'] = ProgramCreditLogManager(
            db, 'credit_logs', self.database_name)
        self.clearData()

        #properties:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)
        self.property_helper['timezone'] = 'Africa/Kampala'
        self.property_helper['credit-type'] = 'none'
        self.property_helper['credit-number'] = None
        self.property_helper['credit-from-date'] = None
        self.property_helper['credit-to-date'] = None
        self.property_helper['shortcode'] = '256-8181'

        for collection in self.collections:
            self.collections[collection].set_property_helper(self.property_helper)
            self.collections[collection].set_log_helper(DumbLogManager())
            
        #Initialie manager
        self.cm = CreditManager(self.cm_redis_key, self.redis,
                                self.collections['credit_logs'],
                                self.collections['history'], 
                                self.collections['schedules'],
                                self.property_helper, DumbLogManager())
        test_participant = {
            "model-version": "5", 
            "object-type": "participant", 
            "phone": "+255654033486", 
            "session-id": "ee29e5a2321f426cb52f19e1371cb32e", 
            "last-optin-date": "2012-11-20T13:30:56",
            "last-optout-date": "2012-11-20T14:00:00",
            "enrolled": [ ],
            "tags": [ ],
            "profile": [ ],
            "transport_metadata": [],
            "simulate": False} 
        self.p = Participant(**test_participant)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.collections['history'].drop()
        self.collections['schedules'].drop()
        self.collections['credit_logs'].drop()
        keys = self.redis.keys("%s:*" % self.cm_redis_key)
        for key in keys:
            self.redis.delete(key) 

    def assertCounter(self, expected):
        counter = self.redis.get("%s:creditmanager:count" % self.cm_redis_key)
        self.assertEqual(counter, expected)

    def test_no_limit(self):
        now = datetime.now()
        self.collections['history'].save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))
        self.property_helper['credit-type'] = 'none'
        self.cm.set_limit()
        self.assertTrue(self.cm.is_allowed(1, participant=self.p))
        # Event without limit the credit logs should be increased
        self.assertEqual(1, self.collections['credit_logs'].count())

    def test_outgoing_limit_history(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        future = now + timedelta(days=1)

        self.collections['credit_logs'].increment_outgoing(1)

        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '2'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)

        self.cm.set_limit()
        self.assertCounter('1')

        # first message should be granted
        self.assertTrue(self.cm.is_allowed(message_credits=1, participant=self.p))
        self.assertCounter('2')

        # let add this last message to collection
        self.collections['history'].save(self.mkobj_history_dialogue(
            dialogue_id=1, interaction_id=1, timestamp=time_to_vusion_format(now)))

        # second message should not
        self.assertFalse(self.cm.is_allowed(message_credits=1, participant=self.p))
        self.assertFalse(self.cm.is_allowed(message_credits=1, participant=self.p))
        self.assertCounter('2')

        # until the limit is increased
        self.property_helper['credit-number'] = '4'
        self.cm.set_limit()
        self.assertTrue(self.cm.is_allowed(message_credits=1, participant=self.p))
        self.assertCounter('3')

    def test_sync_history_outgoing_only(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        more_past = past - timedelta(days=1)
        future = now + timedelta(days=1)
        more_future = future + timedelta(days=1)

        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '4'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)
        self.cm.set_limit()

        ## Count dialogue history
        credit_log = self.mkobj_program_credit_log(now,
                                                   program_database=self.database_name)        
        self.collections['credit_logs'].save_document(credit_log)
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 1)

        credit_log = self.mkobj_program_credit_log(past,
                                                   program_database=self.database_name)        
        self.collections['credit_logs'].save_document(credit_log)
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 2)


    def test_sync_history_outgoing_incoming(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        more_past = past - timedelta(days=1)
        future = now + timedelta(days=1)
        more_future = future + timedelta(days=1)
        
        self.property_helper['credit-type'] = 'outgoing-incoming'
        self.property_helper['credit-number'] = '4'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)
        self.cm.set_limit()

        ## Out of the timeframe histories
        credit_log = self.mkobj_program_credit_log(
            more_past,
            program_database=self.database_name)
        self.collections['credit_logs'].save_document(credit_log)
        credit_log = self.mkobj_program_credit_log(
            more_future,
            program_database=self.database_name)
        self.collections['credit_logs'].save_document(credit_log)
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 0)

        ## Count dialogue history
        credit_log = self.mkobj_program_credit_log(
            now,
            program_database=self.database_name)
        self.collections['credit_logs'].save_document(credit_log)
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 2)

        credit_log = self.mkobj_program_credit_log(
            past,
            program_database=self.database_name)
        self.collections['credit_logs'].save_document(credit_log)
        self.assertEqual(self.cm.get_used_credit_counter_mongo(), 4)


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
        self.collections['schedules'].save(schedule_second.get_as_dict())

        # first message should be granted
        self.assertTrue(
            self.cm.is_allowed(message_credits=1, participant=self.p, schedule=schedule_first))

        # the whitecard didn't book the allowed space, so another message can still be send
        self.assertTrue(self.cm.is_allowed(message_credits=1, participant=self.p))

        # At this point the manager start to reject message 
        self.assertFalse(self.cm.is_allowed(message_credits=1, participant=self.p))

        # Except the one having a whitecard
        self.assertTrue(self.cm.is_allowed(message_credits=1, participant=self.p, schedule=schedule_second))

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
        self.collections['schedules'].save(schedule_first.get_as_dict())
        self.collections['schedules'].save(schedule_second.get_as_dict())

        # first message should not be granted as the total credit required is 4
        self.assertFalse(
            self.cm.is_allowed(message_credits=2, participant=self.p, schedule=schedule_first))
        # Other message are still allowed
        self.assertTrue(
            self.cm.is_allowed(message_credits=1, participant=self.p))
        # Still same origin unattach message are rejected
        self.assertFalse(
            self.cm.is_allowed(message_credits=2, participant=self.p, schedule=schedule_second))

        self.assertTrue(self.cm.is_allowed(message_credits=1, participant=self.p))
        # At this point the manager start to reject message 
        self.assertFalse(self.cm.is_allowed(message_credits=1, participant=self.p))

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
        self.property_helper['credit-number'] = '4'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past.date())
        self.property_helper['credit-to-date'] = time_to_vusion_format(future.date())
        self.cm.set_limit()

        self.assertTrue(self.cm.is_allowed(message_credits=1, participant=self.p))
        self.property_helper.get_local_time = lambda v: more_future
        self.assertFalse(self.cm.is_allowed(message_credits=1, participant=self.p))
        self.property_helper.get_local_time = lambda v: more_past
        self.assertFalse(self.cm.is_allowed(message_credits=1, participant=self.p))
        
        ## current day of the to-date limit is allowed
        same_date_future = future + timedelta(minutes=1)
        self.property_helper.get_local_time = lambda v: same_date_future
        self.assertTrue(self.cm.is_allowed(message_credits=1, participant=self.p))

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
        self.property_helper.get_local_time = lambda v: now        
        self.cm.set_limit()
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(now))

        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)
        self.property_helper.get_local_time = lambda v: now        
        self.cm.set_limit()
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'ok')
        
        self.property_helper['credit-from-date'] = time_to_vusion_format(more_past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(past)
        self.property_helper.get_local_time = lambda v: now        
        self.cm.set_limit()
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(now))
        
        ## even more future keep the time since the status changed
        self.property_helper.get_local_time = lambda v: future        
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(now))
        
    def test_check_status_no_credit_timeframe_2(self):
        now = self.property_helper.get_local_time()
        past = now - timedelta(days=1)
        more_past = past - timedelta(days=1)
        future = now + timedelta(days=1)

        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '0'
        self.property_helper['credit-from-date'] = time_to_vusion_format(more_past.date())
        self.property_helper['credit-to-date'] = time_to_vusion_format(future.date())
        self.cm.set_limit()
        
        self.property_helper.get_local_time = lambda v: now
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit')
        self.assertEqual(status['since'], time_to_vusion_format(now))

        self.property_helper.get_local_time = lambda v: now
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit')
        self.assertEqual(status['since'], time_to_vusion_format(now))
        
        self.property_helper.get_local_time = lambda v: now
        self.property_helper['credit-to-date'] = time_to_vusion_format(past.date())
        self.cm.set_limit()
        self.property_helper.get_local_time = lambda v: now
        
        status = self.cm.check_status()
        self.assertEqual(status['status'], 'no-credit-timeframe')
        self.assertEqual(status['since'], time_to_vusion_format(now))

    def test_simulated_participant_no_credit_count(self):        
        test_participant = {
            "model-version": "5", 
            "object-type": "participant", 
            "phone": "+255654033486", 
            "session-id": "ee29e5a2321f426cb52f19e1371cb32e", 
            "last-optin-date": "2012-11-20T13:30:56",
            "last-optout-date": "2012-11-20T14:00:00",
            "enrolled": [ ],
            "tags": [ ],
            "profile": [ ],
            "transport_metadata": [],
            "simulate": True} 
        p = Participant(**test_participant)
        now = datetime.now()
        past = now - timedelta(days=1)
        more_past = past - timedelta(days=1)
        future = now + timedelta(days=1)
        more_future = future + timedelta(days=1)

        self.property_helper['credit-type'] = 'outgoing-only'
        self.property_helper['credit-number'] = '4'
        self.property_helper['credit-from-date'] = time_to_vusion_format(past)
        self.property_helper['credit-to-date'] = time_to_vusion_format(future)
        self.cm.set_limit()
        self.assertTrue(
            self.cm.is_allowed(message_credits=1,participant=p, schedule=None))
        # Event without limit the credit logs should be increased
        self.assertEqual(0, self.collections['credit_logs'].count())
