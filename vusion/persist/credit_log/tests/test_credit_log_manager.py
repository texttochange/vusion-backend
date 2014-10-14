import pymongo
from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.persist import (CreditLog, ProgramCreditLogManager,
                            GarbageCreditLogManager)
from vusion.component import DialogueWorkerPropertyHelper, PrintLogger


class TestProgramCreditLogManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.program_database_name = 'test_program_db'
        self.database_name = 'test_vusion_db'
        c = pymongo.Connection()
        c.safe = True
        db = c[self.database_name]
        self.clm = ProgramCreditLogManager(
            db,
            'credit_logs',
            self.program_database_name)
        self.clearData()
        
        #parameters:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)
        self.property_helper['timezone'] = 'Africa/Kampala'
        self.property_helper['shortcode'] = '256-8181'
        self.clm.set_property_helper(self.property_helper)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.clm.drop()

    def test_get_count(self):
        now = self.property_helper.get_local_time()        
        self.clm.increment_outgoing(2)
        self.assertEqual(2, self.clm.get_count(now, counters=['outgoing']))
        self.assertEqual(4, self.clm.get_count(now, counters=['outgoing', 'outgoing-pending']))
        self.assertEqual(2, self.clm.get_count(now, counters=['outgoing-pending']))        
        self.assertEqual(0, self.clm.get_count(now, counters=['outgoing-ack']))

    def test_increment(self):
        now = self.property_helper.get_local_time()
        self.assertEqual(0, self.clm.get_count(now))
        
        self.clm.increment_outgoing(2)
        self.assertEqual(2, self.clm.get_count(now))
        self.assertEqual(2, self.clm.get_count(now, counters=['outgoing-pending']))
        
        self.clm.increment_incoming(1)
        self.assertEqual(2, self.clm.get_count(now, counters=['outgoing']))
        self.assertEqual(3, self.clm.get_count(now))

        self.clm.increment_failed(2)
        self.assertEqual(3, self.clm.get_count(now))
        self.assertEqual(2, self.clm.get_count(now, counters=['outgoing-pending']))
        
        self.clm.property_helper['shortcode'] = '256-8282'
        self.clm.increment_outgoing(2)
        self.assertEqual(5, self.clm.get_count(now))

    def test_set(self):
        past = self.property_helper.get_local_time() - timedelta(days=1)
        past_more = past - timedelta(days=1)
        self.assertEqual(0, self.clm.get_count(past_more))
        
        self.clm.set_counters({'incoming': 2, 'outgoing': 3}, date=past)
        self.assertEqual(5, self.clm.get_count(past_more))

    # Is in reallity done by the front end
    def test_deleting_program(self):
        now = self.property_helper.get_local_time()
        past = now - timedelta(days=1)
        self.clm.set_counters({'incoming': 1, 'outgoing': 2}, date=now)
        self.clm.set_counters({'incoming': 2, 'outgoing': 2}, date=past)
        
        self.clm.deleting_program('My program name');
        
        c = self.clm.find({'object-type': 'deleted-program-credit-log'})
        self.assertEqual(2, c.count())
        for item in c:
            self.assertEqual(item['program-name'], "My program name")
            self.assertTrue('program-database' not in item)

    def test_increment_event_counter(self):
        now = self.property_helper.get_local_time()
        self.clm.set_counters(
            {'incoming': 0,
             'outgoing': 4,
             'outgoing-pending': 3,
             'outgoing-ack':2 },
            date=now)
        
        self.clm.increment_event_counter('pending', 'failed', 2)        
        credit_log = self.clm.find_one()
        self.assertEqual(1, credit_log['outgoing-pending'])
        self.assertEqual(2, credit_log['outgoing-failed'])
        
        self.clm.increment_event_counter('ack', 'failed', 1)
        credit_log = self.clm.find_one()
        self.assertEqual(1, credit_log['outgoing-pending'])
        self.assertEqual(3, credit_log['outgoing-failed'])
        
        self.clm.increment_event_counter('pending', 'delivered', 1)
        credit_log = self.clm.find_one()
        self.assertEqual(0, credit_log['outgoing-pending'])
        self.assertEqual(1, credit_log['outgoing-delivered'])


class TestGarbageCreditLogManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_vusion_db'
        c = pymongo.Connection()
        c.safe = True
        db = c[self.database_name]
        self.clm = GarbageCreditLogManager(
            db,
            'credit_logs')
        self.clearData()

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.clm.drop()

    def test_count(self):
        now = datetime.now()
        self.assertEqual(0, self.clm.get_count(now, code='256-8181'))
        
        self.clm.increment_outgoing(2, code='256-8181')
        self.assertEqual(2, self.clm.get_count(now, code='256-8181'))
        
        self.clm.increment_incoming(1, code='256-8181')
        self.assertEqual(2, self.clm.get_count(now,
                                               counters=['outgoing'],
                                               code='256-8181'))
        self.assertEqual(3, self.clm.get_count(now, code='256-8181'))

        self.clm.increment_failed(2, code='256-8181')
        self.assertEqual(3, self.clm.get_count(now, code='256-8181'))
        
        ## increment on another shortcode
        self.clm.increment_outgoing(2, code='256-8282')
        self.assertEqual(3, self.clm.get_count(now, code='256-8181'))

    def test_set(self):
        past = datetime.now() - timedelta(days=1)
        past_more = past - timedelta(days=1)
        self.assertEqual(0, self.clm.get_count(past_more, code='256-8181'))
        
        self.clm.set_counters({'incoming': 2, 'outgoing': 3}, code='256-8181', date=past)
        self.assertEqual(5, self.clm.get_count(past_more, code='256-8181'))
