import pymongo
from datetime import datetime

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.persist import (CreditLog, ProgramCreditLogManager,
                            GarbageCreditLogManager)
from vusion.component import DialogueWorkerPropertyHelper


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

    def test_count(self):
        now = self.property_helper.get_local_time()
        self.assertEqual(0, self.clm.get_count(now))
        
        self.clm.increment_outgoing(2)
        self.assertEqual(2, self.clm.get_count(now))
        
        self.clm.increment_incoming(1)
        self.assertEqual(2, self.clm.get_count(now, count_type='outgoing-only'))
        self.assertEqual(3, self.clm.get_count(now))

        self.clm.increment_failed(2)
        self.assertEqual(3, self.clm.get_count(now))
        
        self.clm.property_helper['shortcode'] = '256-8282'
        self.clm.increment_outgoing(2)
        self.assertEqual(5, self.clm.get_count(now))


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
                                               count_type='outgoing-only',
                                               code='256-8181'))
        self.assertEqual(3, self.clm.get_count(now, code='256-8181'))

        self.clm.increment_failed(2, code='256-8181')
        self.assertEqual(3, self.clm.get_count(now, code='256-8181'))
        
        ## increment on another shortcode
        self.clm.increment_outgoing(2, code='256-8282')
        self.assertEqual(3, self.clm.get_count(now, code='256-8181'))
