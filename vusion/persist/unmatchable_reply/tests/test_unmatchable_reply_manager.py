import pymongo

from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.persist import UnmatchableReplyManager


class TestUnmatchableReplyManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_vusion_db'
        c = pymongo.Connection()
        c.safe = True
        db = c[self.database_name]
        self.manager = UnmatchableReplyManager(db, 'unmatchable_reply')
        self.clearData()

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.manager.drop()

    def test_get_older_date(self):
        now = datetime.now()
        past = now - timedelta(days=1)
        past_more = past - timedelta(days=1)
        past_more_more = past_more - timedelta(days=2)

        um = self.mkobj_unmatchable_reply(
            timestamp=now, to='256-8181')
        self.manager.save_document(um)
        
        um = self.mkobj_unmatchable_reply(
            timestamp=past, to='256-8181')
        self.manager.save_document(um)
        
        um = self.mkobj_unmatchable_reply(
            timestamp=past_more, to='256-8181')
        self.manager.save_document(um)

        um = self.mkobj_unmatchable_reply(
            timestamp=past_more_more, to='256-8181')
        self.manager.save_document(um)
        
        um = self.mkobj_unmatchable_reply(
            timestamp=past, to='256-8282')
        self.manager.save_document(um)
        
        date = self.manager.get_older_date(code='256-8181')
        self.assertEqual(date.date(), now.date())
        
        date = self.manager.get_older_date(now, code='256-8181')
        self.assertEqual(date.date(), past.date())
        
        date = self.manager.get_older_date(past_more, code='256-8181')
        self.assertEqual(date.date(), past_more_more.date())
        
        date = self.manager.get_older_date(past_more_more, code='256-8181')
        self.assertTrue(date is None)
        
        date = self.manager.get_older_date(code='256-8282')
        self.assertEqual(date.date(), past.date())   

    def test_count_day_credits(self):
        now = datetime.now()
        past = now - timedelta(hours=1)
        past_more = past - timedelta(days=1)
        
        um = self.mkobj_unmatchable_reply(
            timestamp=now, to="256-8181")
        self.manager.save_document(um)
        
        um = self.mkobj_unmatchable_reply(
            timestamp=now, to="256-8282")
        self.manager.save_document(um)

        um = self.mkobj_unmatchable_reply(
            timestamp=past,
            direction='outgoing',
            to="256-8181")
        self.manager.save_document(um)
        
        um = self.mkobj_unmatchable_reply(
            timestamp=past_more,
            to="256-8181")
        self.manager.save_document(um)

        self.assertEqual(
            {'incoming':1, 'outgoing': 1},
            self.manager.count_day_credits(now, code="256-8181"))
        
        self.assertEqual(
            {'incoming':1, 'outgoing': 0},
            self.manager.count_day_credits(past_more, "256-8181"))
