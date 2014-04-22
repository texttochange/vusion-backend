import pymongo

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
