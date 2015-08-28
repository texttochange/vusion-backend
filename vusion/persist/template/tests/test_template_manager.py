from pymongo import MongoClient

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.persist import TemplateManager


class TestTemplateManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_vusion_db'
        c = MongoClient(w=1)
        db = c[self.database_name]
        self.manager = TemplateManager(db, 'templates')
        self.clearData()

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.manager.drop()
