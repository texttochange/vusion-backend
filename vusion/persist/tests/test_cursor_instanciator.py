import pymongo
from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker
from vusion.error import VusionError
from vusion.persist import Participant, schedule_generator, DialogueSchedule
from vusion.persist.cursor_instanciator import CursorInstanciator

class TestCursorInstanciator(TestCase, ObjectMaker):

    def setUp(self):
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        db = c.test_program_db
        self.test_collection = db.test_collection
        self.clearData()

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.test_collection.drop()

    def test_with_class(self):
        self.test_collection.save(self.mkobj_participant())
        cursor = CursorInstanciator(self.test_collection.find(), Participant)
        self.assertEqual(1, cursor.count())
        for participant in cursor:
            self.assertIsInstance(participant, Participant)
    
    def test_with_generator(self):
        self.test_collection.save(self.mkobj_schedule())
        cursor = CursorInstanciator(self.test_collection.find().limit(1), schedule_generator)
        self.assertEqual(1, cursor.count())
        for schedule in cursor:
            self.assertIsInstance(schedule, DialogueSchedule)

    #this is going to fail when instanciate a participant item with schedule_generator
    def test_failure_callbacks(self):
        self.test_collection.save(self.mkobj_participant())
        cursor = CursorInstanciator(self.test_collection.find(), schedule_generator)
        
        def failed_callback(exception, item):
            self.assertEqual('participant', item['object-type'])
            self.assertIsInstance(exception, VusionError)
            
        cursor.add_failure_callback(failed_callback)
        self.assertEqual(1, cursor.count())
        participant = cursor.next()
        self.assertTrue(participant is None)
