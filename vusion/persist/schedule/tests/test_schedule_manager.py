"""Test for vusion.persist.ScheduleManager"""
import pymongo
from datetime import timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper
from vusion.persist import (ScheduleManager, schedule_generator, 
                            ReminderSchedule, DialogueSchedule)
from vusion.utils import time_to_vusion_format


class TestScheduleManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        c.safe = True
        db = c[self.database_name]
        self.manager = ScheduleManager(db, 'schedules')
        self.clearData()

        #parameters:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)
        self.property_helper['timezone'] = 'Africa/Kampala'
        
        self.manager.set_property_helper(self.property_helper)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.manager.drop()

    def test_save_schedule(self):
        schedule = schedule_generator(**self.mkobj_schedule())
        self.manager.save_schedule(schedule)
        self.assertEqual(1, self.manager.count())

    def test_remove_schedule(self):
        schedule = schedule_generator(**self.mkobj_schedule())
        self.manager.save_schedule(schedule)
        self.manager.remove_schedule(schedule)
        self.assertEqual(0, self.manager.count())
    
    def test_remove_schedules(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='2'))
        self.manager.save_schedule(schedule_2)
        
        self.manager.remove_schedules('1')
        self.assertEqual(1, self.manager.count())
        self.assertEqual(1, self.manager.find({'participant-phone': '2'}).count())

    def test_remove_interaction(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='reminder-schedule',
            dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_2) 
        
        self.manager.remove_interaction('1', '1', '1')
        self.assertEqual(1, self.manager.count())
        self.assertEqual(1, self.manager.find({'object-type': 'reminder-schedule'}).count())

    def test_remove_reminders(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='deadline-schedule', 
            dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='reminder-schedule',
            dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_2) 
        
        self.manager.remove_reminders('1', '1', '1')
        self.assertEqual(1, self.manager.count())
        self.assertEqual(1, self.manager.find({'object-type': 'deadline-schedule'}).count())

    def test_remove_deadline(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='deadline-schedule', 
            dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='reminder-schedule',
            dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_2) 
        
        self.manager.remove_deadline('1', '1', '1')
        self.assertEqual(1, self.manager.count())
        self.assertEqual(1, self.manager.find({'object-type': 'reminder-schedule'}).count())

    def test_get_reminder_tail(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='reminder-schedule', dialogue_id='1', interaction_id='2'))
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='reminder-schedule', dialogue_id='1',interaction_id='3'))
        self.manager.save_schedule(schedule_1)
        self.manager.save_schedule(schedule_2)
        
        reminders = self.manager.get_reminder_tail('1', '1', '2')
        self.assertEqual(1, len(reminders))
        self.assertIsInstance(reminders[0], ReminderSchedule)

    def test_get_due_schedules(self):
        now = self.manager.get_local_time()
        past = now - timedelta(minutes=5)
        more_past = past - timedelta(minutes=5)
        
        schedule = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', date_time=time_to_vusion_format(past)))
        self.manager.save_schedule(schedule)
        schedule = schedule_generator(**self.mkobj_schedule(
            participant_phone='2', date_time=time_to_vusion_format(more_past)))
        self.manager.save_schedule(schedule)
        
        schedules = self.manager.get_due_schedules(limit=1)
        self.assertEqual(1, len(schedules))
        self.assertIsInstance(schedules[0], DialogueSchedule)
        self.assertEqual('2', schedules[0]['participant-phone'])

    def test_get_next_schedule_time(self):
        now = self.manager.get_local_time()
        future = now + timedelta(minutes=5)
        more_future = future + timedelta(minutes=5)

        schedule = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', date_time=time_to_vusion_format(future)))
        self.manager.save_schedule(schedule)
        schedule = schedule_generator(**self.mkobj_schedule(
            participant_phone='2', date_time=time_to_vusion_format(more_future)))
        self.manager.save_schedule(schedule)
        
        invalid_schedule = {'date-time':time_to_vusion_format(now)}
        self.manager.save(invalid_schedule)

        schedule_time = self.manager.get_next_schedule_time()
        self.assertTrue(future - schedule_time < timedelta(seconds=1))
        self.assertEqual(2, self.manager.count())
