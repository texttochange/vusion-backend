"""Test for vusion.persist.ScheduleManager"""
import pymongo
from datetime import timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper
from vusion.persist import (ScheduleManager, schedule_generator, 
                            ReminderSchedule, DialogueSchedule,
                            UnattachSchedule, UnattachMessage,
                            Participant)
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.error import InvalidField


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
        sometime = time_from_vusion_format('2014-10-02T10:00:00')
        schedule = schedule_generator(**self.mkobj_schedule(date_time=sometime))
        self.manager.save_schedule(schedule)
        self.assertEqual(1, self.manager.count())
        saved_schedule = schedule_generator(**self.manager.find_one())
        self.assertEqual('2014-10-02T10:00:00', saved_schedule['date-time'])

    def test_save_schedule_fail(self):
        sometime = time_from_vusion_format('2014-10-02T10:00:00')
        schedule = schedule_generator(**self.mkobj_schedule())
        schedule['date-time'] = sometime
        try:
            self.manager.save_schedule(schedule)
            self.fail()
        except InvalidField:
            return
        self.fail()

    def test_remove_schedule(self):
        schedule = schedule_generator(**self.mkobj_schedule())
        self.manager.save_schedule(schedule)
        self.manager.remove_schedule(schedule)
        self.assertEqual(0, self.manager.count())
    
    def test_remove_participant_schedules(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='2'))
        self.manager.save_schedule(schedule_2)
        
        self.manager.remove_participant_schedules('1')
        self.assertEqual(1, self.manager.count())
        self.assertEqual(1, self.manager.find({'participant-phone': '2'}).count())

    def test_remove_participant_interaction(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='reminder-schedule',
            dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_2) 
        
        self.manager.remove_participant_interaction('1', '1', '1')
        self.assertEqual(1, self.manager.count())
        self.assertEqual(1, self.manager.find({'object-type': 'reminder-schedule'}).count())

    def test_remove_participant_reminders(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='deadline-schedule', 
            dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='reminder-schedule',
            dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_2) 
        
        self.manager.remove_participant_reminders('1', '1', '1')
        self.assertEqual(1, self.manager.count())
        self.assertEqual(1, self.manager.find({'object-type': 'deadline-schedule'}).count())

    def test_remove_participant_deadline(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='deadline-schedule', 
            dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='reminder-schedule',
            dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_2) 
        
        self.manager.remove_participant_deadline('1', '1', '1')
        self.assertEqual(1, self.manager.count())
        self.assertEqual(1, self.manager.find({'object-type': 'reminder-schedule'}).count())

    def test_get_participant_reminder_tail(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='reminder-schedule', dialogue_id='1', interaction_id='2'))
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='reminder-schedule', dialogue_id='1',interaction_id='3'))
        self.manager.save_schedule(schedule_1)
        self.manager.save_schedule(schedule_2)
        
        reminders = self.manager.get_participant_reminder_tail('1', '1', '2')
        self.assertEqual(1, reminders.count())
        reminder = reminders.next()
        self.assertIsInstance(reminder, ReminderSchedule)

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
        # the count is still 2 event if the cursor will only iterate over the first one
        self.assertEqual(2, schedules.count())
        self.assertEqual(1, schedules.count(True)) 
        schedule = schedules.next()
        self.assertIsInstance(schedule, DialogueSchedule)
        self.assertEqual('2', schedule['participant-phone'])

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
        #the invalid_schedule has been removed
        self.assertEqual(2, self.manager.count())

    def test_remove_unattach(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule_unattach(
            participant_phone='1', unattach_id='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule_unattach(
            participant_phone='1', unattach_id='2'))
        self.manager.save_schedule(schedule_2)
        
        self.manager.remove_unattach('1')
        self.assertEqual(1, self.manager.count())
        self.assertEqual(1, self.manager.find({'unattach-id': '2'}).count())

    def test_get_participant_unattach(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule_unattach(
            participant_phone='1', unattach_id='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule_unattach(
            participant_phone='1', unattach_id='2'))
        self.manager.save_schedule(schedule_2)
        
        schedule = self.manager.get_participant_unattach('1', '1')
        self.assertIsInstance(schedule, UnattachSchedule)
        self.assertEqual('1', schedule['unattach-id'])

    def test_get_interaction(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='dialogue-schedule', dialogue_id='1', interaction_id='2'))
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', object_type='dialogue-schedule', dialogue_id='1',interaction_id='3'))
        self.manager.save_schedule(schedule_1)
        self.manager.save_schedule(schedule_2)
        
        interaction = self.manager.get_participant_interaction('1', '1', '2')
        self.assertIsInstance(interaction, DialogueSchedule)

    def test_remove_dialogue(self):
        schedule_1 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', dialogue_id='1', interaction_id='1'))
        self.manager.save_schedule(schedule_1)
        schedule_2 = schedule_generator(**self.mkobj_schedule(
            participant_phone='1', dialogue_id='1', interaction_id='2'))
        self.manager.save_schedule(schedule_2) 
        
        self.manager.remove_dialogue('1')
        self.assertEqual(0, self.manager.count())

    def test_save_unattached_schedule_update(self):
        schedule = schedule_generator(**self.mkobj_schedule_unattach(
            participant_phone='06', unattach_id='1',
            date_time='2010-03-12T12:30:00'))
        self.manager.save_schedule(schedule)

        unattach = UnattachMessage(**self.mkobj_unattach_message(
            fixed_time='2200-03-12T12:30:00'))
        unattach['_id'] = '1'
        participant = Participant(**self.mkobj_participant(
            participant_phone='06'))

        self.manager.save_unattach_schedule(participant, unattach)

        self.assertEqual(1, self.manager.count())
        save_schedule = self.manager.find_one()
        self.assertEqual(
            save_schedule['date-time'],
            '2200-03-12T12:30:00')

    def test_save_unattached_schedule_new(self):
        schedule = schedule_generator(**self.mkobj_schedule_unattach(
            participant_phone='06', unattach_id='2'))
        self.manager.save_schedule(schedule)

        unattach = UnattachMessage(**self.mkobj_unattach_message())
        unattach['_id'] = '1'
        participant = Participant(**self.mkobj_participant(
            participant_phone='06'))

        self.manager.save_unattach_schedule(participant, unattach)

        self.assertEqual(2, self.manager.count())
