"""Tests for vusion.persist.schedule."""

from datetime import timedelta, datetime

from twisted.trial.unittest import TestCase

from vusion.persist import schedule_generator, DialogueSchedule
from tests.utils import ObjectMaker
from vusion.utils import time_from_vusion_format, time_to_vusion_format

class TestSchedule(TestCase, ObjectMaker):
    
    def test_instanciate(self):
        sometime = time_from_vusion_format('2014-10-02T10:00:00')
        schedule = DialogueSchedule(**self.mkobj_schedule(date_time=time_to_vusion_format(sometime)))
        self.assertEqual('2014-10-02T10:00:00', schedule['date-time'])
        
        schedule = DialogueSchedule(**self.mkobj_schedule(date_time=sometime))
        self.assertEqual('2014-10-02T10:00:00', schedule['date-time'])
    
    def test_is_expired(self):
        now = datetime.now()
        
        schedule = schedule_generator(**self.mkobj_schedule(
            date_time=time_to_vusion_format(now)))
        self.assertFalse(schedule.is_expired(now))
        
        past = now - timedelta(minutes=61)        
        schedule = schedule_generator(**self.mkobj_schedule(
            date_time=time_to_vusion_format(past)))        
        self.assertTrue(schedule.is_expired(now))
        
        future = now + timedelta(minutes=15)
        schedule = schedule_generator(**self.mkobj_schedule(
                    date_time=time_to_vusion_format(future)))        
        self.assertFalse(schedule.is_expired(now))