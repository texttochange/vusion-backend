from datetime import datetime, time, date, timedelta
import pytz

import json
import pymongo
from bson.objectid import ObjectId
from bson.timestamp import Timestamp

from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from vusion.dialogue_worker import DialogueWorker
from vusion.utils import time_to_vusion_format, time_from_vusion_format

from tests.utils import MessageMaker, DataLayerUtils, ObjectMaker
from vusion.tests.test_dialogue_worker import DialogueWorkerTestCase

class DialogueWorkerTestCase_schedule(DialogueWorkerTestCase, MessageMaker,
                                              DataLayerUtils, ObjectMaker):
    
    def test_schedule_participant_dialogue_offset_days(self):
        config = self.simple_config
        dialogue = self.dialogue_announcement
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dNow = dNow.replace(tzinfo=None)
        dPast = dNow - timedelta(minutes=30)

        participant = self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dPast - timedelta(days=1)),
            enrolled=[{'dialogue-id':'0', 'date-time': time_to_vusion_format(dPast)}])

        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['schedules'].count(),2)

        schedules = self.collections['schedules'].find()
        #assert time calculation
        self.assertTrue(
            time_from_vusion_format(schedules[0]['date-time']) -
            datetime.combine(dPast.date() + timedelta(days=1), time(22,30))
            < timedelta(minutes=1))
        self.assertTrue(
            time_from_vusion_format(schedules[1]['date-time']) -
            datetime.combine(dPast.date() + timedelta(days=2), time(22,30))
            < timedelta(minutes=1))

        #assert schedule links
        self.assertEqual(schedules[0]['participant-phone'], '06')
        self.assertEqual(schedules[0]['dialogue-id'], '0')
        self.assertEqual(schedules[0]['interaction-id'], '0')
        self.assertEqual(schedules[1]['interaction-id'], '1')
        
    def test_schedule_participant_dialogue_offset_time(self):        
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()        
        #config = self.simple_config
        dialogue = self.mkobj_dialogue_announcement_offset_time()
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dNow = dNow.replace(tzinfo=None)
        dPast = dNow - timedelta(minutes=3)

        participant = self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dPast - timedelta(minutes=60)),
            enrolled=[{'dialogue-id': '0',
                       'date-time': time_to_vusion_format(dPast)}]
        )
        
        self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['schedules'].count(),2)
        
        schedules = self.collections['schedules'].find()
        #assert time calculation
        self.assertEqual(
            time_to_vusion_format(time_from_vusion_format(schedules[0]['date-time'])),
            time_to_vusion_format(dPast + timedelta(minutes=10)))
        self.assertEqual(
            time_to_vusion_format(time_from_vusion_format(schedules[1]['date-time'])),
            time_to_vusion_format(dPast + timedelta(minutes=50)))

    def test_schedule_interaction_while_interaction_in_history(self):
        mytimezone = self.program_settings[2]['value']
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dPast = dNow - timedelta(minutes=30)

        dialogue = self.dialogue_announcement
        participant = self.mkobj_participant(
            '06', 
            last_optin_date=time_to_vusion_format(dPast - timedelta(days=1)),
            enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)}]
        )

        self.save_history(
            timestamp=dPast,
            participant_phone='06',
            participant_session_id=participant['session-id'],
            metadata = {'interaction-id':'0',
                        'dialogue-id':'0'})
        #Starting the test
        schedules = self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['history'].count(), 1)
        self.assertEqual(self.collections['schedules'].count(), 1)

    def test_schedule_interaction_while_interaction_in_schedule(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
      
        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=30)
        dFuture = dNow + timedelta(minutes=30)
        dLaterFuture = dNow + timedelta(minutes=60)

        participant = self.mkobj_participant(
            enrolled = [{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)}]
        )
        
        dialogue = self.dialogue_announcement        
        dialogue['interactions'][1]['type-schedule'] = 'fixed-time'
        dialogue['interactions'][1]['date-time'] = dLaterFuture.strftime(
            self.time_format)

        #Declare collection for scheduling messages
        self.collections['schedules'].save({
            'date-time': dFuture.strftime(self.time_format),
            'participant-phone': '06',
             'object-type': 'dialogue-schedule',
            'interaction-id': '1',
            'dialogue-id': '0'})
        self.save_history(
            timestamp=dPast,
            participant_phone='06',
            participant_session_id='1',
            metadata = {'interaction-id':'0',
                        'dialogue-id':'0'})

        #Starting the test
        schedules = self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['history'].count(), 1)
        self.assertEqual(self.collections['schedules'].count(), 1)
        schedule = self.collections['schedules'].find_one()
        self.assertEqual(schedule['date-time'], dLaterFuture.strftime(self.time_format))

    def test_schedule_interaction_fixed_time_expired(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
      
        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=5)

        participant = self.mkobj_participant()
        dialogue = self.mkobj_dialogue_annoucement()
        dialogue['interactions'][0]['type-schedule'] = 'fixed-time'
        dialogue['interactions'][0]['date-time'] = time_to_vusion_format(dPast)

        self.worker.schedule_participant_dialogue(
            participant, dialogue)
        
        self.assertEqual(self.collections['schedules'].count(), 0)
        self.assertEqual(self.collections['history'].count(), 1)

    def test_schedule_interaction_offset_days_expired(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
      
        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(days=2)

        dialogue = self.mkobj_dialogue_annoucement()
        participant = self.mkobj_participant(
            enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)}]
        )
        
        self.worker.schedule_participant_dialogue(
            participant, dialogue)
        
        self.assertEqual(self.collections['schedules'].count(), 0)
        self.assertEqual(self.collections['history'].count(), 1)


    def test_schedule_interaction_offset_time_expired(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
      
        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=60)

        dialogue = self.mkobj_dialogue_announcement_offset_time()
        participant = self.mkobj_participant(
            enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)}])
        
        self.worker.schedule_participant_dialogue(
            participant, dialogue)
        
        self.assertEqual(self.collections['schedules'].count(), 0)
        self.assertEqual(self.collections['history'].count(), 2)

    def test_schedule_at_fixed_time(self):
        dialogue = self.dialogue_announcement_fixedtime
        participant = self.mkobj_participant('06')
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
      
        dNow = self.worker.get_local_time()
        dFuture = datetime.now() + timedelta(days=2, minutes=30)
        dialogue['interactions'][0]['date-time'] = dFuture.strftime(
            self.time_format)

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)

        #action
        self.worker.schedule_participant_dialogue(
            participant, dialogue)

        #asserting
        self.assertEqual(self.collections['schedules'].count(), 1)
        schedule = self.collections['schedules'].find_one()
        schedule_datetime = datetime.strptime(schedule['date-time'],
                                              '%Y-%m-%dT%H:%M:%S')
        self.assertEquals(schedule_datetime.year, dFuture.year)
        self.assertEquals(schedule_datetime.hour, dFuture.hour)
        self.assertEquals(schedule_datetime.minute, dFuture.minute)
        
    def test_schedule_participant_reminders(self):
        config = self.simple_config
        dialogue = self.mkobj_dialogue_open_question_reminder()
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dNow = dNow.replace(tzinfo=None)
        dPast = dNow - timedelta(minutes=30)

        participant = self.mkobj_participant('06', last_optin_date=time_to_vusion_format(dPast))
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        interaction = dialogue['interactions'][0]
        # change the date-time of the interaction to match dPast
        interaction['date-time'] = time_to_vusion_format(dPast)
        self.worker.schedule_participant_reminders(
            participant, dialogue, interaction, time_from_vusion_format(interaction['date-time']))

        schedules_count = self.collections['schedules'].count()
        self.assertEqual(schedules_count, 3)

        schedules = self.collections['schedules'].find()
        #assert time calculation
        self.assertEqual(
            time_to_vusion_format(time_from_vusion_format(schedules[0]['date-time'])),
            time_to_vusion_format(dPast + timedelta(minutes=30)))
        self.assertEqual(
            time_to_vusion_format(time_from_vusion_format(schedules[1]['date-time'])),
            time_to_vusion_format(dPast + timedelta(minutes=30) + timedelta(minutes=30)))
        self.assertEqual(
            time_to_vusion_format(time_from_vusion_format(schedules[2]['date-time'])),
            time_to_vusion_format(dPast + timedelta(minutes=30) + timedelta(minutes=30) + timedelta(minutes=30)))

        #assert scheduled reminders are the same
        self.assertEqual(schedules[0]['dialogue-id'], schedules[1]['dialogue-id'])
        self.assertEqual(schedules[0]['dialogue-id'], schedules[2]['dialogue-id'])
        self.assertEqual(schedules[0]['interaction-id'], schedules[1]['interaction-id'])
        self.assertEqual(schedules[0]['interaction-id'], schedules[2]['interaction-id'])
        
        #assert that first schedules are reminder-schedules
        self.assertEqual(schedules[0]['object-type'], 'reminder-schedule')
        self.assertEqual(schedules[1]['object-type'], 'reminder-schedule')
        
        #assert last reminder is deadline-schedule
        self.assertEqual(schedules[2]['object-type'], 'deadline-schedule')

    def test_schedule_unattach_message(self):
        participants = [self.mkobj_participant(),
                        self.mkobj_participant('07')]

        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dFuture = dNow + timedelta(minutes=30)
        dPast = dNow - timedelta(minutes=30)

        unattach_messages = [
            self.mkobj_unattach_message(
                fixed_time=time_to_vusion_format(dFuture)),
            self.mkobj_unattach_message(
                content='Hello again',
                fixed_time=time_to_vusion_format(dPast))]

        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        for participant in participants:
            self.collections['participants'].save(participant)

        unattach_id = self.collections['unattached_messages'].save(unattach_messages[0])
        self.collections['unattached_messages'].save(unattach_messages[1])

        self.collections['history'].save(self.mkobj_history_unattach(
            unattach_id, dPast))

        self.worker.load_data()

        self.worker.schedule_participants_unattach_messages(
            participants)

        schedules_count = self.collections['schedules'].count()
        self.assertEqual(schedules_count, 1)
        schedules = self.collections['schedules'].find()
        self.assertEqual(schedules[0]['participant-phone'], '07')