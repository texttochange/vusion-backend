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
from vusion.persist import Dialogue, Participant, schedule_generator


class DialogueWorkerTestCase_schedule(DialogueWorkerTestCase):

    def test_schedule_participant_dialogue_offset_days(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=30)

        dialogue = Dialogue(**self.mkobj_dialogue_announcement_offset_days())
        participant = Participant(**self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dPast - timedelta(days=1)),
            enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)}]))

        self.worker.schedule_participant_dialogue(participant, dialogue)

        self.assertEqual(self.collections['schedules'].count(), 2)

        schedules = self.collections['schedules'].find()
        #assert time calculation
        self.assertTrue(
            time_from_vusion_format(schedules[0]['date-time']) -
            datetime.combine(dPast.date() + timedelta(days=1), time(22, 30))
            < timedelta(minutes=1))
        self.assertTrue(
            time_from_vusion_format(schedules[1]['date-time']) -
            datetime.combine(dPast.date() + timedelta(days=2), time(22, 30))
            < timedelta(minutes=1))

        #assert schedule links
        self.assertEqual(schedules[0]['participant-phone'], '06')
        self.assertEqual(schedules[0]['dialogue-id'], '0')
        self.assertEqual(schedules[0]['interaction-id'], '0')
        self.assertEqual(schedules[1]['interaction-id'], '1')

    def test_schedule_participant_dialogue_offset_time(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=3)

        dialogue = Dialogue(**self.mkobj_dialogue_announcement_offset_time())

        participant = Participant(**self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dPast - timedelta(minutes=60)),
            enrolled=[{'dialogue-id': '0',
                       'date-time': time_to_vusion_format(dPast)}]))

        self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['schedules'].count(), 3)

        schedules = self.collections['schedules'].find()
        #assert time calculation
        self.assertEqual(
            time_to_vusion_format(time_from_vusion_format(schedules[0]['date-time'])),
            time_to_vusion_format(dPast + timedelta(seconds=10)))
        self.assertEqual(
            time_to_vusion_format(time_from_vusion_format(schedules[1]['date-time'])),
            time_to_vusion_format(dPast + timedelta(minutes=10)))
        self.assertEqual(
            time_to_vusion_format(time_from_vusion_format(schedules[2]['date-time'])),
            time_to_vusion_format(dPast + timedelta(minutes=50)))

    def test_schedule_interaction_while_interaction_in_history(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=30)

        dialogue = Dialogue(**self.mkobj_dialogue_announcement_offset_days())
        participant = Participant(**self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dPast - timedelta(days=1)),
            enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)}]))

        self.save_history(
            timestamp=dPast,
            participant_phone='06',
            participant_session_id=participant['session-id'],
            metadata={'interaction-id': '0',
                      'dialogue-id': '0'})
        #Starting the test
        schedules = self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['history'].count(), 1)
        self.assertEqual(self.collections['schedules'].count(), 1)

    def test_schedule_interaction_while_interaction_in_schedule(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=30)
        dFuture = dNow + timedelta(minutes=30)
        dLaterFuture = dNow + timedelta(minutes=60)

        participant = self.mkobj_participant(
            enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)}])

        dialogue = self.mkobj_dialogue_announcement_offset_days()
        dialogue['interactions'][1]['type-schedule'] = 'fixed-time'
        dialogue['interactions'][1]['date-time'] = time_to_vusion_format(dLaterFuture)
        dialogue = Dialogue(**dialogue)

        #Declare collection for scheduling messages
        
        self.collections['schedules'].save(self.mkobj_schedule(
            date_time=time_to_vusion_format(dFuture),
            participant_phone='06',
            interaction_id='1',
            dialogue_id='0'))
        self.save_history(
            timestamp=dPast,
            participant_phone='06',
            participant_session_id='1',
            metadata={'interaction-id': '0',
                      'dialogue-id': '0'})

        #Starting the test
        schedules = self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['history'].count(), 1)
        self.assertEqual(self.collections['schedules'].count(), 1)
        schedule = schedule_generator(**self.collections['schedules'].find_one())
        self.assertTrue(schedule.get_schedule_time() - dLaterFuture < timedelta(seconds=1))

    def test_schedule_interaction_fixed_time_expired(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=5)

        participant = self.mkobj_participant()
        dialogue = self.mkobj_dialogue_annoucement()
        dialogue['interactions'][0]['type-schedule'] = 'fixed-time'
        dialogue['interactions'][0]['date-time'] = time_to_vusion_format(dPast)
        dialogue = Dialogue(**dialogue)

        self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['schedules'].count(), 0)
        self.assertEqual(self.collections['history'].count(), 1)

    def test_schedule_interaction_offset_days_expired(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(days=2)

        dialogue = Dialogue(**self.mkobj_dialogue_annoucement())
        participant = Participant(**self.mkobj_participant(
            enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)}]))

        self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['schedules'].count(), 0)
        self.assertEqual(self.collections['history'].count(), 1)

    def test_schedule_interaction_offset_time_expired(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=60)

        dialogue = Dialogue(**self.mkobj_dialogue_announcement_offset_time())
        participant = Participant(**self.mkobj_participant(
            enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)}]))

        self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['schedules'].count(), 0)
        self.assertEqual(self.collections['history'].count(), 3)

    def test_schedule_at_fixed_time(self):
        self.initialize_properties()
        dNow = self.worker.get_local_time()
        dFuture = datetime.now() + timedelta(days=2, minutes=30)

        dialogue = self.mkobj_dialogue_announcement_fixedtime()
        dialogue['interactions'][0]['date-time'] = time_to_vusion_format(dFuture)
        dialogue = Dialogue(**dialogue)

        participant = self.mkobj_participant('06')
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
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dNow = dNow.replace(tzinfo=None)
        dPast = dNow - timedelta(minutes=30)

        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder_offset_time())
        participant = self.mkobj_participant('06', last_optin_date=time_to_vusion_format(dPast))

        interaction = dialogue.interactions[0]
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
        self.assertEqual(
            schedules[0]['dialogue-id'], schedules[1]['dialogue-id'])
        self.assertEqual(
            schedules[0]['dialogue-id'], schedules[2]['dialogue-id'])
        self.assertEqual(
            schedules[0]['interaction-id'], schedules[1]['interaction-id'])
        self.assertEqual(
            schedules[0]['interaction-id'], schedules[2]['interaction-id'])

        #assert that first schedules are reminder-schedules
        self.assertEqual(schedules[0]['object-type'], 'reminder-schedule')
        self.assertEqual(schedules[1]['object-type'], 'reminder-schedule')

        #assert last reminder is deadline-schedule
        self.assertEqual(schedules[2]['object-type'], 'deadline-schedule')

    def test_reschedule_reminder_after_interaction_in_history(self):
        self.initialize_properties()
        
        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=6)
        dFuture = dNow + timedelta(minutes=20)
        dMoreFuture = dFuture + timedelta(minutes=30)

        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder_offset_time())
        participant = self.mkobj_participant('06', last_optin_date=time_to_vusion_format(dPast))

        interaction = dialogue.interactions[0]
        interaction_id = interaction['interaction-id']
        
        interaction['reminder-minutes'] = '100'

        history = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction_id,
            timestamp=time_to_vusion_format(dPast))
        self.collections['history'].save(history)
    
        reminder_schedule = self.mkobj_schedule(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction_id,
            date_time=time_to_vusion_format(dFuture),
            object_type='reminder-schedule')
        self.collections['schedules'].save(reminder_schedule)

        deadline_schedule = self.mkobj_schedule(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction_id,
            date_time=time_to_vusion_format(dFuture),
            object_type='deadline-schedule')
        self.collections['schedules'].save(deadline_schedule)        

        self.worker.schedule_participant_dialogue(participant, dialogue)

        self.assertEqual(3, self.collections['schedules'].count())
        self.assertEqual(1, self.collections['history'].count())

        schedules = self.collections['schedules'].find()
        #assert time calculation
        self.assertEqual(
            schedules[0]['date-time'],
            time_to_vusion_format(dPast + timedelta(minutes=100)))
        self.assertEqual(
            schedules[1]['date-time'],
            time_to_vusion_format(dPast + timedelta(minutes=200)))
        self.assertEqual(
            schedules[2]['date-time'],
            time_to_vusion_format(dPast + timedelta(minutes=300)))

        #assert that first schedules are reminder-schedules
        self.assertEqual(schedules[0]['object-type'], 'reminder-schedule')
        self.assertEqual(schedules[1]['object-type'], 'reminder-schedule')
        #assert last reminder is deadline-schedule
        self.assertEqual(schedules[2]['object-type'], 'deadline-schedule')

    def test_reschedule_reminder_after_interaction_and_reminder_in_history(self):
        self.initialize_properties()

        d_now = self.worker.get_local_time()
        d_enrolled = d_now - timedelta(minutes=7)
        d_interaction_send = d_enrolled + timedelta(minutes=3)
        d_interaction_reminder_send = d_interaction_send + timedelta(minutes=3)
        d_interaction_deadline = d_interaction_reminder_send + timedelta(minutes=3)

        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder())
        interaction = dialogue['interactions'][0]

        participant = self.mkobj_participant(
            participant_phone='06',
            enrolled=[{'dialogue-id': dialogue['dialogue-id'],
                       'date-time': time_to_vusion_format(d_enrolled)}])
        
        history_send = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction['interaction-id'],
            timestamp=time_to_vusion_format(d_interaction_send))
        self.collections['history'].save(history_send)

        history_reminder_send = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction['interaction-id'],
            timestamp=time_to_vusion_format(d_interaction_reminder_send))
        self.collections['history'].save(history_reminder_send)

        deadline_schedule = self.mkobj_schedule(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction['interaction-id'],
            date_time=time_to_vusion_format(d_interaction_deadline),
            object_type='deadline-schedule')
        self.collections['schedules'].save(deadline_schedule)

        self.worker.schedule_participant_dialogue(participant, dialogue)
        
        self.assertEqual(2, self.collections['schedules'].count())
        self.assertEqual(2, self.collections['history'].count())

        schedules = self.collections['schedules'].find()
        self.assertEqual('reminder-schedule', schedules[0]['object-type'])        
        self.assertEqual('deadline-schedule', schedules[1]['object-type'])

    def test_reschedule_reminder_after_reminder_in_history_and_reducing_reminder(self):
        self.initialize_properties()

        d_now = self.worker.get_local_time()
        d_enrolled = d_now - timedelta(minutes=10)
        d_interaction_send = d_enrolled + timedelta(minutes=3)
        d_interaction_reminder_1_send = d_interaction_send + timedelta(minutes=3)
        d_interaction_reminder_2_send = d_interaction_reminder_1_send + timedelta(minutes=3)
        d_interaction_deadline = d_interaction_reminder_2_send + timedelta(minutes=3)

        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder())
        dialogue.interactions[0]['reminder-number'] = '1'
        interaction = dialogue.interactions[0]

        participant = self.mkobj_participant(
            participant_phone='06',
            enrolled=[{'dialogue-id': dialogue['dialogue-id'],
                       'date-time': time_to_vusion_format(d_enrolled)}])
        
        history_send = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction['interaction-id'],
            timestamp=time_to_vusion_format(d_interaction_send))
        self.collections['history'].save(history_send)

        history_reminder_send = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'], 
            interaction_id=interaction['interaction-id'],
            timestamp=time_to_vusion_format(d_interaction_reminder_1_send))
        self.collections['history'].save(history_reminder_send)

        history_reminder_send = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction['interaction-id'],
            timestamp=time_to_vusion_format(d_interaction_reminder_2_send))
        self.collections['history'].save(history_reminder_send)

        deadline_schedule = self.mkobj_schedule(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction['interaction-id'],
            date_time=time_to_vusion_format(d_interaction_deadline),
            object_type='deadline-schedule')
        self.collections['schedules'].save(deadline_schedule)    

        self.worker.schedule_participant_dialogue(participant, dialogue)

        self.assertEqual(1, self.collections['schedules'].count())
        self.assertEqual(3, self.collections['history'].count())

        schedule = self.collections['schedules'].find_one()
        self.assertEqual('deadline-schedule', schedule['object-type'])

        self.assertTrue(
            time_from_vusion_format(schedule['date-time']) - d_now < timedelta(seconds=1))

    def test_reschedule_reminder_after_already_answer(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=6)
        dFuture = dNow + timedelta(minutes=20)
        dMoreFuture = dFuture + timedelta(minutes=30)

        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder_offset_time())
        participant = self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dPast))

        interaction = dialogue.interactions[0]
        interaction_id = interaction['interaction-id']

        interaction['reminder-minutes'] = '100'

        question_history = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction_id,
            timestamp=time_to_vusion_format(dPast))
        self.collections['history'].save(question_history)

        answer_history = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction_id,
            timestamp=time_to_vusion_format(dPast),
            direction='incoming',
            matching_answer='something')
        self.collections['history'].save(answer_history)

        self.worker.schedule_participant_dialogue(participant, dialogue)

        self.assertEqual(0, self.collections['schedules'].count())

    def test_reschedule_reminder_after_one_way_marker_in_history(self):
        self.initialize_properties()
        
        self.broker.dispatched = {}
        d_now = self.worker.get_local_time()
        d_enrolled = d_now - timedelta(minutes=60)
        d_interaction_send = d_enrolled + timedelta(minutes=3)
        d_interaction_reminder_send = d_interaction_send + timedelta(minutes=3)
        d_interaction_deadline = d_interaction_reminder_send + timedelta(minutes=3)

        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder())
        participant = self.mkobj_participant(
            participant_phone='06',
            enrolled=[{'dialogue-id': '04',
                       'date-time': time_to_vusion_format(d_enrolled)}])

        history_send = self.mkobj_history_dialogue(
            dialogue_id='04',
            interaction_id='01-01',
            timestamp=time_to_vusion_format(d_interaction_send))
        self.collections['history'].save(history_send)

        history_reminder_send = self.mkobj_history_dialogue(
            dialogue_id='04',
            interaction_id='01-01',
            timestamp=time_to_vusion_format(d_interaction_reminder_send))
        self.collections['history'].save(history_reminder_send)

        history_feedback_send = self.mkobj_history_one_way_marker(
            dialogue_id='04', 
            interaction_id='01-01',
            timestamp=time_to_vusion_format(d_interaction_deadline))
        self.collections['history'].save(history_feedback_send)

        self.worker.schedule_participant_dialogue(participant, dialogue)

        self.assertEqual(0, self.collections['schedules'].count())
        self.assertEqual(3, self.collections['history'].count())

    def test_reschedule_reminder_at_correct_time_after_reminder_in_history(self):
        self.initialize_properties()

        d_now = self.worker.get_local_time()
        d_enrolled = d_now - timedelta(minutes=60)
        d_interaction_send = d_enrolled + timedelta(minutes=3)
        d_interaction_reminder_1_send = d_interaction_send + timedelta(minutes=30)
        d_interaction_reminder_2_send = d_interaction_reminder_1_send + timedelta(minutes=30)
        d_interaction_deadline = d_interaction_reminder_2_send + timedelta(minutes=30)

        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder_offset_time())
        dialogue.interactions[0]['reminder-number'] = '2'
        dialogue.interactions[0]['reminder-minutes'] = '60'
        interaction = dialogue.interactions[0]
        participant = self.mkobj_participant(
            participant_phone='06',
            enrolled=[{'dialogue-id': '04',
                       'date-time': time_to_vusion_format(d_enrolled)}])

        history_send = self.mkobj_history_dialogue(
            dialogue_id='04',
            interaction_id='01-01',
            timestamp=time_to_vusion_format(d_interaction_send))
        self.collections['history'].save(history_send)

        history_reminder_send = self.mkobj_history_dialogue(
            dialogue_id='04',
            interaction_id='01-01',
            timestamp=time_to_vusion_format(d_interaction_reminder_1_send))
        self.collections['history'].save(history_reminder_send)

        reminder_schedule = self.mkobj_schedule(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction['interaction-id'],
            date_time=time_to_vusion_format(d_interaction_reminder_2_send),
            object_type='reminder-schedule')
        self.collections['schedules'].save(reminder_schedule)

        deadline_schedule = self.mkobj_schedule(
                    dialogue_id=dialogue['dialogue-id'],
                    interaction_id=interaction['interaction-id'],
                    date_time=time_to_vusion_format(d_interaction_deadline),
                    object_type='deadline-schedule')
        self.collections['schedules'].save(reminder_schedule)
        
        self.worker.schedule_participant_dialogue(participant, dialogue)
        
        #The interaction has one reminder more
        self.assertEqual(2, self.collections['schedules'].count())
        self.assertEqual(2, self.collections['history'].count())
        
        schedules = self.collections['schedules'].find()
        
        self.assertEqual(schedules[0]['date-time'],
                         time_to_vusion_format(d_interaction_reminder_2_send + timedelta(minutes=60)))
        self.assertEqual(schedules[1]['date-time'],
                         time_to_vusion_format(d_interaction_deadline + timedelta(minutes=90)))
        
        self.assertEqual(schedules[0]['object-type'], 'reminder-schedule')
        self.assertEqual(schedules[1]['object-type'], 'deadline-schedule')

    def test_reschedule_reminder_removing_reminder(self):
        self.initialize_properties()

        d_now = self.worker.get_local_time()
        d_enrolled = d_now - timedelta(minutes=7)
        d_interaction_send = d_enrolled + timedelta(minutes=3)
        d_interaction_reminder_1_send = d_interaction_send + timedelta(minutes=3)
        d_interaction_reminder_2_send = d_interaction_reminder_1_send + timedelta(minutes=3)
        d_interaction_deadline = d_interaction_reminder_2_send + timedelta(minutes=3)

        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder_offset_time())
        dialogue.interactions[0]['set-reminder'] = None
        interaction = dialogue.interactions[0]
        participant = self.mkobj_participant(
            participant_phone='06',
            enrolled=[{'dialogue-id': dialogue['dialogue-id'],
                       'date-time': time_to_vusion_format(d_enrolled)}])
        
        history_send = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'], 
            interaction_id=interaction['interaction-id'],
            timestamp=time_to_vusion_format(d_interaction_send))
        self.collections['history'].save(history_send)
        
        history_reminder_send = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'], 
            interaction_id=interaction['interaction-id'],
            timestamp=time_to_vusion_format(d_interaction_reminder_1_send))
        self.collections['history'].save(history_reminder_send)
        
        reminder_schedule = self.mkobj_schedule(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction['interaction-id'],
            date_time=time_to_vusion_format(d_interaction_reminder_2_send),
            object_type='reminder-schedule')
        self.collections['schedules'].save(reminder_schedule)
        
        deadline_schedule = self.mkobj_schedule(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction['interaction-id'],
            date_time=time_to_vusion_format(d_interaction_deadline),
            object_type='deadline-schedule')
        self.collections['schedules'].save(deadline_schedule)    
        
        self.worker.schedule_participant_dialogue(participant, dialogue)

        self.assertEqual(0, self.collections['schedules'].count())
        self.assertEqual(2, self.collections['history'].count())

    def test_reschedule_reminder_interaction_in_schedule_adding_reminder(self):
        self.initialize_properties()

        d_now = self.worker.get_local_time()
        d_enrolled = d_now + timedelta(minutes=7)
        d_interaction = d_enrolled + timedelta(minutes=3)

        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder())
        interaction = dialogue['interactions'][0]
        
        participant = Participant(
            **self.mkobj_participant(
                participant_phone='06',
                enrolled=[{'dialogue-id': dialogue['dialogue-id'],
                           'date-time': time_to_vusion_format(d_enrolled)}]))
        
        schedule = self.mkobj_schedule(
            dialogue_id=dialogue['dialogue-id'],
            interaction_id=interaction['interaction-id'],
            date_time=time_to_vusion_format(d_interaction),
            object_type='dialogue-schedule')
        
        self.worker.schedule_participant_dialogue(participant, dialogue)
        
        self.assertEqual(4, self.collections['schedules'].count())
        self.assertEqual(0, self.collections['history'].count())

    def test_reschedule_reminder_interaction_in_history_adding_reminder(self):
        self.initialize_properties()

        d_now = self.worker.get_local_time()
        d_enrolled = d_now - timedelta(minutes=7)
        d_interaction_send = d_enrolled + timedelta(minutes=3)
        d_interaction_reminder_send = d_interaction_send + timedelta(minutes=3)
        d_interaction_deadline = d_interaction_reminder_send + timedelta(minutes=3)

        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder())
        interaction = dialogue['interactions'][0]
        
        participant = self.mkobj_participant(
            participant_phone='06',
            enrolled=[{'dialogue-id': dialogue['dialogue-id'],
                       'date-time': time_to_vusion_format(d_enrolled)}])
        
        #before the interaction did had any reminder
        history_send = self.mkobj_history_dialogue(
            dialogue_id=dialogue['dialogue-id'], 
            interaction_id=interaction['interaction-id'],
            timestamp=time_to_vusion_format(d_interaction_send))
        self.collections['history'].save(history_send)
        
        self.worker.schedule_participant_dialogue(participant, dialogue)
        
        self.assertEqual(0, self.collections['schedules'].count())
        self.assertEqual(1, self.collections['history'].count())

    def test_schedule_unattach_message(self):
        self.initialize_properties()
        
        dNow = self.worker.get_local_time()
        dFuture = dNow + timedelta(minutes=30)
        dPast = dNow - timedelta(minutes=30)        
        
        participants = [self.mkobj_participant('06', session_id = None),
                        self.mkobj_participant('07', session_id = '1')]

        unattach_messages = [
            self.mkobj_unattach_message_1(
                fixed_time=time_to_vusion_format(dFuture)),
            self.mkobj_unattach_message_1(
                content='Hello again',
                fixed_time=time_to_vusion_format(dPast))]

        for participant in participants:
            self.collections['participants'].save(participant)

        unattach_id = self.collections['unattached_messages'].save(unattach_messages[0])
        self.collections['unattached_messages'].save(unattach_messages[1])

        self.collections['history'].save(self.mkobj_history_unattach(
            unattach_id, time_to_vusion_format(dPast)))

        self.worker.schedule_unattach(unattach_id)

        schedules_count = self.collections['schedules'].count()
        self.assertEqual(schedules_count, 1)
        schedules = self.collections['schedules'].find()
        self.assertEqual(schedules[0]['participant-phone'], '07')

    def test_schedule_unattach_message_match(self):
        self.initialize_properties()       
     
        dNow = self.worker.get_local_time()
        dFuture = dNow + timedelta(minutes=30)
        dPast = dNow - timedelta(minutes=30)

        participant_06 = self.mkobj_participant(
            '06', 
            tags=['geek'],
            profile=[{'label': 'city', 'value': 'jinja', 'raw': None},
                     {'label': 'born', 'value': 'kampala', 'raw': None}]
        )
        participant_07 = self.mkobj_participant(
            '07',
            profile=[{'label': 'city', 'value': 'kampala', 'raw': None}])
        self.collections['participants'].save(participant_06)
        self.collections['participants'].save(participant_07)
        
        unattach_msg_1 = self.mkobj_unattach_message(
            content='Hello',
            send_to_type='match',
            send_to_match_operator='all',
            send_to_match_conditions=['geek'],
            fixed_time=time_to_vusion_format(dFuture))
        unattach_msg_2 = self.mkobj_unattach_message(
            content='Hello again',
            send_to_type='match',
            send_to_match_operator='all',
            send_to_match_conditions=['city:kampala'],
            fixed_time=time_to_vusion_format(dFuture))

        unattach_msg_id_1 = self.collections['unattached_messages'].save(unattach_msg_1)
        unattach_msg_id_2 = self.collections['unattached_messages'].save(unattach_msg_2)

        self.worker.schedule_unattach(str(unattach_msg_id_1))

        schedules_count = self.collections['schedules'].count()
        self.assertEqual(schedules_count, 1)
        schedules = self.collections['schedules'].find()
        self.assertEqual(schedules[0]['participant-phone'], '06')
        
        self.worker.schedule_unattach(str(unattach_msg_id_2))
        
        schedules_count = self.collections['schedules'].count()
        self.assertEqual(schedules_count, 2)
        schedule = self.collections['schedules'].find_one({'unattach-id': str(unattach_msg_id_2)})
        self.assertEqual(schedule['participant-phone'], '07')
        
        #rescheduling is removing non selected participant
        unattach_msg_2 = self.collections['unattached_messages'].find_one(
            {'_id': ObjectId(unattach_msg_id_2)})
        unattach_msg_2['send-to-match-conditions'] = ['geek']
        self.collections['unattached_messages'].save(unattach_msg_2)
        
        self.worker.schedule_unattach(str(unattach_msg_id_2))
        schedules_count = self.collections['schedules'].count()
        self.assertEqual(schedules_count, 2)
        schedule = self.collections['schedules'].find_one({'unattach-id': str(unattach_msg_id_2)})
        self.assertEqual(schedule['participant-phone'], '06')

    def test_schedule_participant(self):
        self.initialize_properties()     

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=30)
        dialogue_1 = self.mkobj_dialogue_annoucement()
        dialogue_2 = self.mkobj_dialogue_announcement_2()
        self.collections['dialogues'].save(dialogue_1)
        self.collections['dialogues'].save(dialogue_2)
        unattach = self.mkobj_unattach_message_2(recipient=['geek'])
        self.collections['unattached_messages'].save(unattach)
        unattach = self.mkobj_unattach_message_2(recipient=['cool'])
        self.collections['unattached_messages'].save(unattach)        
        participant = self.mkobj_participant(
            '06', 
            tags=['geek'],
            enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dNow)}])
        self.collections['participants'].save(participant)
        
        self.worker.schedule_participant('06')
        
        self.assertEqual(self.collections['schedules'].count(), 2)
        
        
    def test_reschedule_participant_after_edit_enrolled(self):
        self.initialize_properties()        

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=55)
        dialogue_1 = self.mkobj_dialogue_announcement_offset_time()
        dialogue_2 = self.mkobj_dialogue_announcement_2()
        self.collections['dialogues'].save(dialogue_1)
        self.collections['dialogues'].save(dialogue_2)        
        participant = self.mkobj_participant(
            '06', 
            tags=['geek'],
            enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)}])
        self.collections['participants'].save(participant)
        
        self.worker.schedule_participant('06')
        
        participant['enrolled'] = [{'dialogue-id': '0', 'date-time': time_to_vusion_format(dPast)},
                                   {'dialogue-id': '2', 'date-time': time_to_vusion_format(dNow)}]
        self.collections['participants'].save(participant)
        self.worker.schedule_participant('06')
        
        self.assertEqual(self.collections['schedules'].count(), 3)
