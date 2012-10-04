from datetime import datetime, time, date, timedelta
import pytz

import json
import pymongo
from bson.objectid import ObjectId
from bson.timestamp import Timestamp

from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from vumi.message import Message, TransportEvent, TransportUserMessage
from vumi.tests.utils import get_stubbed_worker, UTCNearNow, RegexMatcher

from vusion.dialogue_worker import TtcGenericWorker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.error import MissingData, MissingTemplate
from vusion.action import (UnMatchingAnswerAction, EnrollingAction,
                           FeedbackAction, OptinAction, OptoutAction,
                           TaggingAction, ProfilingAction,
                           OffsetConditionAction, RemoveRemindersAction,
                           ResetAction, RemoveDeadlineAction,
                           DelayedEnrollingAction, action_generator, Actions)

from transports import YoUgHttpTransport

from tests.utils import MessageMaker, DataLayerUtils, ObjectMaker


class TtcGenericWorkerTestCase(TestCase, MessageMaker, DataLayerUtils,
                               ObjectMaker):

    @inlineCallbacks
    def setUp(self):
        self.transport_name = 'test'
        self.control_name = 'mycontrol'
        self.database_name = 'test_program_db'
        self.vusion_database_name = 'test_vusion_db'
        self.config = {'transport_name': self.transport_name,
                       'database_name': self.database_name,
                       'vusion_database_name': self.vusion_database_name,
                       'control_name': self.control_name,
                       'dispatcher_name': 'dispatcher',
                       'mongodb_host': 'localhost',
                       'mongodb_port': 27017}
        self.worker = get_stubbed_worker(TtcGenericWorker,
                                         config=self.config)
        self.broker = self.worker._amqp_client.broker

        self.broker.exchange_declare('vumi', 'direct')
        self.broker.queue_declare("%s.outbound" % self.transport_name)
        self.broker.queue_bind("%s.outbound" % self.transport_name,
                               "vumi",
                               "%s.outbound" % self.transport_name)
        #Database#
        connection = pymongo.Connection("localhost", 27017)
        self.db = connection[self.config['database_name']]

        self.collections = {}
        self.setup_collections(['dialogues',
                                'participants',
                                'history',
                                'schedules',
                                'shortcodes',
                                'program_settings',
                                'unattached_messages',
                                'requests'])
        self.db = connection[self.config['vusion_database_name']]
        self.setup_collections(['templates'])

        self.drop_collections()
        self.broker.dispatched = {}
        #Let's rock"
        self.worker.startService()
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        self.broker.dispatched = {}
        self.drop_collections()
        if (self.worker.sender is not None):
            yield self.worker.sender.stop()
        yield self.worker.stopWorker()

    @inlineCallbacks
    def send(self, msg, routing_suffix='control'):
        if (routing_suffix == 'control'):
            routing_key = "%s.%s" % (self.control_name, routing_suffix)
        else:
            routing_key = "%s.%s" % (self.transport_name, routing_suffix)
        self.broker.publish_message('vumi', routing_key, msg)
        yield self.broker.kick_delivery()

    def save_history(self, message_content="hello world",
                     participant_phone="256", participant_session_id="1",
                     message_direction="outgoing", message_status="delivered",
                     timestamp=datetime.now(), metadata={}):
        history = {
            'message-content': message_content,
            'participant-session-id': participant_session_id,
            'participant-phone': participant_phone,
            'message-direction': message_direction,
            'message-status': message_status,
            'timestamp': time_to_vusion_format(timestamp)}
        for key in metadata:
            history[key] = metadata[key] 
        self.collections['history'].save(history)

    def test01_has_already_been_answered(self):
        dNow = datetime.now()

        participant = self.mkobj_participant()
       
        self.assertFalse(self.worker.has_already_valid_answer(
            participant, **{'dialogue-id':'1', 'interaction-id':'1', 'matching-answer': None}))
       
        self.collections['history'].save(self.mkobj_history_dialogue(
            direction='incoming',
            participant_phone='06',
            participant_session_id='1',
            timestamp=time_to_vusion_format(dNow),
            dialogue_id='1',
            interaction_id='1',
            matching_answer=None
        ))
        
        self.assertFalse(self.worker.has_already_valid_answer(
            participant, **{'dialogue-id':'1', 'interaction-id':'1', 'matching-answer': None}))
        
        self.assertFalse(self.worker.has_already_valid_answer(
            participant, **{'dialogue-id':'1', 'interaction-id':'1', 'matching-answer': 'something'}))
      
        self.collections['history'].save(self.mkobj_history_dialogue(
            direction='incoming',
            participant_phone='06',
            participant_session_id='1',
            timestamp=time_to_vusion_format(dNow),
            dialogue_id='1',
            interaction_id='1',
            matching_answer='something'
        ))
       
        self.assertFalse(self.worker.has_already_valid_answer(
            participant, **{'dialogue-id':'1', 'interaction-id':'1', 'matching-answer': 'something'}))

        self.collections['history'].save(self.mkobj_history_dialogue(
            direction='incoming',
            participant_phone='06',
            participant_session_id='1',
            timestamp=time_to_vusion_format(dNow),
            dialogue_id='1',
            interaction_id='1',
            matching_answer='something else'
        ))
        
        self.assertTrue(self.worker.has_already_valid_answer(
            participant, **{'dialogue-id':'1', 'interaction-id':'1', 'matching-answer': 'something else'}))


    def test02_is_enrolled(self):
        participant = self.mkobj_participant(enrolled = [{'dialogue-id':'01',
                                                          'date-time': 'someting'},
                                                         {'dialogue-id':'3',
                                                          'date-time': 'something'}])
        self.assertTrue(self.worker.is_enrolled(participant, '01'))
        self.assertTrue(self.worker.is_enrolled(participant, '3'))
        self.assertFalse(self.worker.is_enrolled(participant, '2'))

    def test03_multiple_dialogue_in_collection(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
      
        dNow = self.worker.get_local_time()
        dPast1 = datetime.now() - timedelta(minutes=30)
        dPast2 = datetime.now() - timedelta(minutes=60)

        id_active_dialogue_one = self.collections['dialogues'].save(
            {'do': 'current dialogue',
             'dialogue-id': '1',
             'activated': 1,
             'modified': dPast1})
        self.collections['dialogues'].save(
            {'do': 'previsous dialogue',
             'dialogue-id': '1',
             'activated': 1,
             'modified': dPast2})
        self.collections['dialogues'].save(
            {'do': 'future dialogue still in draft',
             'dialogue-id': '1',
             'activated': 0,
             'modified': '50'})
        id_active_dialogue_two = self.collections['dialogues'].save(
            {'do': 'current dialogue',
             'dialogue-id': '2',
             'activated': 1,
             'modified': dPast1})
        self.collections['dialogues'].save(
            {'do': 'previsous dialogue',
             'dialogue-id': '2',
             'activated': 1,
             'modified': dPast2})
        self.collections['dialogues'].save(
            {'do': 'future dialogue still in draft',
             'dialogue-id': '2',
             'activated': 0,
             'modified': '50'})

        self.collections['participants'].save({'phone': '06'})

        dialogues = self.worker.get_active_dialogues()
        self.assertEqual(len(dialogues), 2)
        self.assertEqual(dialogues[0]['Dialogue']['_id'],
                         id_active_dialogue_one)
        self.assertEqual(dialogues[1]['Dialogue']['_id'],
                         id_active_dialogue_two)

    def test03_get_current_dialogue(self):
        dialogue = self.mkobj_dialogue_annoucement()
        dialogue['modified'] = Timestamp(datetime.now()-timedelta(minutes=1),0)
        self.collections['dialogues'].save(dialogue)
        other_dialogue = self.mkobj_dialogue_annoucement()
        other_dialogue['interactions'] = []
        self.collections['dialogues'].save(other_dialogue)
        active_dialogue = self.worker.get_current_dialogue("0")
        self.assertTrue(active_dialogue)
        self.assertEqual([], active_dialogue['interactions'])

    def test04_schedule_participant_dialogue_offset_days(self):
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
        self.assertEqual(
            time_from_vusion_format(schedules[0]['date-time']),
            datetime.combine(dPast.date() + timedelta(days=1), time(22,30)))
        self.assertEqual(
            time_from_vusion_format(schedules[1]['date-time']),
            datetime.combine(dPast.date() + timedelta(days=2), time(22,30)))

        #assert schedule links
        self.assertEqual(schedules[0]['participant-phone'], '06')
        self.assertEqual(schedules[0]['dialogue-id'], '0')
        self.assertEqual(schedules[0]['interaction-id'], '0')
        self.assertEqual(schedules[1]['interaction-id'], '1')
        
    def test04_schedule_participant_dialogue_offset_time(self):        
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

    #@inlineCallbacks
    def test05_send_scheduled_messages(self):
        dialogue = self.dialogue_annoucement_2
        participant = self.mkobj_participant('09')
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dNow = dNow - timedelta(minutes=2)
        dPast = dNow - timedelta(minutes=30)
        dFuture = dNow + timedelta(minutes=30)

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        unattached_message = self.collections['unattached_messages'].save({
            'date-time': time_to_vusion_format(dNow),
            'content': 'Hello unattached',
            'to': 'all participants',
            'type-interaction': 'annoucement'
        })
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dPast.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='0',
                participant_phone='09'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dNow.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='1',
                participant_phone='09'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dFuture.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='2',
                participant_phone='09'))
        self.collections['schedules'].save(
            self.mkobj_schedule_unattach(
                date_time=time_to_vusion_format(dNow),
                unattach_id=unattached_message,
                participant_phone='09'))
        self.collections['schedules'].save(
            self.mkobj_schedule_feedback(
                date_time=time_to_vusion_format(dNow),
                content='Thank you',
                participant_phone='09'))
        self.worker.load_data()

        self.worker.send_scheduled()

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[0]['content'], 'Today will be sunny')
        self.assertEqual(messages[1]['content'], 'Hello unattached')
        self.assertEqual(messages[2]['content'], 'Thank you')
        for message in messages:
            self.assertTrue('customized_id' in message['transport_metadata'])

        self.assertEquals(self.collections['schedules'].count(), 1)
        self.assertEquals(self.collections['history'].count(), 4)
        histories = self.collections['history'].find()
        for history in histories:
            self.assertTrue(history['participant-session-id'] is not None)

    @inlineCallbacks
    def test05_send_scheduled_deadline(self):  
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=2)   

        dialogue = self.mkobj_dialogue_open_question_reminder()
        participant = self.mkobj_participant('06')
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        self.collections['schedules'].save({
            'date-time': dPast.strftime(self.time_format),
            'object-type': 'deadline-schedule',
            'dialogue-id': '04',
            'interaction-id': '01-01',
            'participant-phone': '06'})
        yield self.worker.send_scheduled()
        
        saved_participant = self.collections['participants'].find_one()
        self.assertEqual(saved_participant['session-id'], None)

    @inlineCallbacks
    def test05_send_scheduled_run_action(self):  
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=2)   
        
        dialogue = self.mkobj_dialogue_open_question()
        participant = self.mkobj_participant('06')
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        self.collections['schedules'].save({
            'participant-phone': '06',
            'date-time': dPast.strftime(self.time_format),
            'object-type': 'action-schedule',
            'action': {'type-action': 'enrolling',
                       'enroll': '04'}})
        yield self.worker.send_scheduled()
        
        saved_participant = self.collections['participants'].find_one({'enrolled.dialogue-id': '04'})
        self.assertTrue(saved_participant)
        
    @inlineCallbacks
    def test05_send_scheduled_question_multi_keyword(self):
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dPast = dNow - timedelta(minutes=2)        
        
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dialogue = self.mkobj_dialogue_question_multi_keyword()
        participant = self.mkobj_participant('06')
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dPast.strftime(self.time_format),
                dialogue_id='05',
                interaction_id='05',
                participant_phone='06'))
        
        yield self.worker.send_scheduled()
        
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['content'], 'What is your gender?\n male or female')
    
    def test06_schedule_interaction_while_interaction_in_history(self):
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

    def test07_schedule_interaction_while_interaction_in_schedule(self):
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

    def test08_schedule_interaction_fixed_time_expired(self):
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

    def test08_schedule_interaction_offset_days_expired(self):
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


    def test08_schedule_interaction_offset_time_expired(self):
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

    def test09_schedule_at_fixed_time(self):
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
        
    def test10_schedule_participant_reminders(self):
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

    def test11_customize_message(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        interaction_using_tag = {
            'interaction-id': '0',
            'type-interaction': 'announcement',
            'content': 'Hello [participant.name]',
            'type-schedule': 'fixed-time',
            'date-time': '12/03/2012 12:30'
        }

        participant1 = self.mkobj_participant(
            '06',
            profile=[{'label': 'name',
                      'value': 'oliv'}])
        participant2 = self.mkobj_participant(
            '07',
            profile=[{'label': 'gender',
                      'value': 'Female'}])

        self.collections['participants'].save(participant1)
        self.collections['participants'].save(participant2)

        message_one = self.worker.generate_message(interaction_using_tag)
        message_one = self.worker.customize_message('06', message_one)
        self.assertEqual(message_one, 'Hello oliv')

        message_two = self.worker.generate_message(interaction_using_tag)
        self.assertRaises(MissingData, self.worker.customize_message, '07', message_two)

    #@inlineCallbacks
    def test12_generate_message_use_template(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        dialogue = self.mkobj_dialogue_question_offset_days()

        self.assertRaises(MissingTemplate, self.worker.generate_message, dialogue['interactions'][0])

        saved_template_id = self.collections['templates'].save(self.template_closed_question)
        self.collections['program_settings'].save(
            {'key': 'default-template-closed-question',
             'value': saved_template_id}
        )
        self.worker.load_data()

        close_question = self.worker.generate_message(dialogue['interactions'][0])

        self.assertEqual(
            close_question,
            "How are you?\n1. Fine\n2. Ok\n To reply send: FEEL<space><AnswerNb> to 8181")

        saved_template_id = self.collections['templates'].save(self.template_open_question)
        self.collections['program_settings'].save(
            {'key': 'default-template-open-question',
             'value': saved_template_id})
        self.collections['program_settings'].save(
            {'key': 'shortcode',
             'value': '+3123456'})
        self.worker.load_data()
        
        interaction = self.mkobj_dialogue_open_question()['interactions'][0]
        interaction['keyword'] = "name, nam"

        open_question = self.worker.generate_message(interaction)

        self.assertEqual(
            open_question,
            "What is your name?\n To reply send: NAME<space><name> to +3123456")

        self.collections['program_settings'].drop()
        self.collections['program_settings'].save(
            {'key': 'default-template-open-question',
             'value': ObjectId("4fc343509fa4da5e11000000")}
        )
        self.worker.load_data()

        self.assertRaises(MissingTemplate, self.worker.generate_message, interaction)
        
        self.collections['program_settings'].drop()
        self.collections['program_settings'].save(
            {'key': 'default-template-open-question',
             'value': ''}
        )
        self.worker.load_data()
        self.assertRaises(MissingTemplate, self.worker.generate_message, interaction)
        
    def test12_generate_message_question_multi_keyword_uses_no_template(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        interaction_question_multi_keyword = self.mkobj_dialogue_question_multi_keyword()['interactions'][0]
        
        question_multi_keyword = self.worker.generate_message(interaction_question_multi_keyword)
        
        self.assertEqual(question_multi_keyword, "What is your gender?\n male or female")        

    def test12_generate_message_no_template(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        interaction = self.mkobj_dialogue_question_offset_days()['interactions'][0]
        interaction.pop('set-use-template')
        
        close_question = self.worker.generate_message(interaction)
        self.assertEqual(
            close_question,
            "How are you?")

    @inlineCallbacks
    def test13_received_delivered(self):
        event = self.mkmsg_delivery_for_send()

        self.collections['history'].save({
            'message-id': event['user_message_id'],
            'message-direction': 'outgoing',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('delivered', status['message-status'])

    @inlineCallbacks
    def test14_received_delivered_no_reference(self):
        event = self.mkmsg_delivery_for_send()

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertNot(status)

    @inlineCallbacks
    def test15_received_delivered_failure(self):
        event = self.mkmsg_delivery_for_send(delivery_status='failed',
                                             failure_code='404',
                                             failure_level='http',
                                             failure_reason='some reason')

        self.collections['history'].save({
            'message-id': event['user_message_id'],
            'message-direction': 'outgoing',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('failed', status['message-status'])
        self.assertEqual('Code:404 Level:http Message:some reason',
                         status['failure-reason'])

    @inlineCallbacks
    def test16_received_ack(self):
        event = self.mkmsg_delivery_for_send(event_type='ack',
                                             user_message_id='2')

        self.collections['history'].save({
            'message-id': event['user_message_id'],
            'message-direction': 'outgoing',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('ack', status['message-status'])

    @inlineCallbacks
    def test17_receive_inbound_message_matching(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time()
        self.collections['dialogues'].save(self.mkobj_dialogue_question_offset_days())
        self.collections['participants'].save(self.mkobj_participant(
            '06',
            enrolled=[{'dialogue-id': '01', 
                       'date-time': time_to_vusion_format(dNow)}]))

        inbound_msg_matching = self.mkmsg_in(
            from_addr='06',
            content='Feel ok')
        yield self.send(inbound_msg_matching, 'inbound')
        
        #Only message matching keyword should be forwarded to the worker
        inbound_msg_non_matching_keyword = self.mkmsg_in(
            from_addr='06',
            content='ok')
        yield self.send(inbound_msg_non_matching_keyword, 'inbound')

        inbound_msg_non_matching_answer = self.mkmsg_in(
            from_addr='06',
            content='Feel good')
        yield self.send(inbound_msg_non_matching_answer, 'inbound')

        self.assertEqual(3, self.collections['history'].count())
        histories = self.collections['history'].find()
        self.assertEqual('01-01', histories[0]['interaction-id'])
        self.assertEqual('01', histories[0]['dialogue-id'])
        self.assertEqual('Ok', histories[0]['matching-answer'])
        self.assertEqual(None, histories[2]['matching-answer'])
        for history in histories:
            self.assertEqual('1', history['participant-session-id'])
        self.assertEqual(1, self.collections['schedules'].count())

    @inlineCallbacks
    def test17_receive_inbound_message_matching_offset_condition(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time()
        self.collections['dialogues'].save(self.mkobj_dialogue_open_question_offset_conditional())
        self.collections['participants'].save(self.mkobj_participant(
            '06',
            enrolled=[{'dialogue-id': '04', 
                       'date-time': time_to_vusion_format(dNow)}]))
        self.collections['history'].save(self.mkobj_history_dialogue(
            participant_phone='06',
            participant_session_id='1',
            dialogue_id='04',
            interaction_id='01-01',
            direction='outgoing',
            timestamp=time_to_vusion_format(dNow)
        ))
        
        inbound_msg_matching = self.mkmsg_in(
            from_addr='06',
            content='name olivier')
        yield self.send(inbound_msg_matching, 'inbound')
        
        self.assertEqual(1, self.collections['schedules'].count())

    @inlineCallbacks
    def test17_receive_inbound_message_matching_with_reminder(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=4)
        dFuture = dNow + timedelta(minutes=10)

        dialogue = self.mkobj_dialogue_open_question_reminder()
        self.collections['dialogues'].save(dialogue)
        participant = self.mkobj_participant(
            '06',
            session_id='1',
            enrolled=[{'dialogue-id':'04',
                       'date-time': time_to_vusion_format(dNow)}])
        self.collections['participants'].save(participant)
                
        dialogue['interactions'][0]['date-time'] = time_to_vusion_format(dPast)
        
        self.collections['history'].save(self.mkobj_history_dialogue(
            participant_phone='06',
            participant_session_id='1',
            direction='outgoing',
            dialogue_id='04',
            interaction_id='01-01',
            timestamp= time_to_vusion_format(dPast)))
        
        self.collections['schedules'].save(self.mkobj_schedule(
            dialogue_id='04',
            interaction_id='01-01',
            object_type='reminder-schedule',
            participant_phone='06',
            date_time=time_to_vusion_format(dFuture)))
        
        self.collections['schedules'].save(self.mkobj_schedule(
            dialogue_id='04',
            interaction_id='01-01',
            object_type = 'deadline-schedule',
            participant_phone='06',
            date_time=time_to_vusion_format(dFuture)))
          
        inbound_msg_matching = self.mkmsg_in(
            from_addr='06',
            content='name ok')
        
        yield self.send(inbound_msg_matching, 'inbound')

        self.assertEqual(0, self.collections['schedules'].count())   

    @inlineCallbacks
    def test17_receiving_inbound_message_no_repeat_dialogue_action(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dNow = self.worker.get_local_time()
        
        self.collections['dialogues'].save(self.mkobj_dialogue_open_question())
        self.collections['participants'].save(self.mkobj_participant(
            '06',
            enrolled=[{'dialogue-id':'04',
                       'date-time': time_to_vusion_format(dNow)}]))

        inbound_msg_matching_request = self.mkmsg_in(
            from_addr='06',
            content='name john doe')
        yield self.send(inbound_msg_matching_request, 'inbound')

        participant = self.collections['participants'].find_one({'phone': '06'})
        self.assertTrue('name' in participant['profile'][0]['label'])
        self.assertEqual('john doe', participant['profile'][0]['value'])
        
        ## One Way road, and action is not replayed
        inbound_msg_matching_request = self.mkmsg_in(
            from_addr='06',
            content='name olivier')
        yield self.send(inbound_msg_matching_request, 'inbound')
        participant = self.collections['participants'].find_one({'phone': '06'})
        self.assertEqual('john doe', participant['profile'][0]['value']) 

    @inlineCallbacks
    def test17_receiving_inbound_message_no_repeat_dialogue_enroll(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dNow = self.worker.get_local_time()
        
        self.collections['dialogues'].save(self.mkobj_dialogue_open_question_enroll_action('0'))
        self.collections['participants'].save(self.mkobj_participant(
            '06',
            enrolled=[{'dialogue-id':'04',
                       'date-time': time_to_vusion_format(dNow)}]))

        self.collections['history'].save(self.mkobj_history_dialogue(
            participant_phone='06',
            participant_session_id='1',
            direction = 'incoming',
            dialogue_id='04',
            interaction_id='01-01',
            matching_answer='1',
            timestamp=time_to_vusion_format(dNow)))

        inbound_msg_matching_request = self.mkmsg_in(
            from_addr='06',
            content='name john doe')

        yield self.send(inbound_msg_matching_request, 'inbound')

        participant = self.collections['participants'].find_one({'phone': '06'})
        self.assertEqual(1, len(participant['enrolled'])) 

    @inlineCallbacks
    def test17_receiving_inbound_message_only_enrolled(self):
        self.collections['dialogues'].save(self.mkobj_dialogue_open_question())
        self.collections['participants'].save(self.mkobj_participant('06'))

        inbound_msg_matching_request = self.mkmsg_in(
            from_addr='06',
            content='name john doe')
        yield self.send(inbound_msg_matching_request, 'inbound')

        participant = self.collections['participants'].find_one({'phone': '06'})
        self.assertEqual(participant['profile'], [])

    @inlineCallbacks
    def test17_receiving_inbound_request_not_optin(self):
        request_id = self.collections['requests'].save(self.mkobj_request_response())
      
        inbound_msg_matching_request = self.mkmsg_in(from_addr='07',
                                                     content='wWw info')
        yield self.send(inbound_msg_matching_request, 'inbound')
        
        self.assertEqual(0, self.collections['schedules'].count())
        
    @inlineCallbacks
    def test17_receiving_inbound_request_optin(self):
        request_id = self.collections['requests'].save(self.mkobj_request_join())
      
        inbound_msg_matching_request = self.mkmsg_in(from_addr='07',
                                                     content='www')
        yield self.send(inbound_msg_matching_request, 'inbound')
        
        self.assertEqual(2, self.collections['schedules'].count())
        self.assertFalse(self.collections['participants'].find_one({'phone': '07'}) is None)

    @inlineCallbacks
    def test17_receiving_inbound_message_request_optin(self):
        request_id = self.collections['requests'].save(self.mkobj_request_join())
      
        inbound_msg_matching_request = self.mkmsg_in(from_addr='07',
                                                     content='wWw')
        yield self.send(inbound_msg_matching_request, 'inbound')

        inbound_msg_matching_request = self.mkmsg_in(from_addr='08',
                                                     content='www join')
        yield self.send(inbound_msg_matching_request, 'inbound')

        self.assertEqual(2, self.collections['history'].count())
        self.assertEqual(2, self.collections['participants'].count())
        self.assertEqual(4, self.collections['schedules'].count())
        histories = self.collections['history'].find({'request-id':{'$exists':True}})
        participant = self.collections['participants'].find_one({'phone': '07'})
        self.assertEqual(histories.count(), 2)
        self.assertEqual(histories[0]['request-id'], request_id)
        self.assertEqual(histories[1]['request-id'], request_id)
        self.assertEqual(participant['session-id'], histories[0]['participant-session-id'])

    @inlineCallbacks
    def test17_receiving_inbound_message_from_non_participant(self):
        self.collections['requests'].save(self.mkobj_request_join())
        self.collections['requests'].save(self.mkobj_request_tag())
        self.collections['requests'].save(self.mkobj_request_leave())
        
        # No action in case never optin
        inbound_msg_matching = self.mkmsg_in(from_addr='06',
                                             content='www tagme')
        yield self.send(inbound_msg_matching, 'inbound')
        
        self.assertEqual(0, self.collections['participants'].count())
        self.assertEqual(0, self.collections['schedules'].count())
        self.assertEqual(1, self.collections['history'].count())

        # Still participant can optin
        inbound_msg_matching = self.mkmsg_in(from_addr='06',
                                             content='www join')
        yield self.send(inbound_msg_matching, 'inbound')
        self.assertEqual(1, self.collections['participants'].count())
        self.assertEqual(2, self.collections['history'].count())
        
        # When they optout no action is performed
        inbound_msg_matching = self.mkmsg_in(from_addr='06',
                                             content='www quit')
        yield self.send(inbound_msg_matching, 'inbound')
        self.assertEqual(3, self.collections['history'].count())
        inbound_msg_matching = self.mkmsg_in(from_addr='06',
                                             content='www tagme')
        yield self.send(inbound_msg_matching, 'inbound')
        self.assertEqual(None, self.collections['participants'].find_one({'tags':'onetag'}))
        self.assertEqual(4, self.collections['history'].count())

    def test18_run_action(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        self.collections['participants'].save(self.mkobj_participant(
            '08',
            tags=['geek'],
            profile=[{'label': 'name',
                     'value': 'Oliv'}]))
        
        ## Error message
        saved_template_id = self.collections['templates'].save(
            self.template_unmatching_answer)
        self.collections['program_settings'].save(
            {'key': 'default-template-unmatching-answer',
             'value': saved_template_id})

        self.worker.run_action("08", FeedbackAction(**{'content': 'message'}))
        self.assertEqual(1, self.collections['schedules'].count())

        self.worker.run_action("08", UnMatchingAnswerAction(**{'answer': 'best'}))
        unmatching_template = self.collections['program_settings'].find_one({
            'key': 'default-template-unmatching-answer'})
        self.assertEqual(saved_template_id, unmatching_template['value'])
        self.assertEqual(2, self.collections['schedules'].count())
        schedules = self.collections['schedules'].find()
        self.assertEqual(schedules[1]['content'],
                         "best does not match any answer")

        ## Tagging
        self.worker.run_action("08", TaggingAction(**{'tag': 'my tag'}))
        self.worker.run_action("08", TaggingAction(**{'tag': 'my second tag'}))
        self.assertTrue(self.collections['participants'].find_one({'tags': 'my tag'}))
        self.worker.run_action("08", TaggingAction(**{'tag': 'my tag'}))
        self.assertEqual(
            ['geek', 'my tag', 'my second tag'],
            self.collections['participants'].find_one({'tags': 'my tag'})['tags'])
        
        ## Profiling
        self.worker.run_action("08", ProfilingAction(**{'label': 'gender',
                                                        'value': 'Female'}))
        self.assertTrue(self.collections['participants'].find_one({'profile.label': 'gender'}))
        self.assertTrue(self.collections['participants'].find_one({'profile.value': 'Female'}))


    def test18_run_action_enroll(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time();
        self.collections['participants'].save(self.mkobj_participant(
            "08", last_optin_date=time_to_vusion_format(dNow)))        
        self.collections['dialogues'].save(
            self.mkobj_dialogue_question_offset_days())
        dBegin = self.worker.get_local_time()
     
        self.worker.run_action("08", EnrollingAction(**{'enroll': '01'}))
        participant = self.collections['participants'].find_one({'enrolled.dialogue-id':'01'})
        self.assertTrue(participant)
        self.assertEqual(1, self.collections['schedules'].count())
        self.assertTrue('date-time' in participant['enrolled'][0])
        dEnrolled = time_from_vusion_format(participant['enrolled'][0]['date-time'])
        self.assertTrue(dEnrolled - dBegin < timedelta(seconds=1))
       
        #Enrolling again should keep the old date
        self.worker.run_action("08", EnrollingAction(**{'enroll': '01'}))
        participant = self.collections['participants'].find_one({'phone': '08'})
        self.assertEqual(1, len(participant['enrolled']))
        self.assertEqual(
            dEnrolled,
            time_from_vusion_format(participant['enrolled'][0]['date-time']))

        #Enrolling a new number will opt it in
        self.worker.run_action("09", EnrollingAction(**{'enroll': '01'}))
        participant = self.collections['participants'].find_one({'phone': '09', 'enrolled.dialogue-id':'01'})
        self.assertTrue(participant)
        self.assertEqual(participant['session-id'], RegexMatcher(r'^[0-9a-fA-F]{32}$'))
    
    def test18_run_action_enroll_again(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(days=1)
        dialogue = self.mkobj_dialogue_question_offset_days()
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(self.mkobj_participant(
            "08", 
            last_optin_date=time_to_vusion_format(dPast),
            enrolled=[{'dialogue-id': dialogue['dialogue-id'],
                       'date-time': time_to_vusion_format(dPast)}]))
        
        self.worker.run_action("08", EnrollingAction(**{'enroll': '01'}))
        
        participant = self.collections['participants'].find_one({'phone': '08'})
        self.assertEqual(
            time_to_vusion_format(dPast),
            participant['enrolled'][0]['date-time'])
        
    def test18_run_action_enroll_auto_enroll(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        dialogue = self.mkobj_dialogue_annoucement()        
        self.collections['dialogues'].save(dialogue)
        
        self.worker.run_action("04", OptinAction())
        
        self.assertTrue(self.collections['participants'].find_one({'enrolled.dialogue-id':'0'}) is not None)
        self.assertEqual(1, self.collections['schedules'].count())

    def test18_run_action_enroll_clear_profile_if_not_optin(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dialogue = self.mkobj_dialogue_question_offset_days()
        self.collections['dialogues'].save(dialogue)        
        self.collections['participants'].save(self.mkobj_participant(
            participant_phone='06',
            last_optin_date=None,
            session_id=None,
            tags=['geeks'],
            profile=[{'lable':'name',
                      'value':'Oliv'}],
            enrolled=[{'dialogue-id': '01',
                       'date-time': '2012-08-08T12:36:20'}]))
        dNow = self.worker.get_local_time()
        
        self.worker.run_action("06", EnrollingAction(**{'enroll': '01'}))
        
        participant = self.collections['participants'].find_one({'phone':'06'})
        self.assertEqual(participant['tags'], [])
        self.assertEqual(participant['profile'], [])
        self.assertEqual(participant['enrolled'][0]['dialogue-id'], '01')
        self.assertTrue(
            dNow - time_from_vusion_format(participant['enrolled'][0]['date-time']) < timedelta(seconds=1))

    def test18_run_action_delayed_enrolling(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        dialogue = self.mkobj_dialogue_question_offset_days()
        dNow = self.worker.get_local_time();
        self.collections['participants'].save(self.mkobj_participant(
            "08", last_optin_date=time_to_vusion_format(dNow)))        
        self.collections['dialogues'].save(dialogue)
     
        self.worker.run_action(
            "08",
            DelayedEnrollingAction(**{
                'enroll': '01',
                'offset-days': {'days':'1', 'at-time': '12:00'}}),
            origin={'dialogue-id': '02'}
        )

        schedule = self.collections['schedules'].find_one({'object-type': 'action-schedule'})
        self.assertTrue(schedule is not None)
        self.assertEqual(schedule['dialogue-id'], '02')
        self.assertTrue(
            action_generator(**schedule['action']),
            EnrollingAction(**{'enroll': '01'}))

    def test18_run_action_optin_optout(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        ## Participant optin
        self.worker.run_action("08", OptinAction())
        self.assertEqual(1, self.collections['participants'].count())
        participant = self.collections['participants'].find_one()
        self.assertTrue('session-id' in participant)
        self.assertEqual(participant['session-id'], RegexMatcher(r'^[0-9a-fA-F]{32}$'))
        self.assertTrue('last-optin-date' in participant)
        self.assertEqual(participant['last-optin-date'], RegexMatcher(r'^(\d{4})-0?(\d+)-0?(\d+)[T ]0?(\d+):0?(\d+):0?(\d+)$'))
        self.assertTrue('tags' in participant)
        self.assertTrue('profile' in participant)
        self.assertTrue('enrolled' in participant)

        ## Participant optout (All schedule messages should be removed)
        self.collections['schedules'].save(self.mkobj_schedule("08"))
        self.collections['schedules'].save(self.mkobj_schedule("06"))        
        self.worker.run_action("08", OptoutAction())
        self.assertEqual(1, self.collections['participants'].count())
        participant_optout = self.collections['participants'].find_one()
        self.assertTrue(participant_optout['session-id'] is None)
        self.assertTrue(participant_optout['last-optin-date'] is None)
        self.assertEqual(1, self.collections['schedules'].count())
        
        ## Participant can optin again
        self.worker.run_action("08", OptinAction())
        self.assertEqual(1, self.collections['participants'].count())
        participant = self.collections['participants'].find_one()
        self.assertEqual(participant['session-id'], RegexMatcher(r'^[0-9a-fA-F]{32}$'))
        self.assertEqual(participant['last-optin-date'], RegexMatcher(r'^(\d{4})-0?(\d+)-0?(\d+)[T ]0?(\d+):0?(\d+):0?(\d+)$'))

        ## Participant cannot optin while they are aleardy optin
        self.worker.run_action("08", OptinAction())
        self.assertEqual(1, self.collections['participants'].count())
        participant_reoptin = self.collections['participants'].find_one()
        self.assertEqual(participant['session-id'], participant_reoptin['session-id'])
        self.assertEqual(participant['last-optin-date'], participant_reoptin['last-optin-date'])
        
        ## Participant profile is cleared by optin
        self.collections['participants'].save(self.mkobj_participant(
            participant_phone='06',
            last_optin_date=None,
            session_id=None,
            tags=['geeks'],
            profile=[{'label': 'name',
                      'value': 'Oliv'}],
            enrolled=['1']
        ))
        self.worker.run_action("06", OptinAction())
        participant = self.collections['participants'].find_one({'phone':'06'})
        self.assertEqual(participant['tags'], [])
        self.assertEqual(participant['profile'], [])
        self.assertEqual(participant['enrolled'], [])
        
        ## Participant profile is not cleard by optout
        self.collections['participants'].save(self.mkobj_participant(
            participant_phone='07',
            tags=['geeks'],
            profile=[{'label': 'name',
                      'value':'Oliv'}],
            enrolled=['1']
        ))
        self.worker.run_action("06", OptoutAction())
        participant = self.collections['participants'].find_one({'phone':'07'})
        self.assertEqual(participant['tags'], ['geeks'])
        self.assertEqual(participant['profile'], [{'label': 'name',
                                                   'value':'Oliv'}])
        self.assertEqual(participant['enrolled'], ['1'])
        

    def test18_run_action_offset_condition(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time()
       
        self.collections['dialogues'].save(self.mkobj_dialogue_question_offset_conditional())
        self.collections['dialogues'].save(self.mkobj_dialogue_open_question_offset_conditional())
        self.collections['participants'].save(self.mkobj_participant('06'))

        self.save_history(
            timestamp=dNow - timedelta(minutes=30),
            participant_phone='06',
            participant_session_id='1',
            metadata={'dialogue-id':'01',
                      'interaction-id':'01-01'})

        self.save_history(
            timestamp=dNow - timedelta(minutes=30),
            participant_phone='06',
            participant_session_id='1',
            metadata={'dialogue-id':'04',
                      'interaction-id':'01-01'})
        
        # a non matching answer do not trigger the offsetcondition
        self.save_history(
            timestamp=dNow,
            participant_phone='06',
            participant_session_id='1',
            message_direction='incoming',
            metadata={'dialogue-id':'01',
                      'interaction-id':'01-01',
                      'matching-answer':None})

        # Need to store the message into the history
        self.worker.run_action("06", OffsetConditionAction(**{
            'dialogue-id': '01',
            'interaction-id':'01-02'}))
        self.assertEqual(self.collections['schedules'].count(),
                         0)
        
        self.save_history(
            timestamp=dNow,
            participant_phone='06',
            participant_session_id='1',
            message_direction="incoming",
            metadata={'dialogue-id':'01',
                      'interaction-id':'01-01',
                      'matching-answer':'Fine'})

        # Need to store the message into the history
        self.worker.run_action("06", OffsetConditionAction(**{
            'dialogue-id': '01',
            'interaction-id':'01-02'}))
        self.assertEqual(self.collections['schedules'].count(),
                         2)
        
        # Do not reschedule
        self.worker.run_action("06", OffsetConditionAction(**{
            'dialogue-id': '01',
            'interaction-id':'01-02'}))        
        self.assertEqual(self.collections['schedules'].count(),
                         2)
        
        # Do send if open question
        self.save_history(
            timestamp=dNow,
            participant_phone='06',
            participant_session_id='1',
            message_direction='incoming',
            metadata={'dialogue-id':'04',
                      'interaction-id':'01-01'})

        self.worker.run_action("06", OffsetConditionAction(**{
            'dialogue-id': '04',
            'interaction-id':'01-01'}))        
        self.assertEqual(
            self.collections['schedules'].count(),
            3)
        
    def test18_run_action_remove_reminders(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=30)
       
        dialogue = self.mkobj_dialogue_open_question_reminder()
        participant = self.mkobj_participant('06')
        
        interaction = dialogue['interactions'][0]
        interaction['date-time'] = time_to_vusion_format(dPast)
        self.worker.schedule_participant_reminders(
            participant, dialogue, interaction, time_from_vusion_format(interaction['date-time']))

        schedules_count = self.collections['schedules'].count()
        self.assertEqual(schedules_count, 3)
        
        self.worker.run_action("06", RemoveRemindersAction(**{
            'dialogue-id': dialogue['dialogue-id'],
            'interaction-id': interaction['interaction-id']}))        
        self.assertEqual(self.collections['schedules'].count(), 1)
        self.assertEqual(self.collections['schedules'].find_one({'object-type':'reminder-schedule'}), None)
        self.assertTrue(self.collections['schedules'].find_one({'object-type':'deadline-schedule'}) is not None)
        
        self.worker.run_action('06', RemoveDeadlineAction(**{'dialogue-id': dialogue['dialogue-id'],
                                                           'interaction-id': interaction['interaction-id']}))        
        self.assertEqual(self.collections['schedules'].count(), 0) 

    def test18_run_action_reset(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time()
        
        participant = self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dNow),
            profile=[{'label': 'name',
                     'value': 'Oliv'}])
        self.collections['participants'].save(participant)
        
        self.worker.run_action("06", ResetAction())
        
        reset_participant = self.collections['participants'].find_one({'phone':'06'})
        
        self.assertEqual(reset_participant['profile'], [])

    def test21_schedule_unattach_message(self):
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

    @inlineCallbacks
    def test22_register_keywords_in_dispatcher(self):
        self.collections['dialogues'].save(self.dialogue_question)
        self.collections['requests'].save(self.mkobj_request_join())
        self.collections['requests'].save(self.request_leave)
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        yield self.worker.register_keywords_in_dispatcher()

        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(1, len(messages))
        self.assertEqual([
            {'app': 'test', 'keyword': 'feel', 'to_addr': '8181', 'prefix': '+256'},
            {'app': 'test', 'keyword': 'fel', 'to_addr': '8181', 'prefix': '+256'},
            {'app': 'test', 'keyword': 'www', 'to_addr': '8181', 'prefix': '+256'}],
            messages[0]['rules'])

    @inlineCallbacks
    def test22_register_keywords_in_dispatcher_international(self):
        self.collections['requests'].save(self.mkobj_request_join())
        for program_setting in self.mkobj_program_settings_international_shortcode():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        yield self.worker.register_keywords_in_dispatcher()

        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(1, len(messages))
        self.assertEqual([
            {'app': 'test', 'keyword': 'www', 'to_addr': '+318181'}],
            messages[0]['rules'])

    @inlineCallbacks
    def test22_daemon_shortcode_updated(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        ## load a first time the properties
        self.worker.load_data() 
        for program_setting in self.mkobj_program_settings_international_shortcode():
            self.collections['program_settings'].save(program_setting)
    
        yield self.worker.daemon_process()
        
        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(1, len(messages))

    @inlineCallbacks
    def test23_test_send_all_messages(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        yield self.worker.send_all_messages(self.dialogue_announcement, '06')

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]['content'], "Hello")
        self.assertEqual(messages[0]['to_addr'], "06")
        self.assertEqual(messages[1]['content'], "How are you")
        self.assertEqual(messages[1]['to_addr'], "06")

    #TODO: last 2 tests are not isolate, somehow the starting of the worker
    # is called later which is breacking the other tests
    #TODO: reduce the scope of the update-schedule
    @inlineCallbacks
    def test24_consume_control_update_schedule(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time()

        self.collections['dialogues'].save(self.dialogue_announcement)
        self.collections['dialogues'].save(self.dialogue_question)
        self.collections['participants'].save(
            self.mkobj_participant(
                participant_phone='08',
                enrolled=[{'dialogue-id': '0', 'date-time': time_to_vusion_format(dNow)}]))
        self.collections['participants'].save(
            self.mkobj_participant(
                participant_phone='09',
                enrolled=[{'dialogue-id': '01', 'date-time': time_to_vusion_format(dNow)},
                          {'dialogue-id': '0', 'date-time': time_to_vusion_format(dNow)}]))
        ##optout
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='10', session_id=None))
        self.collections['participants'].save(
            self.mkobj_participant(
                participant_phone='11',
                session_id=None, 
                enrolled=[{'dialogue-id': '01', 'date-time': time_to_vusion_format(dNow)},
                          {'dialogue-id': '0', 'date-time': time_to_vusion_format(dNow)}]))

        event = self.mkmsg_dialogueworker_control('update-schedule')
        yield self.send(event, 'control')
        self.assertEqual(5, self.collections['schedules'].count())

        self.collections['unattached_messages'].save(
            self.mkobj_unattach_message())

        event = self.mkmsg_dialogueworker_control('update-schedule')
        yield self.send(event, 'control')
        self.assertEqual(7, self.collections['schedules'].count())

    @inlineCallbacks
    def test25_consume_control_test_send_all_messages(self):
        dialogue_id = self.collections['dialogues'].save(
            self.mkobj_dialogue_annoucement())
        self.collections['participants'].save(self.mkobj_participant('08'))
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        event = self.mkmsg_dialogueworker_control('test-send-all-messages',
                                                  dialogue_id.__str__(),
                                                  phone_number='08')
        yield self.send(event, 'control')

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
