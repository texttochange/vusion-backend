from datetime import datetime, time, date, timedelta
import pytz
from copy import deepcopy

import json
import pymongo
from bson.objectid import ObjectId
from bson.timestamp import Timestamp

from twisted.trial.unittest import TestCase

from vumi.tests.utils import get_stubbed_worker, UTCNearNow, RegexMatcher

from vusion.dialogue_worker import DialogueWorker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.error import MissingData, MissingTemplate
from vusion.persist.action import (UnMatchingAnswerAction, EnrollingAction,
                                   FeedbackAction, OptinAction, OptoutAction,
                                   TaggingAction, ProfilingAction,
                                   OffsetConditionAction, RemoveRemindersAction,
                                   ResetAction, RemoveDeadlineAction,
                                   DelayedEnrollingAction, ProportionalTagging,
                                   action_generator, Actions)
from vusion.context import Context
from vusion.persist import Dialogue

from tests.utils import MessageMaker, DataLayerUtils, ObjectMaker
from vusion.tests.test_dialogue_worker import DialogueWorkerTestCase


class DialogueWorkerTestCase_runAction(DialogueWorkerTestCase):

    def test_run_action_unmatching_answer(self):
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

        context = Context()
        context.update({'request-id':'1'})

        self.worker.run_action(
            '08',
            UnMatchingAnswerAction(**{'answer': 'best'}),
            context,
            '1')
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
        self.assertEqual(self.collections['history'].count(), 1)
        history = self.collections['history'].find_one()
        self.assertEqual(history['participant-session-id'], '1')
        self.assertEqual(history['participant-phone'], '08')
        self.assertEqual(history['message-content'],
                         "best does not match any answer")

    def test_run_action_tagging(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        self.collections['participants'].save(self.mkobj_participant(
            '08',
            tags=['geek'],
            profile=[{'label': 'name',
                     'value': 'Oliv'}]))

        ## Tagging
        self.worker.run_action("08", TaggingAction(**{'tag': 'my tag'}))
        self.worker.run_action("08", TaggingAction(**{'tag': 'my second tag'}))
        self.assertTrue(self.collections['participants'].find_one({'tags': 'my tag'}))
        self.worker.run_action("08", TaggingAction(**{'tag': 'my tag'}))
        self.assertEqual(
            ['geek', 'my tag', 'my second tag'],
        self.collections['participants'].find_one({'tags': 'my tag'})['tags'])

    def test_run_action_profiling(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        self.collections['participants'].save(self.mkobj_participant(
            '08',
            tags=['geek'],
            profile=[{'label': 'name',
                     'value': 'Oliv'}]))

        self.worker.run_action("08", ProfilingAction(**{'label': 'gender',
                                                        'value': 'Female'}))
        self.assertTrue(self.collections['participants'].find_one({'profile.label': 'gender'}))
        self.assertTrue(self.collections['participants'].find_one({'profile.value': 'Female'}))


    def test_run_action_feedback(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        self.collections['participants'].save(self.mkobj_participant())
        
        context = Context()
        context.update({'request-id': '1'})
        
        self.worker.run_action(
            '06', 
            FeedbackAction(**{'content': 'message'}),
            context,
            '1')
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
        self.assertEqual(self.collections['history'].count(), 1)
        history = self.collections['history'].find_one()
        self.assertEqual(history['participant-session-id'], '1')
        self.assertEqual(history['participant-phone'], '06')

    def test_run_action_enroll(self):
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
    
    def test_run_action_enroll_again(self):
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
        
    def test_run_action_enroll_auto_enroll(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        dialogue = self.mkobj_dialogue_annoucement()        
        self.collections['dialogues'].save(dialogue)
        
        self.worker.run_action("04", OptinAction())
        
        self.assertTrue(self.collections['participants'].find_one({'enrolled.dialogue-id':'0'}) is not None)
        self.assertEqual(1, self.collections['schedules'].count())

    def test_run_action_enroll_clear_profile_if_not_optin(self):
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
            profile=[{'label':'name',
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

    def test_run_action_delayed_enrolling(self):
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
            Context(**{'dialogue-id': '02', 'interaction-id': '1'}))

        schedule = self.collections['schedules'].find_one({'object-type': 'action-schedule'})
        self.assertTrue(schedule is not None)
        self.assertEqual(schedule['context']['dialogue-id'], '02')
        self.assertEqual(schedule['context']['interaction-id'], '1')        
        self.assertTrue(
            action_generator(**schedule['action']),
            EnrollingAction(**{'enroll': '01'}))

    def test_run_action_optin_optout(self):
        regex_time = RegexMatcher(r'^(\d{4})-0?(\d+)-0?(\d+)[T ]0?(\d+):0?(\d+):0?(\d+)$')
        
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
        self.assertEqual(participant['last-optin-date'], regex_time)
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
        self.assertEqual(participant_optout['last-optin-date'], regex_time)
        self.assertEqual(participant_optout['last-optout-date'], regex_time)
        self.assertEqual(1, self.collections['schedules'].count())
        
        ## Participant can optin again
        self.worker.run_action("08", OptinAction())
        self.assertEqual(1, self.collections['participants'].count())
        participant = self.collections['participants'].find_one()
        self.assertEqual(participant['session-id'], RegexMatcher(r'^[0-9a-fA-F]{32}$'))
        self.assertEqual(participant['last-optin-date'], regex_time)
        self.assertEqual(participant['last-optout-date'], None)

        ## Participant cannot optin while they are aleardy optin
        self.worker.run_action("08", OptinAction())
        self.assertEqual(1, self.collections['participants'].count())
        participant_reoptin = self.collections['participants'].find_one()
        self.assertEqual(participant['session-id'], participant_reoptin['session-id'])
        self.assertEqual(participant['last-optin-date'], participant_reoptin['last-optin-date'])
        self.assertEqual(participant['last-optout-date'], None)
        
        ## Participant profile is cleared by optin
        self.collections['participants'].save(self.mkobj_participant(
            participant_phone='06',
            last_optin_date=None,
            session_id=None,
            tags=['geeks'],
            profile=[{'label': 'name',
                      'value': 'Oliv'}],
            enrolled=[{'dialogue-id':'1', 
                       'date-time': '2012-11-01T10:30:20'}]
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
            enrolled=[{'dialogue-id':'1', 
                       'date-time': '2012-11-01T10:30:20'}]
        ))
        self.worker.run_action("06", OptoutAction())
        participant = self.collections['participants'].find_one({'phone':'07'})
        self.assertEqual(participant['tags'], ['geeks'])
        self.assertEqual(participant['profile'], [{'label': 'name',
                                                   'value':'Oliv', 
                                                   'raw': None}])
        self.assertEqual(participant['enrolled'], [
            {'dialogue-id':'1', 
             'date-time': '2012-11-01T10:30:20'}])

    def test_run_action_optin_double_option_no_setting(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
            
        self.collections['participants'].save(self.mkobj_participant())           
        
        self.worker.run_action('06', OptinAction())
        
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 0)
        self.assertEqual(self.collections['history'].count(), 0)
        
    def test_run_action_optin_double_option_error_message_in_setting(self):
        program_settings = deepcopy(self.program_settings)
        program_settings.append({'key': 'double-optin-error-feedback',
                                 'value': 'You are double optin'})
        for program_setting in program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()        
        
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='06', session_id='1'))        
        
        self.worker.run_action('06', OptinAction(),  Context(**{'request-id': '22'}), '1')
        
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
        self.assertEqual(self.collections['history'].count(), 1)
        
        history = self.collections['history'].find_one()
        self.assertEqual(history['participant-session-id'], '1')
        self.assertEqual(history['participant-phone'], '06')


    def test_run_action_offset_condition(self):
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
        
    def test_run_action_remove_reminders(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=30)
       
        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder_offset_time())
        participant = self.mkobj_participant('06')
        
        interaction = dialogue.interactions[0]
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

    def test_run_action_reset(self):
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

    def test_run_conditional_action(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        self.collections['participants'].save(self.mkobj_participant(
            '08',
            session_id='01',
            tags=['geek'],
            profile=[{'label': 'name',
                     'value': 'Oliv'}]))

        ## Simple Condition
        self.worker.run_action("08", TaggingAction(**{
            'model-version': '2',
            'set-condition': 'condition',
            'condition-operator': 'all-subconditions',
            'subconditions':[{
                'subcondition-field': 'labelled',
                'subcondition-operator': 'not-with',
                'subcondition-parameter': 'name:Oliv'
            }],
            'tag': 'my tag'}),
            participant_session_id='01')
        participant = self.collections['participants'].find_one({'phone':'08'})
        self.assertEqual(
            ['geek'],
            participant['tags'])
                
        self.worker.run_action("08", TaggingAction(**{
            'model-version': '2',
            'set-condition': 'condition',
            'condition-operator': 'all-subconditions',
            'subconditions':[{
                'subcondition-field': 'tagged',
                'subcondition-operator': 'with',
                'subcondition-parameter': 'imported'
            }],
            'tag': 'marker'}),
            participant_session_id='01')
        participant = self.collections['participants'].find_one({'phone':'08'})
        self.assertEqual(
            ['geek'],
            participant['tags'])        
        
        ## Complex Condition
        self.worker.run_action(
            "08", 
            TaggingAction(**{
                'model-version': '2',
                'set-condition': 'condition',
                'condition-operator': 'any-subconditions',
                'subconditions':[
                    {'subcondition-field': 'labelled',
                     'subcondition-operator': 'not-with',
                     'subcondition-parameter': 'name:Oliv'},
                    {'subcondition-field': 'tagged',
                     'subcondition-operator': 'with',
                     'subcondition-parameter': 'geek'},                
                    ],
                'tag': 'my second tag'}),
            participant_session_id='01')
        participant = self.collections['participants'].find_one({'phone':'08'})
        self.assertEqual(
            ['geek', 'my second tag'],
            participant['tags'])        

    def test_run_action_proportional_tagging(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        self.collections['participants'].save(self.mkobj_participant(
            '08',
            tags=['geek'],
            profile=[{'label': 'name',
                     'value': 'Oliv'}]))

        proportional_tagging = ProportionalTagging(**{
            'proportional-tags': [{'tag': 'Group A', 'weight': '1'}]})
        ## Tagging
        self.worker.run_action("08", proportional_tagging)
        self.assertTrue(self.collections['participants'].find_one({'tags': 'Group A'}))


    def test_run_action_proportional_tagging_double(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        self.collections['participants'].save(self.mkobj_participant(
            '08',
            tags=['geek', 'GroupB'],
            profile=[{'label': 'name',
                     'value': 'Oliv'}]))

        proportional_tagging = ProportionalTagging(**{
            'proportional-tags': [{'tag': 'GroupA', 'weight': '1'},
                                  {'tag': 'GroupB', 'weight': '1'}]})
        ## Tagging
        self.worker.run_action("08", proportional_tagging)
        participant = self.collections['participants'].find_one()
        self.assertEqual(participant['tags'], ['geek', 'GroupB'])
