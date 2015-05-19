from datetime import datetime, time, date, timedelta
import pytz
from copy import deepcopy

import json
import pymongo
from bson.objectid import ObjectId
from bson.timestamp import Timestamp

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.tests.utils import get_stubbed_worker, UTCNearNow, RegexMatcher

from vusion.dialogue_worker import DialogueWorker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.error import MissingData, MissingTemplate
from vusion.persist.action import (
    UnMatchingAnswerAction, EnrollingAction,
    FeedbackAction, OptinAction, OptoutAction,
    TaggingAction, ProfilingAction,
    OffsetConditionAction, RemoveRemindersAction,
    ResetAction, RemoveDeadlineAction,
    DelayedEnrollingAction,
    ProportionalTagging, ProportionalLabelling,
    action_generator, Actions, UrlForwarding, SmsForwarding, SmsInviteAction,
    SaveContentVariableTable)
from vusion.context import Context
from vusion.persist import Dialogue, DialogueHistory

from tests.utils import MessageMaker, DataLayerUtils, ObjectMaker
from vusion.tests.test_dialogue_worker import DialogueWorkerTestCase


class DialogueWorkerTestCase_runAction(DialogueWorkerTestCase):

    def test_run_action_unmatching_answer(self):
        self.initialize_properties()

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
        context.update({'request-id': '1'})

        self.worker.run_action(
            '08',
            UnMatchingAnswerAction(**{'answer': 'best'}),
            context,
            '1')
        messages = self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(self.collections['history'].count(), 1)
        history = self.collections['history'].find_one()
        self.assertEqual(history['participant-session-id'], '1')
        self.assertEqual(history['participant-phone'], '08')
        self.assertEqual(history['message-content'],
                         "best does not match any answer")

    @inlineCallbacks
    def test_run_action_tagging(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dFuture = dNow + timedelta(minutes=30)

        self.collections['participants'].save(self.mkobj_participant(
            '08',
            tags=['geek'],
            profile=[{'label': 'name',
                     'value': 'Oliv'}]))

        ## tagging might make the participant eligible
        unattach_msg = self.mkobj_unattach_message(
            content='Hello',
            send_to_type='match',
            send_to_match_operator='all',
            send_to_match_conditions=['my tag'],
            fixed_time=time_to_vusion_format(dFuture))
        self.collections['unattached_messages'].save(unattach_msg)

        ## Tagging
        yield self.worker.run_action("08", TaggingAction(**{'tag': 'my tag'}))
        yield self.worker.run_action("08", TaggingAction(**{'tag': 'my second tag'}))
        self.assertTrue(self.collections['participants'].find_one({'tags': 'my tag'}))
        yield self.worker.run_action("08", TaggingAction(**{'tag': 'my tag'}))
        self.assertEqual(
            ['geek', 'my tag', 'my second tag'],
            self.collections['participants'].find_one({'tags': 'my tag'})['tags'])
        self.assertEqual(1, self.collections['schedules'].find({'participant-phone': '08'}).count())

    @inlineCallbacks
    def test_run_action_profiling(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dFuture = dNow + timedelta(minutes=30)

        self.collections['participants'].save(self.mkobj_participant(
            '08',
            tags=['geek'],
            profile=[{'label': 'name',
                     'value': 'Oliv'}]))

        ## tagging might make the participant eligible
        unattach_msg = self.mkobj_unattach_message(
            content='Hello',
            send_to_type='match',
            send_to_match_operator='all',
            send_to_match_conditions=['gender:Female'],
            fixed_time=time_to_vusion_format(dFuture))
        self.collections['unattached_messages'].save(unattach_msg)

        yield self.worker.run_action("08",
                                     ProfilingAction(**{
                                         'label': 'gender',
                                         'value': 'Female'}))
        self.assertTrue(self.collections['participants'].find_one({'profile.label': 'gender'}))
        self.assertTrue(self.collections['participants'].find_one({'profile.value': 'Female'}))
        self.assertEqual(1, self.collections['schedules'].find({'participant-phone':'08'}).count())

    @inlineCallbacks
    def test_run_action_feedback(self):
        self.initialize_properties()

        self.collections['participants'].save(self.mkobj_participant())

        context = Context()
        context.update({'request-id': '1'})

        self.worker.run_action(
            '06',
            FeedbackAction(**{'content': 'message'}),
            context,
            '1')
        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(self.collections['history'].count(), 1)
        history = self.collections['history'].find_one()
        self.assertEqual(history['participant-session-id'], '1')
        self.assertEqual(history['participant-phone'], '06')

    def test_run_action_enroll(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        self.collections['participants'].save(self.mkobj_participant(
            "08", last_optin_date=time_to_vusion_format(dNow)))        
        self.collections['dialogues'].save(
            self.mkobj_dialogue_question_offset_days())
        dBegin = self.worker.get_local_time()
     
        self.worker.run_action("08", EnrollingAction(**{'enroll': '01'}))
        participant = self.collections['participants'].find_one({'enrolled.dialogue-id': '01'})
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
        self.initialize_properties()

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
        self.initialize_properties()

        dialogue = self.mkobj_dialogue_announcement()        
        self.collections['dialogues'].save(dialogue)

        self.worker.run_action("04", OptinAction())

        self.assertTrue(self.collections['participants'].find_one({'enrolled.dialogue-id':'0'}) is not None)
        self.assertEqual(1, self.collections['schedules'].count())

    def test_run_action_enroll_clear_profile_if_not_optin(self):
        self.initialize_properties()

        dialogue = self.mkobj_dialogue_question_offset_days()
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(self.mkobj_participant(
            participant_phone='06',
            last_optin_date=None,
            session_id=None,
            tags=['geeks'],
            profile=[{'label': 'name',
                      'value': 'Oliv'}],
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
        self.initialize_properties()

        dialogue = self.mkobj_dialogue_question_offset_days()
        dNow = self.worker.get_local_time()
        self.collections['participants'].save(self.mkobj_participant(
            "08", last_optin_date=time_to_vusion_format(dNow)))
        self.collections['dialogues'].save(dialogue)

        self.worker.run_action(
            "08",
            DelayedEnrollingAction(**{
                'enroll': '01',
                'offset-days': {'days': '1', 'at-time': '12:00'}}),
            Context(**{'dialogue-id': '02', 'interaction-id': '1'}))

        schedule = self.collections['schedules'].find_one({'object-type': 'action-schedule'})
        self.assertTrue(schedule is not None)
        self.assertEqual(schedule['context']['dialogue-id'], '02')
        self.assertEqual(schedule['context']['interaction-id'], '1')
        self.assertTrue(
            action_generator(**schedule['action']),
            EnrollingAction(**{'enroll': '01'}))

    def test_run_action_optin_optout(self):
        self.initialize_properties()

        regex_time = RegexMatcher(r'^(\d{4})-0?(\d+)-0?(\d+)[T ]0?(\d+):0?(\d+):0?(\d+)$')        

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
            enrolled=[{'dialogue-id': '1', 
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
                      'value': 'Oliv'}],
            enrolled=[{'dialogue-id': '1', 
                       'date-time': '2012-11-01T10:30:20'}]
        ))
        self.worker.run_action("06", OptoutAction())
        participant = self.collections['participants'].find_one({'phone': '07'})
        self.assertEqual(participant['tags'], ['geeks'])
        self.assertEqual(participant['profile'], [{'label': 'name',
                                                   'value': 'Oliv', 
                                                   'raw': None}])
        self.assertEqual(participant['enrolled'], [
            {'dialogue-id': '1', 
             'date-time': '2012-11-01T10:30:20'}])

    def test_run_action_optin_double_option_no_setting(self):
        self.initialize_properties()

        self.collections['participants'].save(self.mkobj_participant())           

        self.worker.run_action('06', OptinAction())
        
        messages = self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 0)
        self.assertEqual(self.collections['history'].count(), 0)

    @inlineCallbacks
    def test_run_action_optin_double_option_error_message_in_setting(self):
        settings = self.mk_program_settings(
            double_optin_error_feedback='You are double optin')
        self.initialize_properties(settings)

        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='06', session_id='1'))        

        yield self.worker.run_action('06', OptinAction(),  Context(**{'request-id': '22'}), '1')

        messages =  yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(self.collections['history'].count(), 1)

        history = self.collections['history'].find_one()
        self.assertEqual(history['participant-session-id'], '1')
        self.assertEqual(history['participant-phone'], '06')

    @inlineCallbacks
    def test_run_action_offset_condition(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()

        self.collections['dialogues'].save(self.mkobj_dialogue_question_offset_conditional())
        self.collections['dialogues'].save(self.mkobj_dialogue_open_question_offset_conditional())
        self.collections['participants'].save(self.mkobj_participant('06'))

        self.save_history(
            timestamp=dNow - timedelta(minutes=30),
            participant_phone='06',
            participant_session_id='1',
            metadata={'dialogue-id': '01',
                      'interaction-id': '01-01'})

        self.save_history(
            timestamp=dNow - timedelta(minutes=30),
            participant_phone='06',
            participant_session_id='1',
            metadata={'dialogue-id': '04',
                      'interaction-id': '01-01'})

        # a non matching answer do not trigger the offsetcondition
        self.save_history(
            timestamp=dNow,
            participant_phone='06',
            participant_session_id='1',
            message_direction='incoming',
            metadata={'dialogue-id': '01',
                      'interaction-id': '01-01',
                      'matching-answer': None})

        # Need to store the message into the history
        yield self.worker.run_action("06", OffsetConditionAction(**{
            'dialogue-id': '01',
            'interaction-id': '01-02'}))
        self.assertEqual(
            self.collections['schedules'].count(),
            0)

        self.save_history(
            timestamp=dNow,
            participant_phone='06',
            participant_session_id='1',
            message_direction="incoming",
            metadata={'dialogue-id': '01',
                      'interaction-id': '01-01',
                      'matching-answer': 'Fine'})

        # Need to store the message into the history
        yield self.worker.run_action("06", OffsetConditionAction(**{
            'dialogue-id': '01',
            'interaction-id':'01-02'}))
        self.assertEqual(
            self.collections['schedules'].count(),
            2)
        
        # Do not reschedule
        yield self.worker.run_action("06", OffsetConditionAction(**{
            'dialogue-id': '01',
            'interaction-id':'01-02'}))        
        self.assertEqual(
            self.collections['schedules'].count(),
            2)

        # Do send if open question
        self.save_history(
            timestamp=dNow,
            participant_phone='06',
            participant_session_id='1',
            message_direction='incoming',
            metadata={'dialogue-id': '04',
                      'interaction-id': '01-01'})

        yield self.worker.run_action("06", OffsetConditionAction(**{
            'dialogue-id': '04',
            'interaction-id': '01-01'}))
        self.assertEqual(
            self.collections['schedules'].count(),
            3)
    
    def test_run_action_offset_condition_delayed(self):
        self.initialize_properties()    
        dNow = self.worker.get_local_time()
        
        dialogue = self.mkobj_dialogue_open_question_offset_conditional()
        dialogue['interactions'][1]['offset-condition-delay'] = "10"
        self.collections['dialogues'].save(dialogue)

        self.collections['participants'].save(self.mkobj_participant('06'))
        
        #save the first question send
        self.save_history(
            timestamp=dNow - timedelta(minutes=10),
            participant_phone='06',
            participant_session_id='1',
            metadata={'dialogue-id': '04',
                      'interaction-id': '01-01'})
        #and the first question answered
        self.save_history(
            timestamp=dNow,
            participant_phone='06',
            participant_session_id='1',
            message_direction='incoming',
            metadata={'dialogue-id': '04',
                      'interaction-id': '01-01'})
        #then run the action that should schedule the next question in 5 minutes
        self.worker.run_action('06', OffsetConditionAction(**{
            'dialogue-id': '04',
            'interaction-id': '01-02'}))
        self.assertEqual(1, self.collections['schedules'].count())
        schedule = self.collections['schedules'].find_one()
        self.assertTrue(
            (dNow + timedelta(minutes=10)) - time_from_vusion_format(schedule['date-time']) 
            < timedelta(seconds=10))
        
    def test_run_action_remove_reminders(self):
        self.initialize_properties()

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

    def test_run_action_reset_no_exceptions(self):
        self.initialize_properties()

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
        
    def test_run_action_reset_keep_labels(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        
        participant = self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dNow),
            profile=[{'label': 'name',
                     'value': 'Oliv'}])
        self.collections['participants'].save(participant)

        self.worker.run_action("06", ResetAction(**{'keep-labels': 'name'}))

        reset_participant = self.collections['participants'].find_one({'phone':'06'})

        self.assertEqual(reset_participant['profile'], [{'label': 'name', 'value': 'Oliv', 'raw': ''}])
        
    def test_run_action_reset_keep_labels_only_original_labels(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        
        participant = self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dNow),
            profile=[{'label': 'name',
                     'value': 'Oliv'}, 
                     {'label': 'age',
                      'value': '33'}])
        self.collections['participants'].save(participant)

        self.worker.run_action("06", ResetAction(**{'keep-labels': 'name, gender'}))

        reset_participant = self.collections['participants'].find_one({'phone':'06'})

        self.assertEqual(reset_participant['profile'], [{'label': 'name', 'value': 'Oliv', 'raw': ''}])        
            
    def test_run_action_reset_keep_tags(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        
        participant = self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dNow),
            tags=['geek', 'meek'],
            profile=[{'label': 'name',
                     'value': 'Oliv'}])
        self.collections['participants'].save(participant)

        self.worker.run_action("06", ResetAction(**{'keep-tags': 'geek, meek'}))

        reset_participant = self.collections['participants'].find_one({'phone':'06'})

        self.assertEqual(reset_participant['tags'], ['geek', 'meek']) 
        
    def test_run_action_reset_keep_tags_only_original_tags(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        
        participant = self.mkobj_participant(
            '06',
            last_optin_date=time_to_vusion_format(dNow),
            tags=['geek', 'meek'],
            profile=[{'label': 'name',
                     'value': 'Oliv'}])
        self.collections['participants'].save(participant)

        self.worker.run_action("06", ResetAction(**{'keep-tags': 'geek, fee'}))

        reset_participant = self.collections['participants'].find_one({'phone':'06'})

        self.assertEqual(reset_participant['tags'], ['geek'])    
   

    def test_run_conditional_action(self):
        self.initialize_properties()

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
        participant = self.collections['participants'].find_one({'phone': '08'})
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
        participant = self.collections['participants'].find_one({'phone': '08'})
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

    @inlineCallbacks
    def test_run_action_proportional_tagging(self):
        self.initialize_properties()

        ## First participant
        participant_08 = self.mkobj_participant(
            '08',
            tags=['geek'])
        self.collections['participants'].save(participant_08)

        proportional_tagging = ProportionalTagging(**{
                   'proportional-tags': [{'tag': 'GroupA', 'weight': '1'},
                                         {'tag': 'GroupB', 'weight': '1'}]})
        ## Tagging
        yield self.worker.run_action("08", proportional_tagging)
        participant = self.collections['participants'].find_one()
        self.assertEqual(
            ['geek','GroupA'],
            participant['tags'])
        
        ## Second participant
        participant_09 = self.mkobj_participant(
            '09',
            tags=['father'])
        self.collections['participants'].save(participant_09)
        
        ## Tagging
        yield self.worker.run_action("09", proportional_tagging)
        participant = self.collections['participants'].find_one({'phone': '09'})
        self.assertEqual(
            ['father','GroupB'],
            participant['tags'])

    def test_run_action_proportional_tagging_already_tagged(self):
        self.initialize_properties()

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

    @inlineCallbacks
    def test_run_action_proportional_labelling(self):
        self.initialize_properties()

        participant = self.mkobj_participant(
            '01',
            profile=[{'label': 'group',
                      'value': 'A'}])
        self.collections['participants'].save(participant)

        participant_oliv = self.mkobj_participant(
            '08',
            profile=[{'label': 'name',
                      'value': 'Oliv'}])
        self.collections['participants'].save(participant_oliv)

        action = ProportionalLabelling(**{
            'label-name': 'group',
            'proportional-labels': [
                {'label-value': 'A', 'weight': '1'},
                {'label-value': 'B', 'weight': '1'}]})

        yield self.worker.run_action("08", action)
        participant_oliv = self.collections['participants'].find_one({'phone':'08'})
        self.assertEqual(
            {'label': 'group',
             'value': 'B',
             'raw': None},
            participant_oliv['profile'][1])

    @inlineCallbacks
    def test_run_action_url_forwarding(self):
        self.initialize_properties()

        self.collections['participants'].save(self.mkobj_participant(
            participant_phone='+6',
            profile=[{'label': 'name', 'value': 'olivier', 'raw': None}]))

        history_id = self.collections['history'].save(self.mkobj_history_dialogue(
            dialogue_id='1',
            interaction_id='1',
            timestamp='2012-08-04T15:15:00',
            direction='incoming'))
        participant = self.mkobj_participant(participant_phone='+6')

        message_forwarding = UrlForwarding(**{'forward-url': 'http://partner.com'})

        context = Context(**{'history_id': str(history_id)})

        yield self.worker.run_action(
            participant['phone'],
            message_forwarding,
            context,
            participant['session-id'])

        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['from_addr'], 'sphex')
        self.assertEqual(messages[0]['transport_name'], 'sphex')
        self.assertEqual(messages[0]['transport_type'], 'http_api')
        self.assertEqual(messages[0]['to_addr'], 'http://partner.com')
        self.assertEqual(
            messages[0]['transport_metadata'], 
            {'program_shortcode': '256-8181',
             'participant_phone': '+6',
             'participant_profile': [{'label': 'name', 'value': 'olivier', 'raw': None}]})
        history = DialogueHistory(**self.collections['history'].find_one())
        self.assertEqual(history['message-status'], 'forwarded')

    @inlineCallbacks
    def test_run_action_url_forwarding_not_allowed(self):
        program_settings = self.mk_program_settings('256-8181', sms_forwarding_allowed='none')
        self.initialize_properties(program_settings)

        history_id = self.collections['history'].save(self.mkobj_history_dialogue(
            dialogue_id='1',
            interaction_id='1',
            timestamp='2012-08-04T15:15:00',
            direction='incoming',
            message_status='received'))
        participant = self.mkobj_participant(participant_phone='+6')
        
        message_forwarding = UrlForwarding(**{'forward-url': 'http://partner.com'})
        
        context = Context(**{'history_id': str(history_id)})
        
        yield self.worker.run_action(
            participant['phone'],
            message_forwarding,
            context,
            participant['session-id'])
        
        messages = self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 0)
        history = DialogueHistory(**self.collections['history'].find_one())
        self.assertEqual(history['message-status'], 'received')

    @inlineCallbacks
    def test_run_action_sms_forwarding(self):
        self.initialize_properties()
        
        sender = self.mkobj_participant(
            participant_phone='+1',
            profile=[{'label': 'name',
                      'value': 'mark'},
                     {'label': 'address',
                      'value': 'kampala'}],
            tags=['my tag'])
        self.collections['participants'].save(sender)

        receiver_optin = self.mkobj_participant(
            participant_phone='+9',
            tags=['my tag'])
        self.collections['participants'].save(receiver_optin)

        receiver_optout = self.mkobj_participant(
            participant_phone='+5',
            tags=['my tag'],
            session_id=None)
        self.collections['participants'].save(receiver_optout)

        sms_forwarding = SmsForwarding(**{
            'forward-to': 'my tag',
            'forward-content': ('[participant.name]([participant.phone]) ' 
                                'living in [participant.address] sent '
                                '[context.message] at [time.H:M]'
                                )})
        context = Context(**{'message': 'Alert',
                             'request-id': '1'})
        yield self.worker.run_action(sender['phone'], sms_forwarding, context)

        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['to_addr'], receiver_optin['phone'])
        self.assertEqual(messages[0]['transport_name'], 'sphex')
        self.assertEqual(messages[0]['transport_type'], 'sms')
        self.assertRegexpMatches(messages[0]['content'], 'mark\(\+1\) living in kampala sent Alert at \d{2}:\d{2}')
        self.assertEqual(self.collections['history'].count(), 1)
        history = self.collections['history'].find_one()
        self.assertEqual(history['object-type'], 'request-history')

    @inlineCallbacks
    def test_run_action_sms_forwarding_no_participant(self):
        self.initialize_properties()

        sender = self.mkobj_participant(
            participant_phone='+1',
            profile=[{'label': 'name',
                      'value': 'mark'},
                     {'label': 'address',
                      'value': 'kampala'}],
            tags=['my tag'])
        self.collections['participants'].save(sender)

        sms_forwarding = SmsForwarding(**{
            'forward-to': 'geek',
            'forward-content': 'Hello...',
            'set-forward-message-condition': 'forward-message-condition',
            'forward-message-condition-type': 'phone-number',
            'forward-message-no-participant-feedback': 'No patient is matching the phone number.'})
        context = Context(**{'message': 'ANSWER +1234',
                             'request-id': '1'})
        yield self.worker.run_action(sender['phone'], sms_forwarding, context)

        messages = self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['to_addr'], sender['phone'])
        self.assertEqual(messages[0]['transport_type'], 'sms')
        self.assertRegexpMatches(messages[0]['content'], 'No patient is matching the phone number.')
        self.assertEqual(self.collections['history'].count(), 1)

    @inlineCallbacks
    def test_run_action_sms_forwarding_no_conditions(self):
        self.initialize_properties()

        sender = self.mkobj_participant(
            participant_phone='+1',
            profile=[{'label': 'name',
                      'value': 'mark'},
                     {'label': 'address',
                      'value': 'kampala'}],
            tags=['my tag'])
        self.collections['participants'].save(sender)

        receiver = self.mkobj_participant(
            participant_phone='+9',
            tags=['my tag'])
        self.collections['participants'].save(receiver)

        sms_forwarding = SmsForwarding(**{
            'forward-to': 'geek',
            'forward-content': 'Hello...',
            'set-forward-message-condition': 'forward-message-condition',
            'forward-message-condition-type': 'phone-number',
            'forward-message-no-participant-feedback': 'No patient is matching the phone number \'[context.message.2]\'.'})

        context = Context(**{
            'message': 'ANSWER this is a message',
            'request-id': '1'})
        yield self.worker.run_action(sender['phone'], sms_forwarding, context)

        context = Context(**{
            'message': 'ANSWER this is my message',
            'request-id': '1'})
        yield self.worker.run_action(sender['phone'], sms_forwarding, context)

        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]['to_addr'], sender['phone'])
        self.assertEqual(messages[0]['content'], 'No patient is matching the phone number \'this\'.')
        self.assertEqual(messages[1]['to_addr'], sender['phone'])
        self.assertEqual(messages[1]['content'], 'No patient is matching the phone number \'this\'.')


    @inlineCallbacks
    def test_run_action_sms_invite_new_participant(self):
        self.initialize_properties()

        sender = self.mkobj_participant(
            participant_phone='+154',
            profile=[{'label': 'name',
                      'value': 'max'},
                     {'label': 'address',
                      'value': 'kampala'}],
            tags=['my tag'])
        self.collections['participants'].save(sender)

        sms_invite = SmsInviteAction(**{
            'invite-content': '[participant.name]([participant.phone]) invites you',
            'invitee-tag': 'invited',
            'feedback-inviter': 'already in the program'})
        context = Context(**{'message': 'Join +569', 'request-id': '1'})
        yield self.worker.run_action(sender['phone'], sms_invite, context)

        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['to_addr'], '+569')
        self.assertEqual(messages[0]['content'], 'max(+154) invites you')


    @inlineCallbacks
    def test_run_action_sms_invite_optout_participant(self):
        self.initialize_properties()

        sender = self.mkobj_participant(
            participant_phone='+1545',
            profile=[{'label': 'name',
                      'value': 'max'},
                     {'label': 'address',
                      'value': 'kampala'}],
            tags=['my tag'])
        self.collections['participants'].save(sender)

        invitee_optout = self.mkobj_participant(
            participant_phone='+598',
            tags=['my tag'],
            session_id=None)
        self.collections['participants'].save(invitee_optout)

        sms_invite = SmsInviteAction(**{
            'invite-content': 'invites you',
            'invitee-tag': 'invited',
            'feedback-inviter': 'already in the program'})
        context = Context(**{'message': 'Join +598', 'request-id': '1'})
        yield self.worker.run_action(sender['phone'], sms_invite, context)

        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['to_addr'], '+598')
        self.assertEqual(messages[0]['content'], 'invites you')


    @inlineCallbacks
    def test_run_action_sms_invite_optin_participant(self):
        self.initialize_properties()

        sender = self.mkobj_participant(
            participant_phone='+1545',
            profile=[{'label': 'name',
                      'value': 'max'},
                     {'label': 'address',
                      'value': 'kampala'}],
            tags=['my tag'])
        self.collections['participants'].save(sender)

        invitee_optin = self.mkobj_participant(
            participant_phone='+5987',
            tags=['my tag'])
        self.collections['participants'].save(invitee_optin)

        sms_invite = SmsInviteAction(**{
            'invite-content': 'invites you',
            'invitee-tag': 'invited',
            'feedback-inviter': '[context.message.2] is already in the program'})
        context = Context(**{'message': 'Join +5987', 'request-id': '1'})
        yield self.worker.run_action(sender['phone'], sms_invite, context)

        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['to_addr'], '+1545')
        self.assertEqual(messages[0]['content'], '+5987 is already in the program')

    @inlineCallbacks
    def test_run_action_sms_invite_invitee_phone_empty(self):
        self.initialize_properties()

        sender = self.mkobj_participant(
            participant_phone='+1545',
            profile=[{'label': 'name',
                      'value': 'max'},
                     {'label': 'address',
                      'value': 'kampala'}],
            tags=['my tag'])
        self.collections['participants'].save(sender)

        sms_invite = SmsInviteAction(**{
            'invite-content': 'invites you',
            'invitee-tag': 'invited',
            'feedback-inviter': 'empty phone number sent'})
        context = Context(**{'message': 'Join ',
                             'request-id': '1'})
        yield self.worker.run_action(sender['phone'], sms_invite, context)

        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['to_addr'], '+1545')
        self.assertEqual(messages[0]['content'], 'empty phone number sent')

    @inlineCallbacks
    def test_run_action_save_content_variable(self):
        self.initialize_properties()

        sender = self.mkobj_participant(
            participant_phone='+1545',
            profile=[{'label': 'name',
                      'value': 'max'},
                     {'label': 'address',
                      'value': 'kampala'}],
            tags=['my tag'])
        self.collections['participants'].save(sender)

        cvt = self.mkobj_content_variable_two_key_table_wallet()
        saved_cvt = self.collections['content_variables'].save_object(cvt)

        save_content_variable = SaveContentVariableTable(**{
            'scv-attached-table': str(saved_cvt),
            'scv-row-keys': [
                {'scv-row-header': 'date',
                 'scv-row-value': '[time.Y]/[time.m]/[time.d]'},
                {'scv-row-header': 'phone',
                 'scv-row-value': '[participant.phone]'}],
            'scv-col-key-header': 'gain',
            'scv-col-extras': [
                {'scv-col-extra-header': 'name',
                 'scv-col-extra-value': '[participant.name]'}]})
        context = Context(**{'message': 'gain 100 KES',
                             'request-id': '1',
                             'matching-answer': '100 KES'})

        yield self.worker.run_action(
            sender['phone'], save_content_variable, context)

        t = self.worker.get_local_time()
        cv = self.collections['content_variables'].get_content_variable_from_match(
            {'key1': t.strftime('%Y/%m/%d'),
             'key2': '+1545',
             'key3': 'gain'})
        self.assertEqual(cv.get_value(), '100 KES')
        cv = self.collections['content_variables'].get_content_variable_from_match(
            {'key1': t.strftime('%Y/%m/%d'),
             'key2': '+1545',
             'key3': 'name'})
        self.assertEqual(cv.get_value(), 'max')
