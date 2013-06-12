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

from vusion.dialogue_worker import DialogueWorker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.error import MissingData, MissingTemplate
from vusion.persist.action import (UnMatchingAnswerAction, EnrollingAction,
                                   FeedbackAction, OptinAction, OptoutAction,
                                   TaggingAction, ProfilingAction,
                                   OffsetConditionAction, RemoveRemindersAction,
                                   ResetAction, RemoveDeadlineAction,
                                   DelayedEnrollingAction, action_generator, Actions)
from vusion.persist import Dialogue, schedule_generator, Participant

#from transports import YoUgHttpTransport

from tests.utils import MessageMaker, DataLayerUtils, ObjectMaker


class DialogueWorkerTestCase(TestCase, MessageMaker,
                             DataLayerUtils, ObjectMaker):
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
        self.worker = get_stubbed_worker(DialogueWorker,
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
        yield self.worker.stopService()

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


class DialogueWorkerTestCase_main(DialogueWorkerTestCase):

    def test01_has_already_been_answered(self):
        dNow = datetime.now()

        participant = self.mkobj_participant()

        self.assertFalse(self.worker.has_already_valid_answer(
            participant, '1', '1'))

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
            participant, '1', '1'))
        
        self.assertFalse(self.worker.has_already_valid_answer(
            participant, '1', '1'))

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
            participant, '1', '1'))

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
            participant, '1', '1'))

    def test02_is_enrolled(self):
        participant = self.mkobj_participant(enrolled=[{
            'dialogue-id': '01',
            'date-time': 'someting'},
            {'dialogue-id': '3',
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
        dPast3 = datetime.now() - timedelta(minutes=70)

        dialogue = self.mkobj_dialogue_question_answer()

        dialogue['dialogue-id'] = '1'
        dialogue['activated'] = 1
        dialogue['modified'] = dPast1
        id_active_dialogue_one = self.collections['dialogues'].save(
            dialogue)

        dialogue.pop('_id')
        dialogue['dialogue-id'] = '1'
        dialogue['activated'] = 1
        dialogue['modified'] = dPast2
        self.collections['dialogues'].save(dialogue)

        dialogue.pop('_id')
        dialogue['dialogue-id'] = '1'
        dialogue['activated'] = 0
        dialogue['modified'] = dPast3
        self.collections['dialogues'].save(dialogue)

        dialogue.pop('_id')
        dialogue['dialogue-id'] = '2'
        dialogue['activated'] = 1
        dialogue['modified'] = dPast1
        id_active_dialogue_two = self.collections['dialogues'].save(dialogue)

        dialogue.pop('_id')
        dialogue['dialogue-id'] = '2'
        dialogue['activated'] = 1
        dialogue['modified'] = dPast2
        self.collections['dialogues'].save(dialogue)

        dialogue.pop('_id')
        dialogue['dialogue-id'] = '2'
        dialogue['activated'] = 0
        dialogue['modified'] = dPast2
        self.collections['dialogues'].save(dialogue)

        self.collections['participants'].save({'phone': '06'})

        dialogues = self.worker.get_active_dialogues()
        self.assertEqual(len(dialogues), 2)
        self.assertEqual(dialogues[0]['_id'],
                         id_active_dialogue_one)
        self.assertEqual(dialogues[1]['_id'],
                         id_active_dialogue_two)

    def test04_create_participant(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        participant = self.worker.create_participant('06')
        
        self.assertEqual(participant['model-version'], Participant.MODEL_VERSION)

    def test03_get_current_dialogue(self):
        dialogue = self.mkobj_dialogue_annoucement()
        dialogue['modified'] = Timestamp(datetime.now() - timedelta(minutes=1),
                                         0)
        self.collections['dialogues'].save(dialogue)
        other_dialogue = self.mkobj_dialogue_annoucement()
        other_dialogue['interactions'] = []
        self.collections['dialogues'].save(other_dialogue)
        active_dialogue = self.worker.get_current_dialogue("0")
        self.assertTrue(active_dialogue)
        self.assertEqual([], active_dialogue['interactions'])
    
    def test03_get_matching_request(self):
        request_1 = self.mkobj_request_response('www info')
        request_2 = self.mkobj_request_reponse_lazy_matching('www')
        request_1_id = self.collections['requests'].save(request_1)
        request_2_id = self.collections['requests'].save(request_2)

        context = {}
        self.worker.get_matching_request_actions('www info', Actions(), context)
        self.assertEqual(context['request-id'], request_1_id)
       
        context = {}
        self.worker.get_matching_request_actions('www', Actions(), context)
        self.assertEqual(context['request-id'], request_2_id)
        
        context = {}
        actions = Actions()
        self.worker.get_matching_request_actions('www tata', actions, context)
        self.assertTrue(context != {})
        self.assertEqual(context['request-id'], request_2_id)
        self.assertTrue(actions.contains('feedback'))
        
        context = {}
        self.worker.get_matching_request_actions('ww tata', Actions(), context)
        self.assertTrue(context == {})
        
        context = {}
        self.worker.get_matching_request_actions('ww', Actions(), context)
        self.assertTrue(context == {})

    def test05_send_scheduled_messages(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()        
        #mytimezone = self.program_settings[2]['value']
        dNow = self.worker.get_local_time()
        dNow = dNow - timedelta(minutes=2)
        dPast = dNow - timedelta(minutes=61)
        dFuture = dNow + timedelta(minutes=30)

        dialogue = self.mkobj_dialogue_announcement_2()
        participant_transport_metadata = {'some_key': 'some_value'}
        participant = self.mkobj_participant('09', transport_metadata=participant_transport_metadata)

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
       
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
                participant_phone='09',
                context={'dialogue-id': '2', 'interaction-id': '1'}))

        self.worker.send_scheduled()

        participant_transport_metadata.update({'customized_id': 'myid'})
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[0]['content'], 'Today will be sunny')
        self.assertEqual(messages[0]['transport_metadata'], participant_transport_metadata)
        self.assertEqual(messages[1]['content'], 'Hello unattached')
        self.assertEqual(messages[1]['transport_metadata'], participant_transport_metadata)        
        self.assertEqual(messages[2]['content'], 'Thank you')
        participant_transport_metadata.update({'priority': 'prioritized'})        
        self.assertEqual(messages[2]['transport_metadata'], participant_transport_metadata) 
        for message in messages:
            self.assertTrue('customized_id' in message['transport_metadata'])

        self.assertEquals(self.collections['schedules'].count(), 1)
        self.assertEquals(self.collections['history'].count(), 4)
        histories = self.collections['history'].find()
        for history in histories:
            self.assertTrue(history['participant-session-id'] is not None)
            
    def test05_send_scheduled_messages_with_priority(self):
        dialogue = self.mkobj_dialogue_announcement_prioritized()
        participant = self.mkobj_participant('10')
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dNow = dNow - timedelta(minutes=2)
        #dPast = dNow - timedelta(minutes=61)
        #dFuture = dNow + timedelta(minutes=30)
        
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dNow.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='0',
                participant_phone='10'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dNow.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='1',
                participant_phone='10'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dNow.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='2',
                participant_phone='10'))
        self.collections['schedules'].save(
            self.mkobj_schedule_feedback(
                date_time=time_to_vusion_format(dNow),
                content='Thank you',
                participant_phone='10',
                context={'dialogue-id': '2', 'interaction-id': '1'}))
        
        self.worker.load_data()

        self.worker.send_scheduled()

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 4)
        self.assertTrue('priority' in messages[0]['transport_metadata'])
        self.assertTrue('priority' in messages[1]['transport_metadata'])
        self.assertTrue('priority' in messages[3]['transport_metadata'])
        
        self.assertFalse('priority' in messages[2]['transport_metadata'])
        
        self.assertEqual(messages[0]['transport_metadata']['priority'],
            dialogue['interactions'][0]['prioritized'])
        self.assertEqual(messages[1]['transport_metadata']['priority'],
            dialogue['interactions'][1]['prioritized'])
        self.assertEqual(messages[3]['transport_metadata']['priority'],
            self.program_settings[4]['value'])

    @inlineCallbacks
    def test05_send_scheduled_deadline(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=2)

        dialogue = self.mkobj_dialogue_open_question_reminder_offset_time()
        dialogue['interactions'][0]['reminder-actions'].append(
            {'type-action': 'feedback', 'content': 'Bye'})
        dialogue = Dialogue(**dialogue)
        participant = self.mkobj_participant('06')
        self.collections['dialogues'].save(dialogue.get_as_dict())
        self.collections['participants'].save(participant)
        schedule = schedule_generator(**{
            'object-type': 'deadline-schedule',
            'model-version': '2',
            'date-time': dPast.strftime(self.time_format),
            'dialogue-id': '04',
            'interaction-id': '01-01',
            'participant-phone': '06',
            'participant-session-id': '1'})
        self.collections['schedules'].save(schedule.get_as_dict())

        yield self.worker.send_scheduled()

        saved_participant = self.collections['participants'].find_one()
        self.assertEqual(saved_participant['session-id'], None)
        history = self.collections['history'].find_one({'object-type': 'oneway-marker-history'})
        self.assertTrue(history is not None)
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
        history = self.collections['history'].find_one({'object-type': 'dialogue-history'})
        self.assertEqual(history['participant-session-id'], '1')
        self.assertEqual(history['message-content'], 'Bye')


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
        schedule = schedule_generator(**{
            'participant-phone': '06',
            'date-time': dPast.strftime(self.time_format),
            'object-type': 'action-schedule',
            'action': {'type-action': 'enrolling',
                       'enroll': '04'},
            'context': {'request-id': '1'}})
        self.collections['schedules'].save(schedule.get_as_dict())
        yield self.worker.send_scheduled()

        saved_participant = self.collections['participants'].find_one({
            'enrolled.dialogue-id': '04'})
        self.assertTrue(saved_participant)

    @inlineCallbacks
    def test05_send_scheduled_run_action_expired(self):
          for program_setting in self.mkobj_program_settings():
              self.collections['program_settings'].save(program_setting)
          self.worker.load_data()
  
          dNow = self.worker.get_local_time()
          dPast = dNow - timedelta(minutes=61)
  
          dialogue = self.mkobj_dialogue_open_question()
          participant = self.mkobj_participant('06')
          self.collections['dialogues'].save(dialogue)
          self.collections['participants'].save(participant)
          schedule = schedule_generator(**{
              'participant-phone': '06',
              'date-time': dPast.strftime(self.time_format),
              'object-type': 'action-schedule',
              'action': {'type-action': 'enrolling',
                         'enroll': '04'},
              'context': {'request-id': '1'}})
          self.collections['schedules'].save(schedule.get_as_dict())
          yield self.worker.send_scheduled()
  
          saved_participant = self.collections['participants'].find_one({
              'enrolled.dialogue-id': '04'})
          self.assertTrue(saved_participant is None)
          self.assertEqual(1, self.collections['history'].count())

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
        self.assertEqual(messages[0]['content'],
                         'What is your gender?\n male or female')

    def test06_get_program_actions(self):
        self.collections['program_settings'].save({
            'key': 'unmatching-answer-remove-reminder',
            'value': 1})        
        self.worker.load_data()

        actions = Actions()
        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder_offset_time())
        participant = self.mkobj_participant()
        context = {
            'dialogue-id': '04',
            'interaction-id': '01-01',
            'interaction': dialogue.get_interaction('01-01'),
            'matching-answer': None}
        
        self.worker.get_program_dialogue_actions(participant, context, actions)
        
        self.assertEqual(1, len(actions))

    def test11_customize_message(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        participant1 = self.mkobj_participant(
            '06',
            profile=[{'label': 'name',
                      'value': 'oliv'}])
        participant2 = self.mkobj_participant(
            '07',
            profile=[{'label': 'gender',
                      'value': 'Female'}])

        participant3 = self.mkobj_participant_v2(
            '08',
            profile=[{'label': 'gender',
                      'value': 'Female',
                      'raw': 'gender 2 and proud'},
                     {'label': 'Month of Pregnancy',
                      'value': '2',
                      'raw': 'month2 and proud'},
                     {'label': '5 to 6',
                      'value': '2',
                      'raw': 'usingnumber 2'},])        

        self.collections['participants'].save(participant1)
        self.collections['participants'].save(participant2)
        self.collections['participants'].save(participant3)

        message_one = self.worker.customize_message('06', 'Hello [participant.name]')
        self.assertEqual(message_one, 'Hello oliv')

        self.assertRaises(MissingData,
                          self.worker.customize_message, '07', 'Hello [participant.name]')
        
        message_three = self.worker.customize_message('08', 'u have send: [participant.gender_raw]')        
        self.assertEqual(message_three, 'u have send: gender 2 and proud')
        
        message_4 = self.worker.customize_message('08', 'u have send: [participant.Month of Pregnancy_raw]')        
        self.assertEqual(message_4, 'u have send: month2 and proud')
        
        message_5 = self.worker.customize_message('08', 'u have send: [participant.5 to 6_raw]')        
        self.assertEqual(message_5, 'u have send: usingnumber 2')        
        

    def test12_generate_message_use_template(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dialogue = self.mkobj_dialogue_question_offset_days()

        self.assertRaises(MissingTemplate,
                          self.worker.generate_message,
                          dialogue['interactions'][0])

        saved_template_id = self.collections['templates'].save(
            self.template_closed_question)
        self.collections['program_settings'].save(
            {'key': 'default-template-closed-question',
             'value': saved_template_id}
        )
        self.worker.load_data()

        close_question = self.worker.generate_message(
            dialogue['interactions'][0])

        self.assertEqual(
            close_question,
            "How are you?\n1. Fine\n2. Ok\n To reply send: FEEL<space><AnswerNb> to 8181")

        saved_template_id = self.collections['templates'].save(
            self.template_open_question)
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
            "What is your name?\n To reply send: NAME<space><name> to +3123456"
        )

        self.collections['program_settings'].drop()
        self.collections['program_settings'].save(
            {'key': 'default-template-open-question',
             'value': ObjectId("4fc343509fa4da5e11000000")}
        )
        self.worker.load_data()

        self.assertRaises(MissingTemplate,
                          self.worker.generate_message, interaction)

        self.collections['program_settings'].drop()
        self.collections['program_settings'].save(
            {'key': 'default-template-open-question',
             'value': None}
        )
        self.worker.load_data()
        self.assertRaises(MissingTemplate,
                          self.worker.generate_message, interaction)

    def test12_generate_message_question_multi_keyword_uses_no_template(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        interaction_question_multi_keyword = self.mkobj_dialogue_question_multi_keyword()['interactions'][0]

        question_multi_keyword = self.worker.generate_message(
            interaction_question_multi_keyword)

        self.assertEqual(question_multi_keyword,
                         "What is your gender?\n male or female")

    def test12_generate_message_no_template(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        interaction = self.mkobj_dialogue_question_offset_days()['interactions'][0]
        interaction['set-use-template'] = None

        close_question = self.worker.generate_message(interaction)
        self.assertEqual(
            close_question,
            "How are you?")


    def test13_get_time_next_daemon_iteration(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        self.assertEqual(
            60,
            self.worker.get_time_next_daemon_iteration())
        
        dNow = self.worker.get_local_time()
        dFuture = dNow + timedelta(seconds=70)        
        schedule = self.mkobj_schedule(date_time=time_to_vusion_format(dFuture))
        self.collections['schedules'].save(schedule)
        self.assertEqual(
            60,
            self.worker.get_time_next_daemon_iteration())

        dFuture = dNow + timedelta(seconds=30)
        schedule = self.mkobj_schedule(date_time=time_to_vusion_format(dFuture))
        self.collections['schedules'].save(schedule)
        self.assertTrue(
            30 - self.worker.get_time_next_daemon_iteration() < 1)

        dPast = dNow - timedelta(seconds=30)
        schedule = self.mkobj_schedule(date_time=time_to_vusion_format(dPast))
        self.collections['schedules'].save(schedule)
        self.assertEqual(
            1,
            self.worker.get_time_next_daemon_iteration())

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
            {'app': 'test', 'keyword': 'www', 'to_addr': '8181', 'prefix': '+256'},
            {'app': 'test', 'keyword': 'quit', 'to_addr': '8181', 'prefix': '+256'},
            {'app': 'test', 'keyword': 'quitnow', 'to_addr': '8181', 'prefix': '+256'}],
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

    def test22_daemon_shortcode_updated(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        ## load a first time the properties
        self.worker.load_data()
        for program_setting in self.mkobj_program_settings_international_shortcode():
            self.collections['program_settings'].save(program_setting)
        
        self.worker.sender.cancel()
        self.worker.daemon_process()

        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(1, len(messages))

    def test23_test_send_all_messages(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        self.worker.send_all_messages(self.dialogue_announcement, '06')

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]['content'], "Hello")
        self.assertEqual(messages[0]['to_addr'], "06")
        self.assertEqual(messages[1]['content'], "How are you")
        self.assertEqual(messages[1]['to_addr'], "06")
