from datetime import datetime, time, date, timedelta

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
from vusion.persist.action import Actions
from vusion.persist import Dialogue, Participant

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
        self.setup_collections(['shortcodes'])

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

    def initialize_properties(self, program_settings=None, shortcode=None, register_keyword=False):
        if program_settings is None:
            program_settings = self.mk_program_settings('256-8181')
        if shortcode is None:
            shortcode = self.mkobj_shortcode('8181', '256')
        for program_setting in program_settings:
            self.collections['program_settings'].save(program_setting)
        self.collections['shortcodes'].save(shortcode)
        self.worker.load_properties(register_keyword)

    def delete_properties(self):
        self.collections['shortcodes'].remove()
        self.collections['program_settings'].remove()


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
        #for program_setting in self.program_settings:
            #self.collections['program_settings'].save(program_setting)
        #self.worker.load_properties()
        self.initialize_properties()

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
        #for program_setting in self.program_settings:
            #self.collections['program_settings'].save(program_setting)
        #self.worker.load_properties()
        self.initialize_properties()
        
        participant = self.worker.create_participant('06')
        
        self.assertEqual(participant['model-version'], Participant.MODEL_VERSION)

    def test03_get_current_dialogue(self):
        dialogue = self.mkobj_dialogue_annoucement()
        dialogue['modified'] = Timestamp(datetime.now() - timedelta(minutes=1), 0)
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

    def test06_get_program_actions(self):
        self.initialize_properties()
        self.collections['program_settings'].save({
            'key': 'unmatching-answer-remove-reminder',
            'value': 1})        
        self.worker.load_properties()

        actions = Actions()
        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder_offset_time())
        participant = self.mkobj_participant()
        context = {
            'dialogue-id': '04',
            'interaction-id': '01-01',
            'interaction': dialogue.get_interaction('01-01'),
            'matching-answer': None}
        
        self.worker.get_program_actions(participant, context, actions)
        
        self.assertEqual(1, len(actions))

    def test11_customize_message(self):
        self.initialize_properties()

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
        

    def test12_generate_message_use_template_fail(self):
        self.initialize_properties()

        dialogue = self.mkobj_dialogue_question_offset_days()

        self.assertRaises(MissingTemplate,
                          self.worker.generate_message,
                          dialogue['interactions'][0])

    def test12_generate_message_use_template_open_question(self):
        saved_template_id = self.collections['templates'].save(
            self.template_closed_question)

        settings = self.mk_program_settings(
            default_template_closed_question=saved_template_id)
        self.initialize_properties(settings)

        dialogue = self.mkobj_dialogue_question_offset_days()

        close_question = self.worker.generate_message(
            dialogue['interactions'][0])

        self.assertEqual(
            close_question,
            "How are you?\n1. Fine\n2. Ok\n To reply send: FEEL<space><AnswerNb> to 8181")

    def test12_generate_message_use_template_closed_question(self):
        saved_template_id = self.collections['templates'].save(
            self.template_open_question)
        
        settings = self.mk_program_settings(
            shortcode='+3123456',
            default_template_open_question=saved_template_id)
        shortcode = self.mkobj_shortcode_international('+3123456')
        self.initialize_properties(settings, shortcode)
        
        interaction = self.mkobj_dialogue_open_question()['interactions'][0]
        interaction['keyword'] = "name, nam"

        open_question = self.worker.generate_message(interaction)

        self.assertEqual(
            open_question,
            "What is your name?\n To reply send: NAME<space><name> to +3123456")
        
    def test12_generate_message_use_template_open_question_fail_bad_ref(self):        
        settings = self.mk_program_settings(
            default_template_open_question=ObjectId("4fc343509fa4da5e11000000"))
        self.initialize_properties(settings)
        
        interaction = self.mkobj_dialogue_open_question()['interactions'][0]        
        
        self.assertRaises(MissingTemplate,
                          self.worker.generate_message,
                          interaction)

    def test12_generate_message_use_template_open_question_no_ref(self):
        settings = self.mk_program_settings(
            default_template_open_question=None)
        self.initialize_properties(settings)

        interaction = self.mkobj_dialogue_open_question()['interactions'][0]
                
        self.assertRaises(MissingTemplate,
                          self.worker.generate_message,
                          interaction)

    def test12_generate_message_question_multi_keyword_uses_no_template(self):
        self.initialize_properties()

        interaction_question_multi_keyword = self.mkobj_dialogue_question_multi_keyword()['interactions'][0]

        question_multi_keyword = self.worker.generate_message(
            interaction_question_multi_keyword)

        self.assertEqual(question_multi_keyword,
                         "What is your gender?\n male or female")

    def test12_generate_message_no_template(self):
        self.initialize_properties

        interaction = self.mkobj_dialogue_question_offset_days()['interactions'][0]
        interaction['set-use-template'] = None

        close_question = self.worker.generate_message(interaction)
        self.assertEqual(
            close_question,
            "How are you?")


    def test13_get_time_next_daemon_iteration(self):
        self.initialize_properties()
        
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
        self.initialize_properties()
        self.collections['dialogues'].save(self.dialogue_question)
        self.collections['requests'].save(self.mkobj_request_join())
        self.collections['requests'].save(self.request_leave)

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
        self.initialize_properties(
            program_settings=self.mk_program_settings_international_shortcode(),
            shortcode=self.mkobj_shortcode_international())       
        
        self.collections['requests'].save(self.mkobj_request_join())

        yield self.worker.register_keywords_in_dispatcher()

        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(1, len(messages))
        self.assertEqual([
            {'app': 'test', 'keyword': 'www', 'to_addr': '+318181'}],
            messages[0]['rules'])

    def test22_daemon_shortcode_updated(self):
        ## load a first time the properties        
        self.initialize_properties()
        self.assertTrue(self.worker.is_ready())
        ## reload with a shortcode that doesn't exist  
        self.delete_properties()
        self.initialize_properties(
            self.mk_program_settings_international_shortcode(),
            register_keyword=True)
        
        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(1, len(messages))
        self.assertEqual('remove_exposed', messages[0]['action'])
        self.assertFalse(self.worker.is_ready())

        self.delete_properties()
        self.initialize_properties(
            self.mk_program_settings_international_shortcode(),
            self.mkobj_shortcode_international(),
            register_keyword=True)
        
        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(2, len(messages))
        self.assertEqual('add_exposed', messages[1]['action'])
        self.assertTrue(self.worker.is_ready())
        

    def test23_test_send_all_messages(self):
        self.initialize_properties()

        self.worker.send_all_messages(self.dialogue_announcement, '06')

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]['content'], "Hello")
        self.assertEqual(messages[0]['to_addr'], "06")
        self.assertEqual(messages[1]['content'], "How are you")
        self.assertEqual(messages[1]['to_addr'], "06")


    def test24_is_tagged(self):
        self.initialize_properties()
            
        participant = self.mkobj_participant(
            '06',
            tags=['geek', 'male'])
        
        self.collections['participants'].save(participant)
        
        self.assertTrue(self.worker.is_tagged('06', ['geek']))
        self.assertTrue(self.worker.is_tagged('06', ['geek', 'sometag']))        
        self.assertFalse(self.worker.is_tagged('06', ['sometag']))
        