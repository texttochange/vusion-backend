from datetime import datetime, time, date, timedelta

import json
from pymongo import MongoClient
from redis import StrictRedis
from bson.objectid import ObjectId
from bson.timestamp import Timestamp

from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from vumi.message import Message, TransportEvent, TransportUserMessage
from vumi.tests.utils import UTCNearNow, RegexMatcher, VumiTestCase
from vumi.application.tests.helpers import ApplicationHelper

from vusion.dialogue_worker import DialogueWorker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.error import MissingData, MissingTemplate
from vusion.persist.action import Actions
from vusion.persist import Dialogue, Participant
from vusion.context import Context
from vusion.message import DispatcherControl

from tests.utils import DataLayerUtils, ObjectMaker, MessageMaker


class DialogueWorkerTestCase(VumiTestCase, DataLayerUtils, ObjectMaker, MessageMaker):
    
    @inlineCallbacks
    def setUp(self):
        self.control_name = 'mycontrol'
        self.database_name = 'test_program_db'
        self.vusion_database_name = 'test_vusion_db'
        self.dispatcher_name = 'dispatcher'
        self.config = {'database_name': self.database_name,
                       'vusion_database_name': self.vusion_database_name,
                       'control_name': self.control_name,
                       'dispatcher_name': self.dispatcher_name,
                       'mongodb_host': 'localhost',
                       'mongodb_port': 27017,
                       'mongodb_safe': True}
        self.app_helper = self.add_helper(ApplicationHelper(DialogueWorker))
        self.worker = yield self.app_helper.get_application(self.config)
        self.transport_name = self.worker.transport_name
        
        #retrive all collections from worker
        self.collections = self.worker.collections
        self.drop_collections()
        
        self.redis = StrictRedis()

    @inlineCallbacks
    def tearDown(self):
        self.drop_collections()
        yield super(DialogueWorkerTestCase, self).tearDown()

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
            'timestamp': time_to_vusion_format(timestamp),
            'message-id': '1'}
        for key in metadata:
            history[key] = metadata[key]
        self.collections['history'].save_history(**history)

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
        keys = self.redis.keys('vusion:programs:%s:*' % self.database_name)
        for key in keys:
            self.redis.delete(key)

    def dispatch_control(self, control):
        return self.app_helper.dispatch_raw('.'.join([self.transport_name, 'control']), control)

    def get_dispatched_dispatcher_control(self):
        return self.app_helper.get_dispatched(self.dispatcher_name, 'control', DispatcherControl)

    def wait_for_dispatched_dispatcher_control(self, amount):
        d = self.app_helper.worker_helper.broker.wait_messages('vumi', '.'.join([self.dispatcher_name, 'control']), amount)
        d.addCallback(lambda msgs: [
            DispatcherControl(**msg.payload) for msg in msgs])
        return d


class DialogueWorkerTestCase_main(DialogueWorkerTestCase):

    

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

    def test11_customize_message_participant(self):
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

        message_one = self.worker.customize_message( 'Hello [participant.name] your phone is [participant.phone]', '06')
        self.assertEqual(message_one, 'Hello oliv your phone is 06')

        self.assertRaises(MissingData,
                          self.worker.customize_message, 'Hello [participant.name]', '07')
        
        message_three = self.worker.customize_message('u have send: [participant.gender_raw]', '08')        
        self.assertEqual(message_three, 'u have send: gender 2 and proud')
        
        message_4 = self.worker.customize_message('u have send: [participant.Month of Pregnancy_raw]', '08')        
        self.assertEqual(message_4, 'u have send: month2 and proud')
        
        message_5 = self.worker.customize_message('u have send: [participant.5 to 6_raw]', '08')        
        self.assertEqual(message_5, 'u have send: usingnumber 2')
        
        # Test that using 'participants' works
        message_6 = self.worker.customize_message( 'Hello [participants.name]', '06')
        self.assertEqual(message_6, 'Hello oliv')
        
        # Test that participant is not case sensitive
        message_7 = self.worker.customize_message( 'Hello [PARTICIPANT.name]', '06')
        self.assertEqual(message_7, 'Hello oliv')        
       
    def test11_customize_message_content_variable(self):
        self.initialize_properties()

        content_one_key = self.mkobj_content_variables_one_key(
                    key1='temperature', value='100 C')
        self.collections['content_variables'].save_object(content_one_key)

        content_two_keys = self.mkobj_content_variables_two_keys(
            key1='program', key2='weather', value='30 C')
        self.collections['content_variables'].save_object(content_two_keys)

        content_three_key = self.mkobj_content_variables_three_keys(
            key1='mombasa', key2='chicken', key3='price', value='600')
        self.collections['content_variables'].save_object(content_three_key)
        
        message_two_keys = self.worker.customize_message('Today the temperature is [contentVariable.program.weather]')
        self.assertEqual(message_two_keys, 'Today the temperature is 30 C')
        
        self.assertRaises(
            MissingData,
            self.worker.customize_message, 
            'Today the temperature is [contentVariable.today.weather]')
        
        message_one_key = self.worker.customize_message('Today the temperature is [contentVariable.temperature]')
        self.assertEqual(message_one_key, 'Today the temperature is 100 C')
        
        message_three_keys = self.worker.customize_message('Today the chicken cost [contentVariable.mombasa.chicken.price]')
        self.assertEqual(message_three_keys, 'Today the chicken cost 600')
    
    def test11_customize_message_context_fail(self):
        self.initialize_properties()
        
        self.assertRaises(
            MissingData,
            self.worker.customize_message, 
            'Today "[context.message]" was received')
        
        self.assertRaises(
            MissingData,
            self.worker.customize_message,
            'Today  at "[context.time]" we finish',
            context=Context())
        
        context = Context(**{
            'message': 'hello how are you',
            'time': '09:00'})
        self.assertRaises(
            MissingData,
            self.worker.customize_message,
            'Today  at "[context.message.7]" we finish',
            context=Context())

    def test11_customize_message_context_ok(self):
        context = Context(**{
            'message': 'hello how are you',
            'time': '09:00'})

        message = self.worker.customize_message(
            'Today "[context.message]" was received at [context.time]',
            context=context)
        self.assertEqual(message, 'Today "hello how are you" was received at 09:00')

    
    def test11_customize_message_time(self):
        self.initialize_properties()
        
        message = self.worker.customize_message(
           'Now it is [time.H:M]')
        self.assertRegexpMatches(message,'Now it is ([0-1][0-9]|2[0-3]):[0-5][0-9]')
        
        message = self.worker.customize_message(
            'Now it is [time.Ip]')
        self.assertRegexpMatches(message, 'Now it is (0[1-9]|1[0-2])(AM|PM)')
    
    def test11_customize_message_do_not_fail(self):
        self.initialize_properties()
        
        message = self.worker.customize_message('Today "[context.message]" was received', fail=False)
        self.assertEqual(message, 'Today "[context.message]" was received')

        ## A failed customized content should not stop the loop on matched
        message = self.worker.customize_message('Today "[context.message]" was received at [time.H]', fail=False)
        self.assertRegexpMatches(message, 'Today "\[context.message\]" was received at \d\d')        
    
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
        self.collections['requests'].save(self.mkobj_request_leave())

        yield self.worker.register_keywords_in_dispatcher()

        messages = self.app_helper.get_dispatched('dispatcher', 'control', DispatcherControl)        
        self.assertEqual(1, len(messages))
        self.assertEqual([
            {'app': 'sphex', 'keyword': 'feel', 'to_addr': '8181', 'prefix': '+256'},
            {'app': 'sphex', 'keyword': 'fel', 'to_addr': '8181', 'prefix': '+256'},
            {'app': 'sphex', 'keyword': 'quit', 'to_addr': '8181', 'prefix': '+256'},
            {'app': 'sphex', 'keyword': 'quitnow', 'to_addr': '8181', 'prefix': '+256'},
            {'app': 'sphex', 'keyword': 'www', 'to_addr': '8181', 'prefix': '+256'}],
            messages[0]['rules'])

    @inlineCallbacks
    def test22_register_keywords_in_dispatcher_international(self):
        self.initialize_properties(
            program_settings=self.mk_program_settings_international_shortcode(),
            shortcode=self.mkobj_shortcode_international())       
        
        self.collections['requests'].save(self.mkobj_request_join())

        yield self.worker.register_keywords_in_dispatcher()

        messages = self.app_helper.get_dispatched('dispatcher', 'control', DispatcherControl)
        self.assertEqual(1, len(messages))
        self.assertEqual([
            {'app': 'sphex', 'keyword': 'www', 'to_addr': '+318181'}],
            messages[0]['rules'])

    @inlineCallbacks
    def test22_daemon_shortcode_updated(self):
        ## load a first time the properties        
        self.initialize_properties()
        self.assertTrue(self.worker.is_ready())
        ## reload with a shortcode that doesn't exist  
        self.delete_properties()
        self.initialize_properties(
            self.mk_program_settings_international_shortcode(),
            register_keyword=True)
        
        messages = yield self.app_helper.get_dispatched('dispatcher', 'control', DispatcherControl)
        self.assertEqual(1, len(messages))
        self.assertEqual('remove_exposed', messages[0]['action'])
        self.assertFalse(self.worker.is_ready())

        self.delete_properties()
        self.initialize_properties(
            self.mk_program_settings_international_shortcode(),
            self.mkobj_shortcode_international(),
            register_keyword=True)

        messages = self.app_helper.get_dispatched('dispatcher', 'control', DispatcherControl)        
        self.assertEqual(2, len(messages))
        self.assertEqual('add_exposed', messages[1]['action'])
        self.assertTrue(self.worker.is_ready())

    def test23_test_send_all_messages(self):
        self.initialize_properties()

        self.worker.send_all_messages(self.dialogue_announcement, '06')

        messages = self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]['content'], "Hello")
        self.assertEqual(messages[0]['to_addr'], "06")
        self.assertEqual(messages[1]['content'], "How are you")
        self.assertEqual(messages[1]['to_addr'], "06")
