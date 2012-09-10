from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

import pymongo
import json

from bson.objectid import ObjectId

from datetime import datetime, time, date, timedelta
import pytz

from vumi.tests.utils import get_stubbed_worker, UTCNearNow, RegexMatcher
from vumi.message import Message, TransportEvent, TransportUserMessage

from vusion.dialogue_worker import TtcGenericWorker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.error import MissingData, MissingTemplate
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

        #Let's rock"
        self.worker.startService()
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
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
                     participant_phone="256", message_direction="send",
                     message_status="delivered", timestamp=datetime.now(),
                     dialogue_id=None, interaction_id=None):
        self.collections['history'].save({
            'message-content': message_content,
            'participant-phone': participant_phone,
            'message-direction': message_direction,
            'message-status': message_status,
            'timestamp': timestamp,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id
        })


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

    def test04_schedule_participant_dialogue(self):
        config = self.simple_config
        dialogue = self.dialogue_annoucement
        participant = self.mkobj_participant()
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dNow = dNow.replace(tzinfo=None)

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        self.worker.schedule_participant_dialogue(
            participant, dialogue)

        schedules_count = self.collections['schedules'].count()
        self.assertEqual(schedules_count, 2)

        schedules = self.collections['schedules'].find()
        #assert time calculation
        self.assertTrue(
            time_from_vusion_format(schedules[0]['date-time']) <
            dNow + timedelta(minutes=1))
        self.assertTrue(
            time_from_vusion_format(schedules[1]['date-time']) <
            dNow + timedelta(days=3))
        self.assertTrue(
            time_from_vusion_format(schedules[1]['date-time']) >
            dNow + timedelta(minutes=59))

        #assert schedule links
        self.assertEqual(schedules[0]['participant-phone'], '06')
        self.assertEqual(schedules[0]['dialogue-id'], '0')
        self.assertEqual(schedules[0]['interaction-id'], '0')
        self.assertEqual(schedules[1]['interaction-id'], '1')

    @inlineCallbacks
    def test05_send_scheduled_messages(self):
        dialogue = self.dialogue_annoucement_2
        participant = self.mkobj_participant('09')
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dNow = dNow - timedelta(minutes=1)
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
        self.collections['schedules'].save({
            'date-time': dPast.strftime(self.time_format),
            'dialogue-id': '2',
            'interaction-id': '0',
            'participant-phone': '09'})
        self.collections['schedules'].save({
            'date-time': dNow.strftime(self.time_format),
            'dialogue-id': '2',
            'interaction-id': '1',
            'participant-phone': '09'})
        self.collections['schedules'].save({
            'date-time': dFuture.strftime(self.time_format),
            'dialogue-id': '2',
            'interaction-id': '2',
            'participant-phone': '09'})
        self.collections['schedules'].save({
            'date-time': time_to_vusion_format(dNow),
            'unattach-id': unattached_message,
            'participant-phone': '09'
        })
        self.collections['schedules'].save({
            'date-time': time_to_vusion_format(dNow),
            'type-content': 'feedback',
            'content': 'Thank you',
            'participant-phone': '09'})
        self.worker.load_data()

        yield self.worker.send_scheduled()

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

    def test06_schedule_interaction_while_interaction_in_status(self):
        dialogue = self.dialogue_annoucement
        participant = {'phone': '06'}
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dPast = dNow - timedelta(minutes=30)

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        self.save_history(
            timestamp=dPast.strftime(self.time_format),
            participant_phone='06',
            interaction_id='0',
            dialogue_id='0')
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        #Starting the test
        schedules = self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['history'].count(), 1)
        self.assertEqual(self.collections['schedules'].count(), 1)

    def test07_schedule_interaction_while_interaction_in_schedule(self):
        dialogue = self.dialogue_annoucement
        participant = self.mkobj_participant()
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
      
        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=30)
        dFuture = dNow + timedelta(minutes=30)
        dLaterFuture = dNow + timedelta(minutes=60)

        dialogue['interactions'][1]['type-schedule'] = 'fixed-time'
        dialogue['interactions'][1]['date-time'] = dLaterFuture.strftime(
            self.time_format)

        #program = json.loads(self.simpleProgram)['program']
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        #Declare collection for scheduling messages
        self.collections['schedules'].save({'date-time': dFuture.strftime(self.time_format),
                                        'participant-phone': '06',
                                        'interaction-id': '1',
                                        'dialogue-id': '0'})
        #Declare collection for loging messages
        self.save_history(timestamp=dPast.strftime(self.time_format),
                         participant_phone='06',
                         interaction_id='0',
                         dialogue_id='0')

        #Starting the test
        schedules = self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['history'].count(), 1)
        self.assertEqual(self.collections['schedules'].count(), 1)
        schedule = self.collections['schedules'].find_one()
        self.assertEqual(schedule['date-time'], dLaterFuture.strftime(self.time_format))

    def test08_schedule_interaction_that_has_expired(self):
        dialogue = self.dialogue_annoucement
        participant = self.mkobj_participant()
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
      
        dNow = self.worker.get_local_time()
        dPast = datetime.now() - timedelta(days=3)
        dLaterPast = datetime.now() - timedelta(days=5)

        dialogue['interactions'][1]['type-schedule'] = 'offset-days'

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)

        #Declare collection for scheduling messages
        self.collections['schedules'].save(
            {'date-time': dPast.strftime(self.time_format),
             'participant-phone': '06',
             'interaction-id': '1',
             'dialogue-id': '0'})

        #Declare collection for logging messages
        self.save_history(timestamp=dLaterPast.strftime(self.time_format),
                         participant_phone='06',
                         interaction_id='0',
                         dialogue_id='0')

        #Starting the test
        self.worker.schedule_participant_dialogue(
            participant, dialogue)

        self.assertEqual(self.collections['history'].count(), 2)
        self.assertEqual(self.collections['schedules'].count(), 0)

    def test09_schedule_at_fixed_time(self):
        dialogue = self.dialogue_announcement_fixedtime
        participant = {'phone': '08'}
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

        participants = [
            {'phone': '06',
             'name': 'oliv'},
            {'phone': '07',
             'gender': 'Female'}
        ]

        self.collections['participants'].save(participants[0])
        self.collections['participants'].save(participants[1])

        message_one = self.worker.generate_message(interaction_using_tag)
        message_one = self.worker.customize_message('06', message_one)
        self.assertEqual(message_one, 'Hello oliv')

        message_two = self.worker.generate_message(interaction_using_tag)
        self.assertRaises(MissingData, self.worker.customize_message, '07', message_two)

    #@inlineCallbacks
    def test12_generate_message(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        interaction_closed_question = {
            'type-interaction': 'question-answer',
            'type-question': 'closed-question',
            'content': 'How are you?',
            'keyword': 'FEEL',
            'answers': [
                {'choice': 'Fine'},
                {'choice': 'Ok'}],
        }

        self.assertRaises(MissingTemplate, self.worker.generate_message, interaction_closed_question)

        saved_template_id = self.collections['templates'].save(self.template_closed_question)
        self.collections['program_settings'].save(
            {'key': 'default-template-closed-question',
             'value': saved_template_id}
        )

        close_question = self.worker.generate_message(interaction_closed_question)

        self.assertEqual(
            close_question,
            "How are you?\n1. Fine\n2. Ok\n To reply send: FEEL<space><AnswerNb> to 8181")

        saved_template_id = self.collections['templates'].save(self.template_open_question)
        self.collections['program_settings'].save(
            {'key': 'default-template-open-question',
             'value': saved_template_id}
        )

        interaction_open_question = {
            'type-interaction': 'question-answer',
            'type-question': 'open-question',
            'content': 'Which dealer did you buy the system from?',
            'keyword': 'DEALER, deal',
            'answer-label': 'Name dealer',
        }

        open_question = self.worker.generate_message(interaction_open_question)

        self.assertEqual(
            open_question,
            "Which dealer did you buy the system from?\n To reply send: DEALER<space><Name dealer> to 8181")

        self.collections['program_settings'].drop()
        self.collections['program_settings'].save(
            {'key': 'default-template-open-question',
             'value': ObjectId("4fc343509fa4da5e11000000")}
        )

        self.assertRaises(MissingTemplate, self.worker.generate_message, interaction_open_question)

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
        event = self.mkmsg_delivery_for_send(event_type='ack')

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
    def test17_receive_inbound_message(self):
        self.collections['dialogues'].save(self.dialogue_question)
        self.collections['dialogues'].save(self.dialogue_annoucement_2)
        self.collections['participants'].save(self.mkobj_participant())
        self.collections['requests'].save(self.request_join)

        inbound_msg_matching = self.mkmsg_in(from_addr='06',
                                             content='Feel ok')
        yield self.send(inbound_msg_matching, 'inbound')

        #Only message matching keyword should be forwarded to the worker
        inbound_msg_non_matching_keyword = self.mkmsg_in(from_addr='06',
                                                         content='ok')
        yield self.send(inbound_msg_non_matching_keyword, 'inbound')

        inbound_msg_non_matching_answer = self.mkmsg_in(from_addr='06',
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

        inbound_msg_matching_request = self.mkmsg_in(content='wWw')
        yield self.send(inbound_msg_matching_request, 'inbound')

        inbound_msg_matching_request = self.mkmsg_in(content='www join')
        yield self.send(inbound_msg_matching_request, 'inbound')

        self.assertEqual(5, self.collections['history'].count())
        self.assertEqual(3, self.collections['participants'].count())
        self.assertEqual(6, self.collections['schedules'].count())

        self.collections['dialogues'].save(self.dialogue_open_question)

        inbound_msg_matching_request = self.mkmsg_in(
            from_addr='06',
            content='name john doe')
        yield self.send(inbound_msg_matching_request, 'inbound')

        participant = self.collections['participants'].find_one({'phone': '06'})
        self.assertTrue('name' in participant)
        self.assertEqual('john doe', participant['name'])

    def test18_run_action(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
        ## Error message
        saved_template_id = self.collections['templates'].save(
            self.template_unmatching_answer)
        self.collections['program_settings'].save(
            {'key': 'default-template-unmatching-answer',
             'value': saved_template_id})

        self.worker.run_action("08",
                               {'type-action': 'feedback',
                                'content': 'message'})
        self.assertEqual(1, self.collections['schedules'].count())

        self.worker.run_action("08",
                               {'type-action': 'unmatching-answer',
                                'answer': 'best'})
        unmatching_template = self.collections['program_settings'].find_one({
            'key': 'default-template-unmatching-answer'})
        self.assertEqual(saved_template_id, unmatching_template['value'])
        self.assertEqual(2, self.collections['schedules'].count())
        schedules = self.collections['schedules'].find()
        self.assertEqual(schedules[1]['content'],
                         "best does not match any answer")

        ## Tagging
        self.collections['participants'].save(self.mkobj_participant('08'))
        self.worker.run_action("08", {'type-action': 'tagging',
                                      'tag': 'my tag'})
        self.worker.run_action("08", {'type-action': 'tagging',
                                      'tag': 'my second tag'})
        self.assertTrue(self.collections['participants'].find_one({'tags': 'my tag'}))
        self.worker.run_action("08", {'type-action': 'tagging',
                                      'tag': 'my tag'})
        self.assertEqual(
            ['my tag', 'my second tag'],
            self.collections['participants'].find_one({'tags': 'my tag'})['tags'])

        ## Enrolling
        self.collections['dialogues'].save(self.dialogue_question)
        self.worker.run_action("08", {'type-action': 'enrolling',
                                      'enroll': '01'})
        self.assertTrue(self.collections['participants'].find_one({'enrolled': '01'}))
        self.assertEqual(3, self.collections['schedules'].count())

        self.worker.run_action("08", {'type-action': 'enrolling',
                                      'enroll': '01'})
        self.assertEqual(
            1,
            len(self.collections['participants'].find_one({'phone': '08'})['enrolled']))

        #Enrolling a new number will opt it in
        self.worker.run_action("09", {'type-action': 'enrolling',
                                      'enroll': '01'})
        self.assertTrue(
            self.collections['participants'].find_one({'phone': '09',
                                                       'enrolled': '01'}))

        self.worker.run_action("08", {'type-action': 'profiling',
                                      'label': 'gender',
                                      'value': 'Female'})
        self.assertTrue(self.collections['participants'].find_one({'gender': 'Female'}))

    def test18_run_action_optin_optout(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        
         ## Participant optin
        self.worker.run_action("08", {'type-action': 'optin'})
        self.assertEqual(1, self.collections['participants'].count())
        participant = self.collections['participants'].find_one()
        self.assertTrue('session-id' in participant)
        self.assertEqual(participant['session-id'], RegexMatcher(r'^[0-9a-fA-F]{32}$'))
        self.assertTrue('last-optin-date' in participant)
        self.assertEqual(participant['last-optin-date'], RegexMatcher(r'^(\d{4})-0?(\d+)-0?(\d+)[T ]0?(\d+):0?(\d+):0?(\d+)$'))

        ## Participant optout (All schedule messages should be removed)
        self.collections['schedules'].save(self.mkobj_schedule("08"))
        self.collections['schedules'].save(self.mkobj_schedule("06"))        
        self.worker.run_action("08", {'type-action': 'optout'})
        self.assertEqual(1, self.collections['participants'].count())
        participant_optout = self.collections['participants'].find_one()
        self.assertTrue(participant_optout['session-id'] is None)
        self.assertTrue(participant_optout['last-optin-date'] is None)
        self.assertEqual(1, self.collections['schedules'].count())
        
        ## Participant can optin again
        self.worker.run_action("08", {'type-action': 'optin'})
        self.assertEqual(1, self.collections['participants'].count())
        participant = self.collections['participants'].find_one()
        self.assertEqual(participant['session-id'], RegexMatcher(r'^[0-9a-fA-F]{32}$'))
        self.assertEqual(participant['last-optin-date'], RegexMatcher(r'^(\d{4})-0?(\d+)-0?(\d+)[T ]0?(\d+):0?(\d+):0?(\d+)$'))

        ## Participant cannot optin while they are aleardy optin
        self.worker.run_action("08", {'type-action': 'optin'})
        self.assertEqual(1, self.collections['participants'].count())
        participant_reoptin = self.collections['participants'].find_one()
        self.assertEqual(participant['session-id'], participant_reoptin['session-id'])
        self.assertEqual(participant['last-optin-date'], participant_reoptin['last-optin-date'])


    def test19_schedule_process_handle_crap_in_history(self):
        dialogue = self.dialogue_annoucement
        participant = {'phone': '06'}

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        self.save_history(
            participant_phone="06",
            interaction_id=None,
            dialogue_id=None)

        self.worker.schedule_participant_dialogue(
            participant, dialogue)
        #assert time calculation
        schedules_count = self.collections['schedules'].count()
        self.assertEqual(schedules_count, 2)

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

        self.collections['history'].save(self.mkobj_history_unattach(unattach_id))

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
        self.collections['requests'].save(self.request_join)
        self.collections['requests'].save(self.request_leave)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        yield self.worker.register_keywords_in_dispatcher()

        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(1, len(messages))
        self.assertEqual([
            {'app': 'test', 'keyword': 'feel', 'to_addr': '8181'},
            {'app': 'test', 'keyword': 'fel', 'to_addr': '8181'},
            {'app': 'test', 'keyword': 'www', 'to_addr': '8181'}],
            messages[0]['rules'])

    @inlineCallbacks
    def test23_test_send_all_messages(self):
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        yield self.worker.send_all_messages(self.dialogue_annoucement, '06')

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

        self.collections['dialogues'].save(self.dialogue_annoucement)
        self.collections['dialogues'].save(self.dialogue_question)
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='08'))
        ##optout
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='09', session_id=None))
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='10',
                                   enrolled=[self.dialogue_question['dialogue-id']]))
        ##optout
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='11',
                                   enrolled=[self.dialogue_question['dialogue-id']],
                                   session_id=None))

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
            self.dialogue_annoucement)
        self.collections['participants'].save({'phone': '08'})
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        event = self.mkmsg_dialogueworker_control('test-send-all-messages',
                                                  dialogue_id.__str__(),
                                                  phone_number='08')
        yield self.send(event, 'control')

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 2)
