from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

import pymongo
import json

from datetime import datetime, time, date, timedelta
import pytz

from vumi.tests.utils import get_stubbed_worker, UTCNearNow, RegexMatcher
from vumi.message import Message, TransportEvent, TransportUserMessage

from vusion import TtcGenericWorker
from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.error import MissingData
from transports import YoUgHttpTransport
from tests.utils import MessageMaker, DataLayerUtils


class TtcGenericWorkerTestCase(TestCase, MessageMaker, DataLayerUtils):

    time_format = '%Y-%m-%dT%H:%M:%S'

    simple_config = {
        'database_name': 'test',
        'dispatcher': 'dispatcher',
        'transport_name': 'app',
        }

    program_settings = [{
        'key': 'shortcode',
        'value': '8181'
        }, {
        'key': 'internationalprefix',
        'value': '256'
        }, {
        'key': 'timezone',
        'value': 'Africa/Kampala'
        }]

    shortcodes = {
        'country': 'Uganda',
        'internationalprefix': '256',
        'shortcode': '8181'
    }

    dialogue_annoucement = {
        'activated': 1,
        'auto-enrollment': 'all',
        'dialogue-id': '0',
        'interactions': [
            {'type-interaction': 'announcement',
             'interaction-id': '0',
             'content': 'Hello',
             'type-schedule': 'immediately'},
            {'type-interaction': 'announcement',
             'interaction-id': '1',
             'content': 'How are you',
             'type-schedule': 'wait',
             'minutes': '60'},
        ]
    }

    dialogue_annoucement_2 = {
            "activated": 1,
            "dialogue-id": "2",
            "interactions": [
                {"type-interaction": "announcement",
                 "interaction-id": "0",
                 "content": "Hello"
                 },
                {"type-interaction": "announcement",
                 "interaction-id": "1",
                 "content": "Today will be sunny"
                 },
                {"type-interaction": "announcement",
                 "interaction-id": "2",
                 "content": "Today is the special day"
                 }
            ]
    }

    dialogue_question = {
        'activated': 1,
        'dialogue-id': '01',
        'interactions': [
            {
                'interaction-id': '01-01',
                'type-interaction': 'question-answer',
                'content': 'How are you?',
                'keyword': 'FEEL, FEL',
                'answers': [
                    {'choice': 'Fine'},
                    {'choice': 'Ok',
                     'feedbacks':
                     [{'content': 'Thank you'}],
                     'answer-actions':
                     [{'type-answer-action': 'enrolling',
                       'enroll': '2'}],
                     },
                    ],
                'type-schedule': 'immediately'
            }
        ]
    }

    dialogue_announcement_fixedtime = {
        'activated': 1,
        'dialogue-id': '1',
        'interactions': [
            {
                'interaction-id':'1',
                'type-interaction': 'announcement',
                'content': 'Hello',
                'type-schedule': 'fixed-time',
                'date-time': '2012-03-12T12:30:00'
            }
        ]
    }

    request_join = {
        'keyword': 'www join, www',
        'responses': [
            {
                'content': 'thankyou of joining',
                },
            {
                'content': 'soon more is coming',
                }],
        'actions': [
            {
                'type-action': 'optin',
                },
            {
                'type-action': 'enrolling',
                'enroll': '01'
            }]
    }

    request_tag = {
        'keyword': 'www tagme',
        'actions': [
            {
                'type-action': 'tagging',
                'tag': 'onetag'
            }]
    }

    request_leave = {
        'keyword': 'www quit',
        'actions': [
            {
                'type-action': 'optout',
            }]
    }

    @inlineCallbacks
    def setUp(self):
        self.transport_name = 'test'
        self.control_name = 'mycontrol'
        self.database_name = 'test'
        self.config = {'transport_name': self.transport_name,
                       'database_name': self.database_name,
                       'control_name': self.control_name,
                       'dispatcher_name': 'dispatcher'}
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
        self.drop_collections()

        #Let's rock"
        self.worker.startService()
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        self.drop_collections()
        if (self.worker.sender != None):
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

    def save_status(self, message_content="hello world",
                    participant_phone="256", message_type="send",
                    message_status="delivered", timestamp=datetime.now(),
                    dialogue_id=None, interaction_id=None):
        self.collections['history'].save({
            'message-content': message_content,
            'participant-phone': participant_phone,
            'message-type': message_type,
            'message-status': message_status,
            'timestamp': timestamp,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id
        })

    #TODO: reduce the scope of the update-schedule
    @inlineCallbacks
    def test01_consume_control_update_schedule(self):
        dialogue_id = self.collections['dialogues'].save(
            self.dialogue_annoucement)
        self.collections['dialogues'].save(self.dialogue_question)
        self.collections['participants'].save({'phone': '08'})
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        event = self.mkmsg_dialogueworker_control('update-schedule',
                                                  dialogue_id.__str__())
        yield self.send(event, 'control')

        self.assertEqual(2, self.collections['schedules'].count())

    @inlineCallbacks
    def test02_consume_control_test_send_all_messages(self):
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

    def test03_multiple_dialogue_in_collection(self):
        dNow = datetime.now()
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
        participant = {'phone': '06'}
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dNow = dNow.replace(tzinfo=None)

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.init_program_db(config['database_name'])
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
            dNow + timedelta(minutes=61))
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
        participant = {'phone': '06'}
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
            'participant-phone': '09'
            })
        self.worker.load_data()

        yield self.worker.send_scheduled()

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[0]['content'], 'Today will be sunny')
        self.assertEqual(messages[1]['content'], 'Hello unattached')
        self.assertEqual(messages[2]['content'], 'Thank you')

        self.assertEquals(self.collections['schedules'].count(), 1)
        self.assertEquals(self.collections['history'].count(), 4)

    def test06_schedule_interaction_while_interaction_in_status(self):
        dialogue = self.dialogue_annoucement
        participant = {'phone': '06'}
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dPast = dNow - timedelta(minutes=30)

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        self.save_status(timestamp=dPast.strftime(self.time_format),
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
        participant = {'phone': '06'}

        dNow = datetime.now()
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
        self.save_status(timestamp=dPast.strftime(self.time_format),
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
        schedule = self.collections['schedules'].find_one()
        self.assertEqual(schedule['date-time'], dLaterFuture.strftime(self.time_format))

    def test08_schedule_interaction_that_has_expired(self):
        dialogue = self.dialogue_annoucement
        participant = {'phone': '06'}

        dNow = datetime.now()
        dPast = datetime.now() - timedelta(minutes=50)
        dLaterPast = datetime.now() - timedelta(minutes=80)

        dialogue['interactions'][1]['type-schedule'] = 'wait'

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        #Declare collection for scheduling messages
        self.collections['schedules'].save(
            {'date-time': dPast.strftime(self.time_format),
             'participant-phone': '06',
             'interaction-id': '1',
             'dialogue-id': '0'})
        
        #Declare collection for loging messages
        self.save_status(timestamp=dLaterPast.strftime(self.time_format),
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

        dNow = datetime.now()
        dFuture = datetime.now() + timedelta(days=2, minutes=30)
        dialogue['interactions'][0]['date-time'] = dFuture.strftime(
            self.time_format)

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

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

    #@inlineCallbacks
    def test12_generate_message(self):
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

        interaction_closed_question = {
            'type-interaction': 'question-answer',
            'content': 'How are you?',
            'keyword': 'FEEL',
            'answers': [
                {'choice': 'Fine'},
                {'choice': 'Ok'}],
        }

        close_question = self.worker.generate_message(interaction_closed_question)

        self.assertEqual(
            close_question,
            "How are you? 1. Fine 2. Ok To reply send: FEEL(space)(Answer Nr) to 8181")

        interaction_open_question = {
            'type-interaction': 'question-answer',
            'content': 'Which dealer did you buy the system from?',
            'keyword': 'DEALER, deal',
            'answer-label': 'Name dealer',
        }

        open_question = self.worker.generate_message(interaction_open_question)

        self.assertEqual(
            open_question,
            "Which dealer did you buy the system from? To reply send: DEALER(space)(Name dealer) to 8181")

        interaction_no_keyword = {
            'type-interaction': 'question-answer',
            'content': 'Which dealer did you buy the system from?',
            'keyword': '',
            'answer-label': 'Name dealer',
        }

        open_question = self.worker.generate_message(interaction_no_keyword)

        self.assertEqual(
            open_question,
            "Which dealer did you buy the system from? To reply send: (Name dealer) to 8181")

        interaction_no_keyword_field = {
            'type-interaction': 'question-answer',
            'content': 'Which dealer did you buy the system from?',
            'answer-label': 'Name dealer',
        }

        open_question = self.worker.generate_message(interaction_no_keyword_field)

        self.assertEqual(
            open_question,
            "Which dealer did you buy the system from? To reply send: (Name dealer) to 8181")

    @inlineCallbacks
    def test13_received_delivered(self):
        event = self.mkmsg_delivery()

        self.collections['history'].save({
            'message-id': event['user_message_id'],
            'message-type': 'sent',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('delivered', status['message-status'])

    @inlineCallbacks
    def test14_received_delivered_no_reference(self):
        event = self.mkmsg_delivery()

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertNot(status)

    @inlineCallbacks
    def test15_received_delivered_failure(self):
        event = self.mkmsg_delivery(delivery_status='failed',
                                    failure_code='404',
                                    failure_level='http',
                                    failure_reason='some reason')

        self.collections['history'].save({
            'message-id': event['user_message_id'],
            'message-type': 'sent',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('failed', status['message-status'])
        self.assertEqual('Code:404 Level:http Message:some reason', status['failure-reason'])

    @inlineCallbacks
    def test16_received_ack(self):
        event = self.mkmsg_delivery(event_type='ack')

        self.collections['history'].save({
            'message-id': event['user_message_id'],
            'message-type': 'sent',
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
        self.collections['participants'].save({'phone': '06'})
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
        history = self.collections['history'].find()
        self.assertEqual('01-01', history[0]['interaction-id'])
        self.assertEqual('01', history[0]['dialogue-id'])
        self.assertEqual('Ok', history[0]['matching-answer'])
        self.assertEqual(None, history[2]['matching-answer'])

        self.assertEqual(1, self.collections['schedules'].count())

        inbound_msg_matching_request = self.mkmsg_in(content='wWw')
        yield self.send(inbound_msg_matching_request, 'inbound')

        inbound_msg_matching_request = self.mkmsg_in(content='www join')
        yield self.send(inbound_msg_matching_request, 'inbound')

        self.assertEqual(5, self.collections['history'].count())
        self.assertEqual(3, self.collections['participants'].count())
        self.assertEqual(6, self.collections['schedules'].count())

    def test18_run_action(self):
        self.worker.init_program_db(self.database_name)

        self.worker.run_action("08", {'type-action': 'feedback',
                                      'content': 'message'})
        self.assertEqual(1, self.collections['schedules'].count())

        self.worker.run_action("08", {'type-action': 'optin'})
        self.assertEqual(1, self.collections['participants'].count())

        self.worker.run_action("08", {'type-action': 'optout'})
        self.assertEqual(1, self.collections['participants'].count())
        self.assertTrue(self.collections['participants'].find_one(
            {'phone': '08'})['optout'])

        self.worker.run_action("08", {'type-action': 'tagging',
                                      'tag': 'my tag'})
        self.worker.run_action("08", {'type-action': 'tagging',
                                      'tag': 'my second tag'})
        self.assertTrue(self.collections['participants'].find_one({'tags': 'my tag'}))

        self.collections['dialogues'].save(self.dialogue_question)
        self.worker.run_action("08", {'type-action': 'enrolling',
                                      'enroll': '01'})
        self.assertTrue(self.collections['participants'].find_one({'enrolled': '01'}))
        self.assertEqual(2, self.collections['schedules'].count())

        self.worker.run_action("08", {'type-action': 'profiling',
                                      'label': 'gender',
                                      'value': 'Female'})
        self.assertTrue(self.collections['participants'].find_one({'gender': 'Female'}))

    def test19_schedule_process_handle_crap_in_history(self):
        #config = self.simple_config
        dialogue = self.dialogue_annoucement
        participant = {'phone': '06'}

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        #self.worker.init_program_db(config['database_name'])
        self.worker.load_data()

        self.save_status(participant_phone="06",
                         interaction_id=None,
                         dialogue_id=None)

        self.worker.schedule_participant_dialogue(
            participant, dialogue)
        #assert time calculation
        schedules_count = self.collections['schedules'].count()
        self.assertEqual(schedules_count, 2)

    def test21_schedule_unattach_message(self):
        participants = [{'phone': '06'},
                        {'phone': '07'}]

        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dFuture = dNow + timedelta(minutes=30)
        dPast = dNow - timedelta(minutes=30)

        unattach_messages = [
            {
                'to': 'all participants',
                'content': 'Hello everyone',
                'schedule': time_to_vusion_format(dFuture)},
            {
                'to': 'all participants',
                'content': 'Hello again',
                'schedule': time_to_vusion_format(dPast)}]

        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        for participant in participants:
            self.collections['participants'].save(participant)

        unattach_id = self.collections['unattached_messages'].save(unattach_messages[0])
        self.collections['unattached_messages'].save(unattach_messages[1])

        self.collections['history'].save({
            'participant-phone': '06',
            'message-type': 'sent',
            'message-status': 'delivered',
            'unattach-id': unattach_id
        })

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
        
        yield self.worker.register_keywords_in_dispatcher()

        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(1, len(messages))
        self.assertEqual([['test', 'feel'],
                          ['test', 'fel'],
                          ['test', 'www']],
                         messages[0]['keyword_mappings'])

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
