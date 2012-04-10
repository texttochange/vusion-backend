from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

import pymongo
import json

from datetime import datetime, time, date, timedelta
import pytz
import iso8601

from vumi.tests.utils import get_stubbed_worker, UTCNearNow, RegexMatcher
from vumi.message import Message, TransportEvent, TransportUserMessage

from vusion import TtcGenericWorker
from transports import YoUgHttpTransport
from tests.utils import MessageMaker, DataLayerUtils


class FakeUserMessage(TransportUserMessage):

    def __init__(self, **kw):
        kw['to_addr'] = 'to'
        kw['from_addr'] = 'from'
        kw['transport_name'] = 'test'
        kw['transport_type'] = 'fake'
        kw['transport_metadata'] = {}
        super(FakeUserMessage, self).__init__(**kw)


class TtcGenericWorkerTestCase(TestCase, MessageMaker, DataLayerUtils):

    time_format = '%Y-%m-%dT%H:%M:%S'

    configControl = """
    {"program":{
            "name":"M5H",
            "database-name":"test"}
    }
    """

    simpleConfig = {
        'database_name': 'test',
        'dispatcher': 'dispatcher',
        'transport_name': 'app',
        }

    simpleScript = {
        "activated": 1,
        "script": {
            "shortcode": "8282",
            "dialogues":
            [{"dialogue-id": "0",
              "interactions": [
                  {"type-interaction": "announcement",
                   "interaction-id": "0",
                   "content": "Hello",
                   "type-schedule": "immediately"},
                  {"type-interaction": "announcement",
                   "interaction-id": "1",
                   "content": "How are you",
                   "type-schedule": "wait",
                   "minutes": "60"}]
              }]
        }
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

    twoParticipants = """{"participants":[
            {"phone":"788601462"},
            {"phone":"788601463"}
            ]}"""

    controlMessage = """
    {
        "action":"start"
    }"""

    simpleProgram_Question = {
        "activated": 1,
        "script": {
            "shortcode": "8282",
            "dialogues": [
                {
                    "interactions": [
                        {
                            "type-interaction": "question-answer",
                            "content": "How are you?",
                            "keyword": "FEEL",
                            "answers": [
                                {"choice": "Fine"},
                                {"choice": "Ok"}
                                ],
                            "type-schedule": "immediately"
                        }
                    ]
                }
            ]
        }
    }

    simpleProgram_announcement_fixedtime = """
    {"activated" : 1,
    "script": {
    "shortcode": "8282",
    "dialogues": [
    {
    "dialogue-id":"program.dialogues[0]",
    "interactions": [
    {
    "interaction-id":"0",
    "type-interaction": "announcement",
    "content": "Hello",
    "type-schedule": "fixed-time",
    "day": "2",
    "month":"3",
    "year":"2018",
    "time": "12:30"
    }
    ]
    }
    ]
    }}"""

    simpleAnnouncement = """
    {
    "type-interaction": "announcement",
    "content": "Hello",
    "type-schedule": "wait",
    "time": "02:30"
    }"""

    @inlineCallbacks
    def setUp(self):
        self.transport_name = 'test'
        self.control_name = 'mycontrol'
        self.database_name = 'test'
        self.config = {'transport_name': self.transport_name,
                       'database': self.database_name,
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
        self.db = connection[self.config['database']]
        self.collection_scripts = self.db["scripts"]
        self.collection_scripts.drop()
        self.collection_participants = self.db["participants"]
        self.collection_participants.drop()
        self.collection_status = self.db['history']
        self.collection_status.drop()
        self.collection_schedules = self.db["schedules"]
        self.collection_schedules.drop()

        self.collections = {}
        self.setup_collections(['shortcodes', 'program_settings'])

        #Let's rock"
        self.worker.startService()
        yield self.worker.startWorker()

    @inlineCallbacks
    def tearDown(self):
        self.db.programs.drop()
        if (self.worker.program_name):
            self.worker.collection_schedules.drop()
            self.worker.collection_logs.drop()
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
        self.collection_status.save({
            'message-content': message_content,
            'participant-phone': participant_phone,
            'message-type': message_type,
            'message-status': message_status,
            'timestamp': timestamp,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id
        })

    def mkmsg_ack(self, event_type='ack', user_message_id='1',
                  send_message_id='abc', transport_name=None,
                  transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            event_type=event_type,
            user_message_id=user_message_id,
            sent_message_id=send_message_id,
            transport_name=transport_name,
            transport_metadata=transport_metadata,
        )
        return TransportEvent(**params)

    def mkmsg_delivery(self, event_type='delivery_report', user_message_id='1',
                       send_message_id='abc', delivery_status='delivered',
                       failure_code=None, failure_level=None,
                       failure_reason=None, transport_name=None,
                       transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            event_type=event_type,
            user_message_id=user_message_id,
            sent_message_id=send_message_id,
            delivery_status=delivery_status,
            failure_level=failure_level,
            failure_code=failure_code,
            failure_reason=failure_reason,
            transport_name=transport_name,
            transport_metadata=transport_metadata,
        )
        return TransportEvent(**params)

    @inlineCallbacks
    def test01_consume_control_program(self):
        events = [
            ('config', Message.from_json(self.configControl))
        ]
        self.collection_scripts.save(self.simpleScript)
        self.collection_participants.save({"phone": "08"})

        for name, event in events:
            yield self.send(event, 'control')

        self.assertTrue(self.collection_schedules)
        self.assertTrue(self.collection_status)

    def test02_multiple_script_in_collection(self):
        config = self.simpleConfig
        dNow = datetime.now()
        dPast1 = datetime.now() - timedelta(minutes=30)
        dPast2 = datetime.now() - timedelta(minutes=60)

        activeScript = {"script": {"do": "something"},
                        "activated": 1,
                        "modified": dPast1}
        self.collection_scripts.save(activeScript)

        oldActiveScript = {"script": {"do": "something else"},
                           "activated": 1,
                           "modified": dPast2}
        self.collection_scripts.save(oldActiveScript)

        draftScript = {"script": {"do": "something else one more time"},
                       "activated": 0,
                       "modified": "50"}
        self.collection_scripts.save(draftScript)

        self.collection_participants.save({"phone": "06"})
        self.worker.init_program_db(config['database_name'])

        script = self.worker.get_current_script()
        self.assertEqual(script, activeScript['script'])

    def test03_schedule_participant_dialogue(self):
        config = self.simpleConfig
        script = self.simpleScript
        participant = {"phone": "06"}
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dNow = dNow.replace(tzinfo=None)
        
        self.collection_scripts.save(script)
        self.collection_participants.save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.init_program_db(config['database_name'])
        self.worker.load_data()

        self.worker.schedule_participant_dialogue(
            participant, script['script']['dialogues'][0])

        schedules_count = self.collection_schedules.count()
        self.assertEqual(schedules_count, 2)

        schedules = self.collection_schedules.find()
        #assert time calculation
        self.assertTrue(
            iso8601.parse_date(schedules[0]['datetime']).replace(tzinfo=None) <
            dNow + timedelta(minutes=1))
        self.assertTrue(
            iso8601.parse_date(schedules[1]['datetime']).replace(tzinfo=None) < 
            dNow + timedelta(minutes=61))
        self.assertTrue(
            iso8601.parse_date(schedules[1]['datetime']).replace(tzinfo=None) > 
            dNow + timedelta(minutes=59))

        #assert schedule links
        self.assertEqual(schedules[0]['participant-phone'], "06")
        self.assertEqual(schedules[0]['dialogue-id'], "0")
        self.assertEqual(schedules[0]['interaction-id'], "0")
        self.assertEqual(schedules[1]['interaction-id'], "1")

    @inlineCallbacks
    def test05_send_scheduled_messages(self):
        config = self.simpleConfig
        script = {
            "activated": 1,
            "script": {"dialogues":
                       [
                           {"dialogue-id": "0",
                            "interactions": [
                                {"type": "announcement",
                                 "interaction-id": "0",
                                 "content": "Hello"
                                 },
                                {"type": "announcement",
                                 "interaction-id": "1",
                                 "content": "Today will be sunny"
                                 },
                                {"type": "announcement",
                                 "interaction-id": "2",
                                 "content": "Today is the special day"
                                 }
                             ]
                            }
                       ]
                       }
        }
        participant = {"phone": "06"}
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dNow = dNow - timedelta(minutes=1)
        dPast = dNow - timedelta(minutes=30)
        dFuture = dNow + timedelta(minutes=30)

        self.collection_scripts.save(script)
        self.collection_participants.save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.collection_schedules.save({"datetime": dPast.strftime(self.time_format),
                                        "dialogue-id": "0",
                                        "interaction-id": "0",
                                        "participant-phone": "09"})
        self.collection_schedules.save({"datetime": dNow.strftime(self.time_format),
                                        "dialogue-id": "0",
                                        "interaction-id": "1",
                                        "participant-phone": "09"})
        self.collection_schedules.save({"datetime": dFuture.strftime(self.time_format),
                                        "dialogue-id": "0",
                                        "interaction-id": "2",
                                        "participant-phone": "09"})
        self.worker.init_program_db(config['database_name'])
        self.worker.load_data()

        yield self.worker.send_scheduled()

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(messages[0]['content'], "Hello")
        self.assertEqual(messages[1]['content'], "Today will be sunny")

        self.assertEquals(self.collection_schedules.count(), 1)
        self.assertEquals(self.collection_status.count(), 2)

    def getCollection(self, db, collection_name):
        if (collection_name in self.db.collection_names()):
            return db[collection_name]
        else:
            return db.create_collection(collection_name)

    def test06_schedule_interaction_while_interaction_in_status(self):
        config = self.simpleConfig
        script = self.simpleScript
        participant = {"phone": "06"}
        mytimezone = self.program_settings[2]['value']
        dNow = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(mytimezone))
        dPast = dNow - timedelta(minutes=30)

        self.collection_scripts.save(script)
        self.collection_participants.save(participant)
        self.save_status(timestamp=dPast.strftime(self.time_format),
                         participant_phone="06",
                         interaction_id="0",
                         dialogue_id="0")
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.init_program_db(config['database_name'])
        self.worker.load_data()

        #Starting the test
        schedules = self.worker.schedule_participant_dialogue(
            participant, script['script']['dialogues'][0])

        self.assertEqual(self.collection_status.count(), 1)
        self.assertEqual(self.collection_schedules.count(), 1)

    def test07_schedule_interaction_while_interaction_in_schedule(self):
        config = self.simpleConfig
        script = self.simpleScript
        participant = {"phone": "06"}

        dNow = datetime.now()
        dPast = dNow - timedelta(minutes=30)
        dFuture = dNow + timedelta(minutes=30)
        dLaterFuture = dNow + timedelta(minutes=60)

        script['script']['dialogues'][0]['interactions'][1]['type-schedule'] = 'fixed-time'
        script['script']['dialogues'][0]['interactions'][1]['date-time'] = dLaterFuture.strftime(
            self.time_format)

        #program = json.loads(self.simpleProgram)['program']
        self.collection_scripts.save(script)
        self.collection_participants.save(participant)
        #Declare collection for scheduling messages
        self.collection_schedules.save({"datetime": dFuture.strftime(self.time_format),
                                        "participant-phone": "06",
                                        "interaction-id": "1",
                                        "dialogue-id": "0"})
        #Declare collection for loging messages
        self.save_status(timestamp=dPast.strftime(self.time_format),
                         participant_phone="06",
                         interaction_id="0",
                         dialogue_id="0")
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.init_program_db(config['database_name'])
        self.worker.load_data()

        #Starting the test
        schedules = self.worker.schedule_participant_dialogue(
            participant, script['script']['dialogues'][0])

        self.assertEqual(self.collection_status.count(), 1)
        self.assertEqual(self.collection_schedules.count(), 1)
        schedule = self.collection_schedules.find_one()
        self.assertEqual(schedule['datetime'], dLaterFuture.strftime(self.time_format))

    def test08_schedule_interaction_that_has_expired(self):
        config = self.simpleConfig
        script = self.simpleScript
        participant = {"phone": "06"}

        dNow = datetime.now()
        dPast = datetime.now() - timedelta(minutes=50)
        dLaterPast = datetime.now() - timedelta(minutes=80)

        script['script']['dialogues'][0]['interactions'][1]['type-schedule'] = 'wait'

        self.collection_scripts.save(script)
        self.collection_participants.save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.init_program_db(config['database_name'])
        self.worker.load_data()

        #Declare collection for scheduling messages
        self.collection_schedules.save({"datetime": dPast.strftime(self.time_format),
                                        "participant-phone": "06",
                                        "interaction-id": "1",
                                        "dialogue-id": "0"})

        #Declare collection for loging messages
        self.save_status(timestamp=dLaterPast.strftime(self.time_format),
                         participant_phone="06",
                         interaction_id="0",
                         dialogue_id="0")

        #Starting the test
        schedules = self.worker.schedule_participant_dialogue(
            participant, script['script']['dialogues'][0])

        self.assertEqual(self.collection_status.count(), 2)
        self.assertEqual(self.collection_schedules.count(), 0)

    def test09_schedule_at_fixed_time(self):
        config = self.simpleConfig
        script = json.loads(self.simpleProgram_announcement_fixedtime)
        participant = {"phone": "08"}

        dNow = datetime.now()
        dFuture = datetime.now() + timedelta(days=2, minutes=30)
        script['script']['dialogues'][0]['interactions'][0]['date-time'] = dFuture.strftime(
            self.time_format)

        self.collection_scripts.save(script)
        self.collection_participants.save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.init_program_db(config['database_name'])
        self.worker.load_data()

        #action
        self.worker.schedule_participant_dialogue(
            participant, script['script']['dialogues'][0])

        #asserting
        self.assertEqual(self.collection_schedules.count(), 1)
        schedule = self.collection_schedules.find_one()
        schedule_datetime = datetime.strptime(schedule['datetime'],
                                              "%Y-%m-%dT%H:%M:%S")
        self.assertEquals(schedule_datetime.year, dFuture.year)
        self.assertEquals(schedule_datetime.hour, dFuture.hour)
        self.assertEquals(schedule_datetime.minute, dFuture.minute)

    @inlineCallbacks
    def test10_receive_message(self):
        event = FakeUserMessage(content='Hello World')

        connection = pymongo.Connection('localhost', 27017)
        self.db = connection[self.config['database']]
        self.collection_scripts = self.db.create_collection("scripts")
        self.collection_participants = self.db.create_collection(
            "participants")

        self.worker.init_program_db(self.database_name)

        #action
        yield self.send(event, 'inbound')

        #asserting
        self.assertEqual(self.collection_status.count(), 1)
        status = self.collection_status.find_one()
        self.assertEqual(status['message-content'], 'Hello World')
        self.assertEqual(status['message-type'], 'received')

    def test12_generate_question(self):
        self.assertTrue(False)

    @inlineCallbacks
    def test13_received_delivered(self):
        event = self.mkmsg_delivery()

        connection = pymongo.Connection("localhost", 27017)
        self.db = connection[self.config['database']]
        self.collection_scripts = self.db.create_collection("scripts")
        self.collection_participants = self.db.create_collection(
            "participants")
        self.collection_status = self.db.create_collection('history')
        self.worker.init_program_db(self.database_name)

        self.collection_status.save({
            'message-id': event['user_message_id'],
            'message-type': 'send',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collection_status.find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('delivered', status['message-status'])

    @inlineCallbacks
    def test14_received_delivered_no_reference(self):
        event = self.mkmsg_delivery()

        connection = pymongo.Connection("localhost", 27017)
        self.db = connection[self.config['database']]
        self.collection_scripts = self.db.create_collection("scripts")
        self.collection_participants = self.db.create_collection(
            "participants")
        self.worker.init_program_db(self.database_name)

        yield self.send(event, 'event')

        status = self.collection_status.find_one({
            'message-id': event['user_message_id']})

        self.assertNot(status)

    @inlineCallbacks
    def test15_received_delivered_failure(self):
        connection = pymongo.Connection("localhost", 27017)
        self.db = connection[self.config['database']]
        self.collection_scripts = self.db.create_collection("scripts")
        self.collection_participants = self.db.create_collection(
            "participants")
        self.collection_status = self.db.create_collection('history')
        self.worker.init_program_db(self.database_name)

        event = self.mkmsg_delivery(delivery_status='failed',
                                    failure_code='404',
                                    failure_level='http',
                                    failure_reason='some reason')

        self.collection_status.save({
            'message-id': event['user_message_id'],
            'message-type': 'send',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collection_status.find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('failed', status['message-status'])
        self.assertEqual('404', status['failure-code'])
        self.assertEqual('http', status['failure-level'])
        self.assertEqual('some reason', status['failure-reason'])

    @inlineCallbacks
    def test16_received_ack(self):
        event = self.mkmsg_delivery(event_type='ack')

        connection = pymongo.Connection("localhost", 27017)
        self.db = connection[self.config['database']]
        self.collection_scripts = self.db.create_collection("scripts")
        self.collection_participants = self.db.create_collection(
            "participants")
        self.collection_status = self.db.create_collection('history')
        self.worker.init_program_db(self.database_name)

        self.collection_status.save({
            'message-id': event['user_message_id'],
            'message-type': 'send',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collection_status.find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('ack', status['message-status'])

    def test17_bound_incoming_message_with_script(self):
        self.assertTrue(False)

    def test18_schedule_process_handle_crap_in_history(self):
        config = self.simpleConfig
        script = self.simpleScript
        participant = {"phone": "06"}

        self.collection_scripts.save(script)
        self.collection_participants.save(participant)
        for program_setting in self.program_settings:
            self.collections['program_settings'].save(program_setting)
        self.worker.init_program_db(config['database_name'])
        self.worker.load_data()

        self.save_status(participant_phone="06",
                         interaction_id=None,
                         dialogue_id=None)

        self.worker.schedule_participant_dialogue(
            participant, script['script']['dialogues'][0])
        #assert time calculation
        schedules_count = self.collection_schedules.count()
        self.assertEqual(schedules_count, 2)

    @inlineCallbacks
    def test19_control_dispatcher_keyword_routing(self):
        #config = self.worker.config
        script = self.simpleProgram_Question
        participant = {"phone": "06"}

        self.collection_scripts.save(script)
        self.collection_participants.save(participant)
        self.worker.init_program_db(self.config['database'])

        yield self.worker.register_keywords_in_dispatcher(
            ['keyword1', 'keyword2'])

        msg = self.broker.get_messages('vumi', 'dispatcher.control')
        expected_msg = self.mkmsg_dispatcher_control(
            exposed_name=self.transport_name,
            keyword_mappings=[['test', 'keyword1'],
                              ['test', 'keyword2']])
        self.assertEqual(msg, [expected_msg])

    #@inlineCallbacks
    #def test12_2dialogues_updated_2message_scheduled(self):
        #self.assertTrue(False)

    #@inlineCallbacks
    #def test13_resend_failed_message(self):
        ##control from the user
        #self.assertTrue(False)

    #@inlineCallbacks
    #def test14_add_participant_is_scheduling_dialogues(self):
        #self.assertTrue(False)

    #@inlineCallbacks
    #def test15_after_reply_Goto_Dialogue(self):
        #self.assertTrue(False)

    #@inlineCallbacks
    #def test16_after_reply_send_feedback(self):
        #self.assertTrue(False)

    #@inlineCallbacks
    #def test17_restarting_do_not_schedule_or_send_message(self):
        #self.assertTrue(False)
