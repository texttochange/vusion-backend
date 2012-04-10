# -*- test-case-name: tests.test_ttc -*-

import sys

from twisted.internet.defer import (inlineCallbacks, Deferred)
from twisted.enterprise import adbapi
from twisted.internet import task

import pymongo

from datetime import datetime, time, date, timedelta
import pytz
import iso8601

from vumi.application import ApplicationWorker
from vumi.message import Message, TransportUserMessage, TransportEvent
from vumi.application import SessionManager
from vumi import log


class TtcGenericWorker(ApplicationWorker):

    def __init__(self, *args, **kwargs):
        super(TtcGenericWorker, self).__init__(*args, **kwargs)

    def startService(self):
        self._d = Deferred()
        super(TtcGenericWorker, self).startService()

    @inlineCallbacks
    def startWorker(self):
        log.msg("One Generic Worker is starting")
        super(TtcGenericWorker, self).startWorker()
        self.control_consumer = yield self.consume(
            '%(control_name)s.control' % self.config,
            self.consume_control,
            message_class=Message)
        self.dispatcher_publisher = yield self.publish_to(
            '%(dispatcher_name)s.control' % self.config)

        #config
        self.transport_name = self.config['transport_name']
        self.control_name = self.config['control_name']
        self.transport_type = 'sms'

        self.sender = None
        self.program_name = None
        self.last_script_used = None
        self.collections = {}
        self.properties = {}

        self._d.callback(None)

        if  (('database_name' in self.config)
             and self.config['database_name']):
            self.init_program_db(self.config['database_name'])
            #start looping process of the scheduler
            if (self.sender == None):
                self.sender = task.LoopingCall(self.daemon_process)
                self.sender.start(60.0)

        if ('dispatcher_name' in self.config):
            yield self._setup_dispatcher_publisher()

    #TODO from the keyword link to the corresponding dialogue/interaction
    def consume_user_message(self, message):
        self.log("User message: %s" % message['content'])
        self.save_status(message_content=message['content'],
                         participant_phone=message['from_addr'],
                         message_type='received')

    def save_status(self, message_content, participant_phone, message_type,
                    message_status=None, message_id=None,
                    timestamp=None, dialogue_id=None,
                    interaction_id=None):
        if timestamp:
            timestamp = self.to_vusion_format(timestamp)
        else:
            timestamp = self.to_vusion_format(self.get_local_time())
        self.collection_status.save({
            'message-id': message_id,
            'message-content': message_content,
            'participant-phone': participant_phone,
            'message-type': message_type,
            'message-status': message_status,
            'timestamp': timestamp,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id
            })
        self.log("History saved")

    def get_current_script_id(self):
        for script in self.collection_scripts.find({'activated': 1}).sort('modified', pymongo.DESCENDING).limit(1):
            return script['_id']
        self.log("Fatal Error: no active script found in the database")
        return None

    def get_current_script(self):
        for script in self.collection_scripts.find({"activated": 1}).sort("modified", pymongo.DESCENDING).limit(1):
            return script['script']
        self.log("Fatal Error: no active script found in the database")
        return None

    def init_program_db(self, database_name):
        self.log("Initialization of the program")
        self.database_name = database_name
        self.log("Connecting to database: %s" % self.database_name)

        #Initilization of the database
        connection = pymongo.Connection("localhost", 27017)
        self.db = connection[self.database_name]

        #Declare collection for retriving script
        collection_scripts_name = "scripts"
        if not(collection_scripts_name in self.db.collection_names()):
            self.collection_scripts = self.db.create_collection(
                collection_scripts_name)
        else:
            self.collection_scripts = self.db[collection_scripts_name]

        #Declare collection for retriving participants
        collection_participants_name = "participants"
        if not(collection_participants_name in self.db.collection_names()):
            self.collection_participants = self.db.create_collection(
                collection_participants_name)
        else:
            self.collection_participants = self.db[
                collection_participants_name]

        #Declare collection for scheduling messages
        collection_schedules_name = "schedules"
        if (collection_schedules_name in self.db.collection_names()):
            self.collection_schedules = self.db[collection_schedules_name]
        else:
            self.collection_schedules = self.db.create_collection(
                collection_schedules_name)

        #Declare collection for loging messages
        collection_status_name = "history"
        if (collection_status_name in self.db.collection_names()):
            self.collection_status = self.db[collection_status_name]
        else:
            self.collection_status = self.db.create_collection(
                collection_status_name)

        self.setup_collections(['program_settings', 'unattached_messages'])

    def setup_collections(self, names):
        for name in names:
            self.setup_collection(name)

    def setup_collection(self, name):
        if name in self.db.collection_names():
            self.collections[name] = self.db[name]
        else:
            self.collections[name] = self.db.create_collection(name)

    #@inlineCallbacks
    def consume_control(self, message):
        self.log("Control message!")
        #data = message.payload['data']
        #self.record.append(('config',message))
        if (message.get('program')):
            program = message['program']
            self.log("Receive a config message: %s" % program['name'])
            #MongoDB#
            self.init_program_db(program['database-name'])
            #self.db.programs.save(program)

        elif (message.get('action') == 'resume'
              or message.get('action') == 'start'):
            self.log("Getting an action: %s" % message['action'])
            #self.init_program_db(message.get('content'))
            #reconstruct the scheduling by replaying all
            #the program for each participant
            self.collection_schedules.remove()

        #start looping process of the scheduler
        if (self.sender == None):
            self.sender = task.LoopingCall(self.daemon_process)
            self.sender.start(30.0)

    def dispatch_event(self, message):
        self.log("Event message!")
        status = self.collection_status.find_one({
            'message-id': message['user_message_id']
        })
        if (not status):
            self.log('Error no reference for this event %s' % message)
            return
        if (message['event_type'] == 'ack'):
            status['message-status'] = 'ack'
        if (message['event_type'] == 'delivery_report'):
            status['message-status'] = message['delivery_status']
            if (message['delivery_status'] == 'failed'):
                status['failure-code'] = message['failure_code']
                status['failure-level'] = message['failure_level']
                status['failure-reason'] = message['failure_reason']
        self.collection_status.save(status)

    @inlineCallbacks
    def daemon_process(self):
        self.log('Starting daemon_process()')
        self.load_data()
        if not self.is_ready():
            return
        self.schedule()
        yield self.send_scheduled()
        if self.has_active_script_changed():
            self.log('Synchronizing with dispatcher')
            keywords = self.get_keywords()
            yield self.register_keywords_in_dispatcher(keywords)

    def load_data(self):
        program_settings = self.collections['program_settings'].find()
        for program_setting in program_settings:
            self.properties[program_setting['key']] = program_setting['value']

    def is_ready(self):
        if 'shortcode' not in self.properties:
            self.log('Shortcode not defined')
            return False
        if not self.get_current_script():
            self.log('No active script defined')
            return False
        return True

    def has_active_script_changed(self):
        script_id = self.get_current_script_id()
        if script_id == None:
            return False
        if self.last_script_used == None:
            self.last_script_used = script_id
            return True
        if self.last_script_used == script_id:
            return False
        self.last_script_used = script_id
        return True

    def get_keywords(self):
        keywords = []
        script = self.get_current_script()
        for dialogue in script['dialogues']:
            for interaction in dialogue['interactions']:
                if 'keyword' in interaction:
                    keywords.append(interaction['keyword'])
        return keywords

    def schedule(self):
        self.log('Starting schedule()')
        script = self.get_current_script()
        if (script and ('dialogues' in script)):
            self.schedule_participants_dialogue(
                self.collection_participants.find(),
                script['dialogues'][0])

    def get_local_time(self):
        if 'timezone' not in self.properties:
            self.log('Timezone property not defined, use UTC')
            return datetime.utcnow()
        return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(self.properties['timezone'])).replace(tzinfo=None)

    def to_vusion_format(self, timestamp):
        return timestamp.strftime('%Y-%m-%dT%H:%M:%S')

    #TODO: fire error feedback if the ddialogue do not exit anymore
    #TODO: if dialogue is deleted, need to remove the scheduled message
    #(or they are also canceled if cannot find the dialogue)
    @inlineCallbacks
    def send_scheduled(self):
        self.log('Starting send_scheduled()')
        local_time = self.get_local_time()
        toSends = self.collection_schedules.find(
            spec={"datetime": {"$lt": self.to_vusion_format(local_time)}},
            sort=[("datetime", 1)])
        for toSend in toSends:
            self.collection_schedules.remove(
                {"_id": toSend.get('_id')})
            program = self.get_current_script()
            try:
                interaction = self.get_interaction(
                    program,
                    toSend['dialogue-id'],
                    toSend['interaction-id'])
                self.log("Send scheduled message %s to %s" % (
                    interaction['content'], toSend['participant-phone']))

                message = TransportUserMessage(**{'from_addr': self.properties['shortcode'],
                                                  'to_addr': toSend.get('participant-phone'),
                                                  'transport_name': self.transport_name,
                                                  'transport_type': self.transport_type,
                                                  'transport_metadata': '',
                                                  'content': interaction['content']
                                                  })
                yield self.transport_publisher.publish_message(message)
                self.log("Message has been send: %s" % message)
                self.save_status(message_content=message['content'],
                                  participant_phone=toSend['participant-phone'],
                                  message_type='send',
                                  message_status='pending',
                                  message_id=message['message_id'],
                                  timestamp=self.get_local_time(),
                                  dialogue_id=toSend['dialogue-id'],
                                  interaction_id=toSend['interaction-id'])
            except Exception as e:
                self.log("Error no reference, scheduled message dumpted: %s-%s"
                         % (toSend['dialogue-id'], toSend['interaction-id']))
                self.log("Exception is %s" % sys.exc_info()[0])
                self.log("Exception is %s" % e)

    #MongoDB do not support fetching a subpart of an array
    #may not be necessary in the near future
    #https://jira.mongodb.org/browse/SERVER-828
    #https://jira.mongodb.org/browse/SERVER-3089
    def get_interaction(self, program, dialogue_id, interaction_id):
        for dialogue in program['dialogues']:
            if dialogue["dialogue-id"] == dialogue_id:
                for interaction in dialogue["interactions"]:
                    if interaction["interaction-id"] == interaction_id:
                        return interaction

    def get_dialogue(self, program, dialogue_id):
        for dialogue in program['dialogues']:
            if dialogue["dialogue-id"] == dialogue_id:
                return dialogue

    def schedule_participants_dialogue(self, participants, dialogue):
        for participant in participants:
            self.schedule_participant_dialogue(participant, dialogue)

    #TODO: decide which id should be in an schedule object
    def schedule_participant_dialogue(self, participant, dialogue):
        previousSendDateTime = None
        try:
            for interaction in dialogue.get('interactions'):
                schedule = self.collection_schedules.find_one({
                    "participant-phone": participant['phone'],
                    "dialogue-id": dialogue["dialogue-id"],
                    "interaction-id": interaction["interaction-id"]})
                status = self.collection_status.find_one(
                    {"participant-phone": participant['phone'],
                     "dialogue-id": dialogue["dialogue-id"],
                     "interaction-id": interaction["interaction-id"]},
                    sort=[("datetime", pymongo.ASCENDING)])
                
                if status:
                    previousSendDateTime = iso8601.parse_date(status["timestamp"]).replace(tzinfo=None)                                                    
                    continue
                
                if (interaction['type-schedule'] == "immediately"):
                    if (schedule):
                        sendingDateTime = iso8601.parse_date(schedule['datetime']).replace(tzinfo=None)
                    else:
                        sendingDateTime = self.get_local_time()
                elif (interaction['type-schedule'] == "wait"):
                    sendingDateTime = previousSendDateTime + timedelta(minutes=int(interaction['minutes']))
                elif (interaction['type-schedule'] == "fixed-time"):
                    sendingDateTime = iso8601.parse_date(interaction['date-time']).replace(tzinfo=None)

                    #Scheduling a date already in the past is forbidden.
                if (sendingDateTime + timedelta(minutes=10) < self.get_local_time()):
                    self.save_status(message_content='Not generated yet',
                                     participant_phone=participant['phone'],
                                     message_type='send',
                                     message_status='fail: date in the past',
                                     dialogue_id=dialogue['dialogue-id'],
                                     interaction_id=interaction["interaction-id"])
                    if (schedule):
                        self.collection_schedules.remove(schedule['_id'])
                        continue

                if (not schedule):
                    schedule = {"participant-phone": participant['phone'],
                                "dialogue-id": dialogue['dialogue-id'],
                                "interaction-id": interaction["interaction-id"]}
                schedule['datetime'] = self.to_vusion_format(sendingDateTime)
                previousSendDateTime = sendingDateTime
                self.collection_schedules.save(schedule)
                self.log("Schedule has been saved: %s" % schedule)
        except:
            self.log("Scheduling exception with time: %s"
                     % interaction, 'error')
            self.log("Exception is %s"
                     % (sys.exc_info()[0]), 'error')
            
    def log(self, msg, level='msg'):
        if (level == 'msg'):
            log.msg('[%s] %s' % (self.control_name, msg))
        else:
            log.error('[%s] %s' % (self.control_name, msg))

    @inlineCallbacks
    def _setup_dispatcher_publisher(self):
        self.dispatcher_publisher = yield self.publish_to(
            '%(dispatcher_name)s.control' % self.config)

    @inlineCallbacks
    def register_keywords_in_dispatcher(self, keywords):
        keyword_mappings = []
        for keyword in keywords:
            keyword_mappings.append((self.transport_name, keyword))
        msg = Message(**{'message_type': 'add_exposed',
                         'exposed_name': self.transport_name,
                         'keyword_mappings': keyword_mappings})
        yield self.dispatcher_publisher.publish_message(msg)
