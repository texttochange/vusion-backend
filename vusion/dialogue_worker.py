# -*- test-case-name: tests.test_ttc -*-

import sys
import re

from twisted.internet.defer import (inlineCallbacks, Deferred)
from twisted.enterprise import adbapi
from twisted.internet import task

import pymongo
from pymongo.objectid import ObjectId

import redis

from datetime import datetime, time, date, timedelta
import pytz
import iso8601

from vumi.application import ApplicationWorker
from vumi.message import Message, TransportUserMessage, TransportEvent
from vumi.application import SessionManager
from vumi import log

from vusion.vusion_script import VusionScript
from vusion.utils import (time_to_vusion_format, get_local_time,
                          get_local_time_as_timestamp)
from vusion.error import MissingData


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

        #Set up control consumer
        self.control_consumer = yield self.consume(
            '%(control_name)s.control' % self.config,
            self.consume_control,
            message_class=Message)
        #Set up dispatcher publisher
        self.dispatcher_publisher = yield self.publish_to(
            '%(dispatcher_name)s.control' % self.config)

        #Store basic configuration data
        self.transport_name = self.config['transport_name']
        self.control_name = self.config['control_name']
        self.transport_type = 'sms'
        self.r_config = self.config.get('redis', {})
        self.r_prefix = "%(control_name)s:" % self.config

        #Initializing
        self.sender = None
        self.program_name = None
        self.last_script_used = None
        self.collections = {}
        self.properties = {}
        self.r_server = redis.Redis(**self.r_config)

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

    def save_status(self, message_content, participant_phone, message_type,
                    message_status=None, message_id=None, failure_reason=None,
                    timestamp=None, reference_metadata=None):
        if timestamp:
            timestamp = time_to_vusion_format(timestamp)
        else:
            timestamp = time_to_vusion_format(self.get_local_time())
        history = {
            'message-id': message_id,
            'message-content': message_content,
            'participant-phone': participant_phone,
            'message-type': message_type,
            'message-status': message_status,
            'timestamp': timestamp,
        }
        if failure_reason is not None:
            history['failure-reason'] = failure_reason
        if reference_metadata is None:
            reference_metadata = {}
        for key, value in reference_metadata.iteritems():
            history[key] = value
        self.collection_status.save(history)

    def get_current_script_id(self):
        for script in self.collection_scripts.find(
            {'activated': 1},
            sort=[('modified', pymongo.DESCENDING)],
            limit=1):
            return script['_id']
        return None

    def get_current_script(self):
        for script in self.collection_scripts.find(
            {'activated': 1},
            sort=[('modified', pymongo.DESCENDING)],
            limit=1):
            return script['script']
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
        if message['action'] == 'init':
            config = message['config']
            self.init_program_db(config['database-name'])

        elif message['action'] == 'update-schedule':
            if self.is_ready():
                self.schedule()

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
                status['failure-reason'] = ("Code:%s Level:%s Message:%s" % (
                    message['failure_code'],
                    message['failure_level'],
                    message['failure_reason']))
        self.collection_status.save(status)

    def consume_user_message(self, message):
        self.log("User message: %s" % message['content'])
        try:
            script = self.get_current_script()
            if not script:
                self.save_status(message_content=message['content'],
                                 participant_phone=message['from_addr'],
                                 message_type='received')
                return
            scriptHelper = VusionScript(self.get_current_script())
            data = scriptHelper.get_matching_question_answer(message['content'])
            self.save_status(message_content=message['content'],
                             participant_phone=message['from_addr'],
                             message_type='received',
                             reference_metadata={
                                 'dialogue-id': data['dialogue-id'],
                                 'interaction-id': data['interaction-id'],
                                 'matching-answer': data['matching-answer']})
            for feedback in data['feedbacks']:
                self.collection_schedules.save({
                    'datetime': time_to_vusion_format(self.get_local_time()),
                    'content': feedback['content'],
                    'type-content': 'feedback',
                    'participant-phone': message['from_addr']
                })
        except:
            self.log(
                "Error during consume user message: %s %s" %
                (sys.exc_info()[0], sys.exc_info()[1]))

    @inlineCallbacks
    def daemon_process(self):
        self.log('Starting daemon_process()')
        self.load_data()
        if not self.is_ready():
            return
        #the schedule should be performed only upon request
        #self.schedule()
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
        if (('timezone' not in self.properties)
            and (self.properties['timezone'] not in pytz.all_timezones)):
            self.log('Timezone not defined or not supported')
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

    #TODO: to move into VusionScript
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
        #Schedule the script
        script = self.get_current_script()
        if (script and ('dialogues' in script)):
            self.schedule_participants_dialogue(
                self.collection_participants.find(),
                script['dialogues'][0])
        #Schedule the nonattached messages
        self.schedule_participants_unattach_messages(
            self.collection_participants.find())

    def get_future_unattach_messages(self):
        return self.collections['unattached_messages'].find({
            'schedule': {
                '$gt': time_to_vusion_format(self.get_local_time())
            }})

    def schedule_participants_unattach_messages(self, participants):
        self.log('scheduling unattach messages')
        for participant in self.collection_participants.find():
            self.schedule_participant_unattach_messages(participant)

    def schedule_participant_unattach_messages(self, participant):
        unattach_messages = self.get_future_unattach_messages()
        for unattach_message in unattach_messages:
            schedule = self.collection_schedules.find_one({
                'participant-phone': participant['phone'],
                'unattach-id': unattach_message['_id']})
            status = self.collection_status.find_one({
                'participant-phone': participant['phone'],
                'unattach-id': unattach_message['_id']})
            if status is not None:
                continue
            if schedule is None:
                schedule = {
                'participant-phone': participant['phone'],
                'unattach-id': unattach_message['_id'],
                }
            schedule['datetime'] = unattach_message['schedule']
            self.collection_schedules.save(schedule)

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

                if (interaction['type-schedule'] == 'immediately'):
                    if (schedule):
                        sendingDateTime = iso8601.parse_date(schedule['datetime']).replace(tzinfo=None)
                    else:
                        sendingDateTime = self.get_local_time()
                elif (interaction['type-schedule'] == 'wait'):
                    sendingDateTime = previousSendDateTime + timedelta(minutes=int(interaction['minutes']))
                elif (interaction['type-schedule'] == 'fixed-time'):
                    sendingDateTime = iso8601.parse_date(interaction['date-time']).replace(tzinfo=None)

                #Scheduling a date already in the past is forbidden.
                if (sendingDateTime + timedelta(minutes=10) < self.get_local_time()):
                    self.save_status(
                        message_content='Not generated yet',
                        participant_phone=participant['phone'],
                        message_type='send',
                        message_status='fail: date in the past',
                        reference_metadata={
                            'dialogue-id': dialogue['dialogue-id'],
                            'interaction-id': interaction["interaction-id"]})
                    if (schedule):
                        self.collection_schedules.remove(schedule['_id'])
                        continue

                if (not schedule):
                    schedule = {
                        "participant-phone": participant['phone'],
                        "dialogue-id": dialogue['dialogue-id'],
                        "interaction-id": interaction["interaction-id"]}
                schedule['datetime'] = self.to_vusion_format(sendingDateTime)
                previousSendDateTime = sendingDateTime
                self.collection_schedules.save(schedule)
                self.log("Schedule has been saved: %s" % schedule)
        except:
            self.log("Scheduling exception: %s" % interaction)
            self.log("Exception is %s" % (sys.exc_info()[0]))

    def get_local_time(self):
        if 'timezone' not in self.properties:
            self.log('Timezone property not defined, use UTC')
            return datetime.utcnow()
        return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(
            pytz.timezone(self.properties['timezone'])).replace(tzinfo=None)

    #TODO: Move into Utils
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
            spec={'datetime': {'$lt': time_to_vusion_format(local_time)}},
            sort=[('datetime', 1)])
        for toSend in toSends:
            self.collection_schedules.remove(
                {'_id': toSend['_id']})
            message_content = None
            try:
                if 'dialogue-id' in toSend:
                    interaction = self.get_interaction(
                        self.get_current_script(),
                        toSend['dialogue-id'],
                        toSend['interaction-id'])
                    reference_metadata = {
                        'dialogue-id': toSend['dialogue-id'],
                        'interaction-id': toSend['interaction-id']
                    }
                elif 'unattach-id' in toSend:
                    interaction = self.collections['unattached_messages'].find_one(
                        {'_id': ObjectId(toSend['unattach-id'])})
                    reference_metadata = {
                        'unattach-id': toSend['unattach-id']
                    }
                elif 'type-content' in toSend:
                    interaction = {'content': toSend['content'],
                                   'type-interaction': 'feedback'}
                    reference_metadata = None
                else:
                    self.log("Error schedule object not supported: %s"
                             % (toSend))
                    continue

                message_content = self.generate_message(
                        toSend['participant-phone'],
                        interaction
                )

                message = TransportUserMessage(**{
                    'from_addr': self.properties['shortcode'],
                    'to_addr': toSend['participant-phone'],
                    'transport_name': self.transport_name,
                    'transport_type': self.transport_type,
                    'transport_metadata': '',
                    'content': message_content})
                yield self.transport_publisher.publish_message(message)
                self.log("Message has been send: %s" % message)

                self.save_status(message_content=message['content'],
                                 participant_phone=message['to_addr'],
                                 message_type='send',
                                 message_status='pending',
                                 message_id=message['message_id'],
                                 reference_metadata=reference_metadata)
            except MissingData as e:
                self.save_status(message_content='',
                                 participant_phone=toSend['participant-phone'],
                                 message_type='generate-failed',
                                 failure_reason=('%s' % (e,)),
                                 reference_metadata=reference_metadata)
            except:
                self.log("Unexpected exception: %s" % toSend, 'error')
                self.log("Exception is %s - %s" % (sys.exc_info()[0],
                                                        sys.exc_info()[1]),
                         'error')
                self.save_status(participant_phone=toSend['participant-phone'],
                                 message_content='',
                                 message_type='system-failed',
                                 failure_reason=('%s - %s') % (sys.exc_info()[0],
                                                               sys.exc_info()[1]),
                                 reference_metadata=reference_metadata)

    #TODO: move into VusionScript
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

    #TODO: move into VusionScript
    def get_dialogue(self, program, dialogue_id):
        for dialogue in program['dialogues']:
            if dialogue["dialogue-id"] == dialogue_id:
                return dialogue

    def log(self, msg, level='msg'):
        timezone = None
        if 'timezone' in self.properties:
            timezone = self.properties['timezone']
        rkey = "%slogs" % (self.r_prefix,)
        self.r_server.zadd(rkey,
                           "[%s] %s" % (
                               time_to_vusion_format(get_local_time(timezone)),
                               msg),
                           get_local_time_as_timestamp(timezone))
        #log.msg('%s - %s - %s' % (rkey, get_local_time_as_timestamp(timezone), msg))
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

    #TODO move into VusionScript
    #Support the type-interaction is not defined
    def generate_message(self, participant_phone, interaction):
        message = interaction['content']

        if ('type-interaction' in interaction and
            interaction['type-interaction'] == 'question-answer'):
            if 'answers' in interaction:
                i = 1
                for answer in interaction['answers']:
                    message = ('%s %s. %s' % (message, i, answer['choice']))
                    i = i + 1
                message = ('%s To reply send: %s(space)(Answer Nr) to %s'
                           % (message, interaction['keyword'], self.properties['shortcode']))

            if 'answer-label' in interaction:
                message = ('%s To reply send: %s(space)(%s) to %s'
                           % (message, interaction['keyword'], interaction['answer-label'], self.properties['shortcode']))

        tags = re.findall(re.compile(r'\[(?P<table>\w*)\.(?P<attribute>\w*)\]'), message)
        for table, attribute in tags:
            participant = self.collection_participants.find_one({'phone': participant_phone})
            if not attribute in participant:
                raise MissingData("%s has no attribute %s" % (participant_phone, attribute))
            message = message.replace('[%s.%s]' % (table, attribute), participant[attribute])

        return message
