# -*- test-case-name: tests.test_ttc -*-

import sys, traceback
import re

from twisted.internet.defer import (inlineCallbacks, Deferred)
from twisted.enterprise import adbapi
from twisted.internet import task

import pymongo
from bson.objectid import ObjectId

import redis

from datetime import datetime, time, date, timedelta
import pytz

from vumi.application import ApplicationWorker
from vumi.message import Message, TransportUserMessage, TransportEvent
from vumi.application import SessionManager
from vumi import log

from vusion.vusion_script import VusionScript, split_keywords
from vusion.utils import (time_to_vusion_format, get_local_time,
                          get_local_time_as_timestamp, time_from_vusion_format)
from vusion.error import MissingData, SendingDatePassed, VusionError


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

        #Store basic configuration data
        self.transport_name = self.config['transport_name']
        self.control_name = self.config['control_name']
        self.transport_type = 'sms'
        self.r_config = self.config.get('redis', {})
        self.r_prefix = "%(control_name)s:" % self.config

        #Initializing
        self.program_name = None
        self.last_script_used = None
        self.properties = {}
        self.r_server = redis.Redis(**self.r_config)
        self._d.callback(None)

        self.collections = {}
        self.init_program_db(self.config['database_name'],
                             self.config['vusion_database_name'])

        send_loop_period = self.config['send_loop_period'] if 'send_loop_period' in self.config else "60"
        self.sender = task.LoopingCall(self.daemon_process)
        self.sender.start(float(send_loop_period))

        #Set up dispatcher publisher
        self.dispatcher_publisher = yield self.publish_to(
            '%(dispatcher_name)s.control' % self.config)
        
        #Set up control consumer
        self.control_consumer = yield self.consume(
            '%(control_name)s.control' % self.config,
            self.consume_control,
            message_class=Message)
                
        if self.is_ready():
            yield self.register_keywords_in_dispatcher()

    def stopWorker(self):
        self.log("Worker is stopped.")
        if (self.sender.running):
            self.sender.stop()

    def save_history(self, message_content, participant_phone, message_type,
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
        self.collections['history'].save(history)

    def get_current_dialogue(self, dialogue_id):
        for dialogue in self.collections['dialogues'].find(
            {'activated': 1, 'dialogue-id': dialogue_id},
            sort=[('modified', pymongo.DESCENDING)],
            limit=1):
            return dialogue
        return None

    def get_active_dialogues(self):
        return self.collections['dialogues'].group(
            ['dialogue-id'],
            None,
            {'Dialogue': 0},
            """function(obj, prev){
            if (obj.activated &&
            (prev.Dialogue==0 || prev.Dialogue.modified <= obj.modified))
            prev.Dialogue = obj;}"""
            )

    def get_dialogue(self, dialogue_id):
        dialogue = self.collections['dialogues'].find_one(
            {'_id': ObjectId(dialogue_id)})
        return dialogue

    def init_program_db(self, database_name, vusion_database_name):
        self.log("Initialization of the program")
        self.database_name = database_name
        self.vusion_database_name = vusion_database_name
        self.log("Connecting to database: %s" % self.database_name)

        #Initilization of the database
        connection = pymongo.Connection("localhost", 27017)
        self.db = connection[self.database_name]
        self.setup_collections(['dialogues',
                                'participants',
                                'history',
                                'schedules',
                                'program_settings',
                                'unattached_messages',
                                'requests'])
        
        self.db = connection[self.vusion_database_name]
        self.setup_collections(['templates'])

    def setup_collections(self, names):
        for name in names:
            self.setup_collection(name)

    def setup_collection(self, name):
        if name in self.db.collection_names():
            self.collections[name] = self.db[name]
        else:
            self.collections[name] = self.db.create_collection(name)
        self.log("Collection initialised: %s" % name)

    @inlineCallbacks
    def consume_control(self, message):
        try:
            self.log("Control message received to %r" % (message['action'],))
            if (not self.is_ready()):
                self.log("Worker is not ready, cannot performe the action.")
                return
            if message['action'] == 'update-schedule':
                yield self.register_keywords_in_dispatcher()
                self.schedule()
            elif message['action'] == 'test-send-all-messages':
                dialogue = self.get_dialogue(message['dialogue_obj_id'])
                yield self.send_all_messages(dialogue, message['phone_number'])
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error during consume control message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
            

    def dispatch_event(self, message):
        self.log("Event message received %s" % (message,))
        status = self.collections['history'].find_one({
            'message-id': message['user_message_id']
        })
        if (not status):
            self.log('No reference of this event in history, nothing stored.')
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
        self.collections['history'].save(status)

    def get_matching_request_actions(self, content, actions):
        regx = re.compile(('(,\s|^)%s($|,)' % content), re.IGNORECASE)
        matching_request = self.collections['requests'].find_one(
            {'keyword': {'$regex': regx}})
        if matching_request:
            if 'actions' in matching_request:
                for action in matching_request['actions']:
                    actions.append(action)
            if 'responses' in matching_request:
                for response in matching_request['responses']:
                    actions.append(
                        {'type-action': 'feedback',
                         'content': response['content']})
        return actions

    def run_action(self, participant_phone, action):
        if (action['type-action'] == 'optin'):
            self.collections['participants'].save({'phone': participant_phone})
        elif (action['type-action'] == 'optout'):
            self.collections['participants'].update(
                {'phone': participant_phone},
                {'$set': {'optout': True}})
        elif (action['type-action'] == 'feedback'):
            self.collections['schedules'].save({
                'date-time': time_to_vusion_format(self.get_local_time()),
                'content': action['content'],
                'type-content': 'feedback',
                'participant-phone': participant_phone
            })
        elif (action['type-action'] == 'tagging'):
            self.collections['participants'].update(
                {'phone': participant_phone, 'tags': {'$ne':action['tag']}},
                {'$push': {'tags': action['tag']}})
        elif (action['type-action'] == 'enrolling'):
            self.collections['participants'].update(
                {'phone': participant_phone, 'enrolled': {'$ne': action['enroll']}},
                {'$push': {'enrolled': action['enroll']}}, True)
            dialogue = self.get_current_dialogue(action['enroll'])
            participant = self.collections['participants'].find_one(
                {'phone': participant_phone})
            self.schedule_participant_dialogue(participant, dialogue)
        elif (action['type-action'] == 'profiling'):
            self.collections['participants'].update(
                {'phone': participant_phone},
                {'$set': {action['label']: action['value']}})
        else:
            self.log("The action is not supported %s" % action['type-action'])

    def consume_user_message(self, message):
        self.log("User message received from %s '%s'" % (message['from_addr'],
                                                         message['content']))
        try:
            ref = None
            actions = []
            active_dialogues = self.get_active_dialogues()
            for dialogue in active_dialogues:
                scriptHelper = VusionScript(dialogue['Dialogue'])
                ref, actions = scriptHelper.get_matching_reference_and_actions(
                    message['content'], actions)
                if ref:
                    break
            actions = self.get_matching_request_actions(
                message['content'],
                actions)
            self.save_history(
                message_content=message['content'],
                participant_phone=message['from_addr'],
                message_type='received',
                reference_metadata=ref)
            for action in actions:
                self.run_action(message['from_addr'], action)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    @inlineCallbacks
    def daemon_process(self):
        #self.log('Starting daemon_process()')
        self.load_data()
        if not self.is_ready():
            return
        yield self.send_scheduled()

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

    def schedule(self):
        #Schedule the dialogues
        active_dialogues = self.get_active_dialogues()
        for dialogue in active_dialogues:
            if ('auto-enrollment' in dialogue['Dialogue']
                and dialogue['Dialogue']['auto-enrollment'] == 'all'):
                participants = self.collections['participants'].find(
                    {'optout':{'$ne':True}})
            else:
                participants = self.collections['participants'].find(
                    {'enrolled': dialogue['dialogue-id'],
                     'optout':{'$ne':True}})
            self.schedule_participants_dialogue(
                    participants,
                    dialogue['Dialogue'])
        #Schedule the nonattached messages
        self.schedule_participants_unattach_messages(
            self.collections['participants'].find())

    def get_future_unattach_messages(self):
        return self.collections['unattached_messages'].find({
            'schedule': {
                '$gt': time_to_vusion_format(self.get_local_time())
            }})

    def schedule_participants_unattach_messages(self, participants):
        for participant in self.collections['participants'].find():
            self.schedule_participant_unattach_messages(participant)

    def schedule_participant_unattach_messages(self, participant):
        unattach_messages = self.get_future_unattach_messages()
        for unattach_message in unattach_messages:
            schedule = self.collections['schedules'].find_one({
                'participant-phone': participant['phone'],
                'unattach-id': unattach_message['_id']})
            status = self.collections['history'].find_one({
                'participant-phone': participant['phone'],
                'unattach-id': unattach_message['_id']})
            if status is not None:
                continue
            if schedule is None:
                schedule = {
                'participant-phone': participant['phone'],
                'unattach-id': unattach_message['_id'],
                }
            schedule['date-time'] = unattach_message['schedule']
            self.collections['schedules'].save(schedule)

    def schedule_participants_dialogue(self, participants, dialogue):
        for participant in participants:
            self.schedule_participant_dialogue(participant, dialogue)

    #TODO: decide which id should be in an schedule object
    def schedule_participant_dialogue(self, participant, dialogue):
        previousSendDateTime = None
        #self.log('Scheduling for %s dialogue %r' % (participant, dialogue))
        try:
            for interaction in dialogue.get('interactions'):
                schedule = self.collections['schedules'].find_one({
                    "participant-phone": participant['phone'],
                    "dialogue-id": dialogue["dialogue-id"],
                    "interaction-id": interaction["interaction-id"]})
                status = self.collections['history'].find_one(
                    {"participant-phone": participant['phone'],
                     "dialogue-id": dialogue["dialogue-id"],
                     "interaction-id": interaction["interaction-id"]},
                    sort=[("datetime", pymongo.ASCENDING)])

                if status:
                    previousSendDateTime = time_from_vusion_format(status["timestamp"])
                    continue

                if (interaction['type-schedule'] == 'immediately'):
                    if (schedule):
                        sendingDateTime = time_from_vusion_format(schedule['date-time'])
                    else:
                        sendingDateTime = self.get_local_time()
                elif (interaction['type-schedule'] == 'wait'):
                    sendingDateTime = previousSendDateTime + timedelta(minutes=int(interaction['minutes']))
                elif (interaction['type-schedule'] == 'fixed-time'):
                    sendingDateTime = time_from_vusion_format(interaction['date-time'])

                #Scheduling a date already in the past is forbidden.
                if (sendingDateTime + timedelta(minutes=10) < self.get_local_time()):
                    self.save_history(
                        message_content='Not generated yet',
                        participant_phone=participant['phone'],
                        message_type='sent',
                        message_status='fail: date in the past',
                        reference_metadata={
                            'dialogue-id': dialogue['dialogue-id'],
                            'interaction-id': interaction["interaction-id"]})
                    if (schedule):
                        self.collections['schedules'].remove(schedule['_id'])
                        continue

                if (not schedule):
                    schedule = {
                        "participant-phone": participant['phone'],
                        "dialogue-id": dialogue['dialogue-id'],
                        "interaction-id": interaction["interaction-id"]}
                schedule['date-time'] = self.to_vusion_format(sendingDateTime)
                previousSendDateTime = sendingDateTime
                self.collections['schedules'].save(schedule)
                self.log("Schedule has been saved: %s" % schedule)
        except:
            self.log("Scheduling exception: %s" % interaction)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))


    def get_local_time(self):
        try:
            return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(
                pytz.timezone(self.properties['timezone'])).replace(tzinfo=None)
        except:
            return datetime.utcnow()

    #TODO: Move into Utils
    def to_vusion_format(self, timestamp):
        return timestamp.strftime('%Y-%m-%dT%H:%M:%S')

    def from_schedule_to_message(self, schedule):
        if 'dialogue-id' in schedule:
            interaction = self.get_interaction(
                self.get_active_dialogues(),
                schedule['dialogue-id'],
                schedule['interaction-id'])
            reference_metadata = {
                'dialogue-id': schedule['dialogue-id'],
                'interaction-id': schedule['interaction-id']
            }
        elif 'unattach-id' in schedule:
            interaction = self.collections['unattached_messages'].find_one(
                {'_id': ObjectId(schedule['unattach-id'])})
            reference_metadata = {
                'unattach-id': schedule['unattach-id']
            }
        elif 'type-content' in schedule:
            interaction = {
                'content': schedule['content'],
                'type-interaction': 'feedback'}
            reference_metadata = None
        else:
            self.log("Error schedule object not supported: %s"
                     % (schedule))
            return None, None
        return interaction, reference_metadata

    #TODO: fire error feedback if the ddialogue do not exit anymore
    #TODO: if dialogue is deleted, need to remove the scheduled message
    #(or they are also canceled if cannot find the dialogue)
    @inlineCallbacks
    def send_scheduled(self):
        self.log('Checking the schedule list...')
        local_time = self.get_local_time()
        toSends = self.collections['schedules'].find(
            spec={'date-time': {'$lt': time_to_vusion_format(local_time)}},
            sort=[('date-time', 1)])
        for toSend in toSends:
            self.collections['schedules'].remove(
                {'_id': toSend['_id']})
            message_content = None
            try:
                interaction, reference_metadata = self.from_schedule_to_message(toSend)

                if not interaction:
                    continue

                message_content = self.generate_message(interaction)
                message_content = self.customize_message(
                    toSend['participant-phone'],
                    message_content)

                if (time_from_vusion_format(toSend['date-time']) <
                    (local_time - timedelta(minutes=15))):
                    raise SendingDatePassed(
                        "Message should have been send at %s" %
                        (toSend['date-time'],))

                message = TransportUserMessage(**{
                    'from_addr': self.properties['shortcode'],
                    'to_addr': toSend['participant-phone'],
                    'transport_name': self.transport_name,
                    'transport_type': self.transport_type,
                    'transport_metadata': '',
                    'content': message_content})
                yield self.transport_publisher.publish_message(message)
                self.log(
                    "Message has been send to %s '%s'" % (message['to_addr'],
                                                          message['content']))
                self.save_history(
                    message_content=message['content'],
                    participant_phone=message['to_addr'],
                    message_type='sent',
                    message_status='pending',
                    message_id=message['message_id'],
                    reference_metadata=reference_metadata)

            except VusionError as e:
                self.save_history(
                    message_content='',
                    participant_phone=toSend['participant-phone'],
                    message_type=None,
                    failure_reason=('%s' % (e,)),
                    reference_metadata=reference_metadata)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log(
                    "Error during consume user message: %r" %
                    traceback.format_exception(exc_type, exc_value, exc_traceback))
                self.save_history(
                    participant_phone=toSend['participant-phone'],
                    message_content='',
                    message_type='system-failed',
                    failure_reason=('%s - %s') % (sys.exc_info()[0],
                                                  sys.exc_info()[1]),
                    reference_metadata=reference_metadata)

    @inlineCallbacks
    def send_all_messages(self, dialogue, phone_number):
        for interaction in dialogue['interactions']:
            message_content = self.generate_message(interaction)
            message = TransportUserMessage(**{
                'from_addr': self.properties['shortcode'],
                'to_addr': phone_number,
                'transport_name': self.transport_name,
                'transport_type': self.transport_type,
                'transport_metadata': '',
                'content': message_content})
            yield self.transport_publisher.publish_message(message)
            self.log("Test message has been sent to %s '%s'"
                     % (message['to_addr'], message['content'],))

    #TODO: move into VusionScript
    #MongoDB do not support fetching a subpart of an array
    #may not be necessary in the near future
    #https://jira.mongodb.org/browse/SERVER-828
    #https://jira.mongodb.org/browse/SERVER-3089
    def get_interaction(self, active_dialogues, dialogue_id, interaction_id):
        for dialogue in active_dialogues:
            if dialogue['dialogue-id'] == dialogue_id:
                for interaction in dialogue['Dialogue']['interactions']:
                    if interaction["interaction-id"] == interaction_id:
                        return interaction

    def log(self, msg, level='msg'):
        timezone = None
        local_time = self.get_local_time()
        rkey = "%slogs" % (self.r_prefix,)
        self.r_server.zremrangebyscore(
            rkey,
            1,
            get_local_time_as_timestamp(
                local_time - timedelta(hours=2))
        )
        self.r_server.zadd(
            rkey,
            "[%s] %s" % (
                time_to_vusion_format(local_time),
                msg),
            get_local_time_as_timestamp(local_time))
        if (level == 'msg'):
            log.msg('[%s] %s' % (self.control_name, msg))
        else:
            log.error('[%s] %s' % (self.control_name, msg))

    @inlineCallbacks
    def register_keywords_in_dispatcher(self):
        self.log('Synchronizing with dispatcher')
        keywords = []
        for dialogue in self.get_active_dialogues():
            keywords += VusionScript(dialogue['Dialogue']).get_all_keywords()
        for request in self.collections['requests'].find():
            keyphrases = request['keyword'].split(', ')
            for keyphrase in keyphrases:
                if not (keyphrase.split(' ')[0]) in keywords:
                    keywords.append(keyphrase.split(' ')[0])
        keyword_mappings = []
        for keyword in keywords:
            keyword_mappings.append((self.transport_name, keyword))
        msg = Message(**{'message_type': 'add_exposed',
                         'exposed_name': self.transport_name,
                         'keyword_mappings': keyword_mappings})
        yield self.dispatcher_publisher.publish_message(msg)

    #TODO no template defined and no default template defined... what to do?
    def generate_message(self, interaction):
        regex_QUESTION = re.compile('QUESTION')
        regex_ANSWERS = re.compile('ANSWERS')
        regex_ANSWER = re.compile('ANSWER')
        regex_SHORTCODE = re.compile('SHORTCODE')
        regex_KEYWORD = re.compile('KEYWORD')
        regex_Breakline = re.compile('\\r\\n')

        message = interaction['content']
        if ('type-interaction' in interaction and interaction['type-interaction'] == 'question-answer'):
            if 'template' in interaction:
                #need to get the template
                pass
            else:
                if (interaction['type-question']=='closed-question'):
                    default_template = self.collections['program_settings'].find_one({"key": "default_template_closed_question"})
                elif (interaction['type-question']=='open-question'):
                    default_template = self.collections['program_settings'].find_one({"key": "default_template_open_question"})
                else:
                    #no default for this question type
                    pass
                template = self.collections['templates'].find_one({"_id": default_template['value']})
            #replace question
            message = re.sub(regex_QUESTION, interaction['content'], template['template'])
            #replace answers
            if (interaction['type-question']=='closed-question'):
                i = 1
                answers = ""
                for answer in interaction['answers']:
                    answers = ('%s%s. %s\\n' % (answers, i, answer['choice']))
                    i = i + 1
                message = re.sub(regex_ANSWERS, answers, message)
            #replace keyword
            keyword = split_keywords(interaction['keyword'])[0]            
            message = re.sub(regex_KEYWORD, keyword.upper(), message)
            #replace shortcode
            message = re.sub(regex_SHORTCODE, self.properties['shortcode'], message)
            if (interaction['type-question']=='open-question'):
                message = re.sub(regex_ANSWER, interaction['answer-label'], message)
            message = re.sub(regex_Breakline, '\n', message)
        return message

    def customize_message(self, participant_phone, message):
        tags_regexp = re.compile(r'\[(?P<table>\w*)\.(?P<attribute>\w*)\]')
        tags = re.findall(tags_regexp, message)
        for table, attribute in tags:
            participant = self.collections['participants'].find_one(
                {'phone': participant_phone})
            attribute = attribute.lower()
            if not attribute in participant:
                raise MissingData("%s has no attribute %s" %
                                  (participant_phone, attribute))
            message = message.replace('[%s.%s]' %
                                      (table, attribute),
                                      participant[attribute])
        return message
