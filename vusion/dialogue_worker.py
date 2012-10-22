# -*- test-case-name: tests.test_ttc -*-

import sys
import traceback
import re

from uuid import uuid4

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
from vumi.utils import get_first_word

from vusion.persist import Dialogue
from vusion.utils import (time_to_vusion_format, get_local_time,
                          get_local_time_as_timestamp, time_from_vusion_format,
                          get_shortcode_value, get_offset_date_time,
                          split_keywords)
from vusion.error import (MissingData, SendingDatePassed, VusionError,
                          MissingTemplate)
from vusion.message import DispatcherControl
from vusion.action import (Actions, action_generator,FeedbackAction,
                           EnrollingAction, OptinAction, OptoutAction,
                           RemoveRemindersAction)
from vusion.persist import Request


class DialogueWorker(ApplicationWorker):
    
    INCOMING = "incoming"
    OUTGOING = "outgoing"
    
    def __init__(self, *args, **kwargs):
        super(DialogueWorker, self).__init__(*args, **kwargs)

    def startService(self):
        self._d = Deferred()
        self._consumers = []
        self.properties = {
            'shortcode':None,
            'timezone': None,
            'international-prefix': None,
            'default-template-closed-question': None,
            'default-template-open-question': None,
            'default-template-unmatching-answer': None,
            'unmatching-answer-remove-reminder': 0, 
            'customized-id': None,
            'double-matching-answer-feedback': None}
        self.sender = None
        self.r_prefix = None
        self.r_config = {}
        self.control_name = None
        self.transport_name = None
        self.transport_type = None
        self.program_name = None
        super(DialogueWorker, self).startService()

    @inlineCallbacks
    def setup_application(self):
        log.msg("One Generic Worker is starting")

        #Store basic configuration data
        self.transport_name = self.config['transport_name']
        self.control_name = self.config['control_name']
        self.transport_type = 'sms'
        self.r_config = self.config.get('redis', {})
        self.r_prefix = "%(control_name)s:" % self.config

        #Initializing
        self.program_name = None
        self.last_script_used = None
        self.r_server = redis.Redis(**self.r_config)
        self._d.callback(None)

        self.collections = {}
        self.init_program_db(self.config['database_name'],
                             self.config['vusion_database_name'])

        #Set up dispatcher publisher
        self.dispatcher_publisher = yield self.publish_to(
            '%(dispatcher_name)s.control' % self.config)

        #Set up control consumer
        self.control_consumer = yield self.consume(
            '%(control_name)s.control' % self.config,
            self.consume_control,
            message_class=Message)
        self._consumers.append(self.control_consumer)

        send_loop_period = (self.config['send_loop_period']
                            if 'send_loop_period' in self.config else "60")
        self.sender = task.LoopingCall(self.daemon_process)
        self.sender.start(float(send_loop_period))

        self.load_data()
        if self.is_ready():
            yield self.register_keywords_in_dispatcher()

    @inlineCallbacks
    def teardown_application(self):
        self.log("Worker is stopped.")
        if self.is_ready():
            yield self.unregister_from_dispatcher()
        if (self.sender and self.sender.running):
            self.sender.stop()

    def save_schedule(self, participant_phone, date_time, object_type,
                      action=None, origin=None, **kwargs):
        schedule = {
            'object-type': object_type,
            'participant-phone': participant_phone,
            'date-time': date_time}        
        if '_id' in kwargs and kwargs['_id'] is not None: 
            schedule['_id']= kwargs['_id']
        if object_type == 'dialogue-schedule' or object_type=='deadline-schedule' or object_type=='reminder-schedule':
            schedule['dialogue-id']=kwargs['dialogue_id']
            schedule['interaction-id']=kwargs['interaction_id']
        elif object_type == 'unattach-schedule':
            schedule['unattach-id']=kwargs['unattach_id']
        elif object_type == 'feedback-schedule':
            schedule['content']=kwargs['content']
            schedule['type-content']='feedback'
        elif object_type == 'action-schedule':
            schedule['action'] = action.get_as_dict()
            if origin is not None:
                for key in origin:
                    schedule[key] = origin[key]
        else:
            raise VusionError('object type not supported by schedule %s' % object_type)
        #self.log("Save schedule %r" % schedule)
        self.collections['schedules'].save(schedule)        

    def save_history(self, message_content, participant_phone,
                     message_direction, participant_session_id=None,
                     message_status=None, message_id=None,
                     failure_reason=None, timestamp=None,
                     reference_metadata=None):
        if timestamp:
            timestamp = time_to_vusion_format(timestamp)
        else:
            timestamp = time_to_vusion_format(self.get_local_time())
        history = {
            'message-content': message_content,
            'participant-phone': participant_phone,
            'message-direction': message_direction,            
            'participant-session-id': participant_session_id,
            'message-status': message_status,
            'message-id': message_id,
            'timestamp': timestamp,
        }
        if failure_reason is not None:
            history['failure-reason'] = failure_reason
        if reference_metadata is None:
            reference_metadata = {}
        for key, value in reference_metadata.iteritems():
            if (key=='interaction'):
                continue
            history[key] = value
        self.collections['history'].save(history)

    def get_participant_session_id(self, participant_phone):
        participant = self.collections['participants'].find_one({'phone':participant_phone})
        if participant is None:
            return None
        else:
            return participant['session-id']

    def get_current_dialogue(self, dialogue_id):
        try:
            dialogue = self.get_active_dialogues({'dialogue-id': dialogue_id})
            if dialogue == []:
                return None
            return dialogue[0]
        except:
            self.log("Cannot get current dialogue %s" % dialogue_id)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    def get_active_dialogues(self, conditions=None):
        dialogues = self.collections['dialogues'].group(
            ['dialogue-id'],
            conditions,
            {'Dialogue': 0},
            """function(obj, prev){
            if (obj.activated==1 &&
            (prev.Dialogue==0 || prev.Dialogue.modified <= obj.modified))
            prev.Dialogue = obj;}"""
        )
        active_dialogues = []
        for dialogue in dialogues:
            if dialogue['Dialogue'] == 0.0:
                continue
            try:
                active_dialogues.append(Dialogue(**dialogue['Dialogue']))
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log(
                    "Error while applying dialogue model on dialogue %s: %r" %
                    (dialogue['Dialogue']['name'],
                     traceback.format_exception(exc_type, exc_value, exc_traceback)))
        return active_dialogues

    def get_dialogue_obj(self, dialogue_obj_id):
        dialogue = self.collections['dialogues'].find_one(
            {'_id': ObjectId(dialogue_obj_id)})
        return dialogue
    
    def init_program_db(self, database_name, vusion_database_name):
        self.log("Initialization of the program")
        self.database_name = database_name
        self.vusion_database_name = vusion_database_name
        self.log("Connecting to database: %s" % self.database_name)

        #Initilization of the database
        connection = pymongo.Connection(self.config['mongodb_host'],
                                        self.config['mongodb_port'])
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
            self.load_data()
            if (not self.is_ready()):
                self.log("Worker is not ready, cannot performe the action.")
                return
            if message['action'] == 'update-schedule':
                yield self.register_keywords_in_dispatcher()
                self.schedule()
            elif message['action'] == 'test-send-all-messages':
                dialogue = self.get_dialogue_obj(message['dialogue_obj_id'])
                yield self.send_all_messages(dialogue, message['phone_number'])
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error during consume control message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    def dispatch_event(self, message):
        self.log("Event message received %s" % (message,))
        history = self.collections['history'].find_one({
            'message-id': message['user_message_id']
        })
        if (not history):
            self.log('No reference of this event in history, nothing stored.')
            return
        if (message['event_type'] == 'ack'):
            history['message-status'] = 'ack'
        if (message['event_type'] == 'delivery_report'):
            history['message-status'] = message['delivery_status']
            if (message['delivery_status'] == 'failed'):
                history['failure-reason'] = ("Code:%s Level:%s Message:%s" % (
                    message['failure_code'],
                    message['failure_level'],
                    message['failure_reason']))
        self.collections['history'].save(history)

    def get_matching_request_actions(self, content, actions):
        # exact matching
        exact_regex = re.compile(('(,\s|^)%s($|,)' % content), re.IGNORECASE)
        matching_request = self.collections['requests'].find_one(
            {'keyword': {'$regex': exact_regex}})
        if matching_request:
            request = Request(**matching_request)
            request.append_actions(actions)
            return {'request-id': matching_request['_id']}, actions
        # lazy keyword matching
        lazy_regex = re.compile(
            ('(,\s|^)%s(\s.*|$|,)' % get_first_word(content)), re.IGNORECASE)
        matching_request = self.collections['requests'].find_one(
            {'keyword': {'$regex': lazy_regex},
             'set-no-request-matching-try-keyword-only': 'no-request-matching-try-keyword-only'})
        if matching_request:
            request = Request(**matching_request)
            request.append_actions(actions)
            return {'request-id': matching_request['_id']}, actions
        return None, actions 

    def create_participant(self, participant_phone):
        return {
            'model-version': 1,
            'phone': participant_phone,
            'session-id': uuid4().get_hex(), 
            'last-optin-date': time_to_vusion_format(self.get_local_time()),
            'tags': [],
            'enrolled':[],
            'profile':[]
        }

    def run_action(self, participant_phone, action, origin=None):
        regex_ANSWER = re.compile('ANSWER')
        self.log(("Run action for %s %s" % (participant_phone, action,)))
        if (action.get_type() == 'optin'):
            participant = self.collections['participants'].find_one({'phone': participant_phone})
            if participant:
                if (participant['session-id'] != None):
                    return
                self.collections['participants'].update(
                    {'phone': participant_phone},
                    {'$set': {'session-id': uuid4().get_hex(), 
                              'last-optin-date': time_to_vusion_format(self.get_local_time()),
                              'tags': [],
                              'enrolled': [],
                              'profile': [] }})
            else:
                self.collections['participants'].save(self.create_participant(participant_phone))
            for dialogue in self.get_active_dialogues({'auto-enrollment':'all'}):
                self.run_action(participant_phone, EnrollingAction(**{'enroll': dialogue['dialogue-id']}))            
        elif (action.get_type() == 'optout'):
            self.collections['participants'].update(
                {'phone': participant_phone},
                {'$set': {'session-id': None,
                          'last-optin-date': None}})
            self.collections['schedules'].remove({
                'participant-phone': participant_phone,
                'object-type': {'$ne': 'feedback-schedule'}})
        elif (action.get_type() == 'feedback'):
            self.save_schedule(participant_phone,
                               time_to_vusion_format(self.get_local_time()),
                               'feedback-schedule',
                               content=action['content'])
        elif (action.get_type() == 'unmatching-answer'):
            setting = self.collections['program_settings'].find_one({
                'key': 'default-template-unmatching-answer'})
            if setting is None:
                return
            template = self.collections['templates'].find_one({
                '_id': ObjectId(setting['value'])})
            if template is None:
                return
            error_message = TransportUserMessage(**{
                'from_addr': '8282',
                'to_addr': participant_phone,
                'transport_name': None,
                'transport_type': None,
                'transport_metadata': None,
                'content': re.sub(regex_ANSWER,
                                  action['answer'],
                                  template['template'])
            })
            self.save_schedule(participant_phone,
                               time_to_vusion_format(self.get_local_time()),
                               'feedback-schedule',
                               content=error_message['content'])
            log.debug("Reply '%s' sent to %s" %
                      (error_message['content'], error_message['to_addr']))
        elif (action.get_type() == 'tagging'):
            self.collections['participants'].update(
                {'phone': participant_phone,
                 'session-id': {'$ne': None},
                 'tags': {'$ne': action['tag']}},
                {'$push': {'tags': action['tag']}})
        elif (action.get_type() == 'enrolling'):
            self.run_action(participant_phone, OptinAction())
            self.collections['participants'].update(
                {'phone': participant_phone,
                 'enrolled.dialogue-id': {'$ne': action['enroll']}},
                {'$push': {'enrolled': {'dialogue-id': action['enroll'],
                                        'date-time': time_to_vusion_format(self.get_local_time())}}})
            dialogue = self.get_current_dialogue(action['enroll'])
            if dialogue is None:
                self.log(("Enrolling error: Missing Dialogue %s" % action['enroll']))
                return
            participant = self.collections['participants'].find_one(
                {'phone': participant_phone})
            self.schedule_participant_dialogue(participant, dialogue)
        elif (action.get_type() == 'delayed-enrolling'):
            self.save_schedule(
                participant_phone,
                time_to_vusion_format(get_offset_date_time(self.get_local_time(), 
                                                           action['offset-days']['days'],
                                                           action['offset-days']['at-time'])),
                'action-schedule',
                EnrollingAction(**{'enroll': action['enroll']}),
                origin)
        elif (action.get_type() == 'profiling'):
            self.collections['participants'].update(
                {'phone': participant_phone,
                 'session-id': {'$ne': None}},
                {'$push': {'profile': {'label': action['label'],
                                        'value': action['value']}}})
        elif (action.get_type() == 'offset-conditioning'):
            self.schedule_participant_dialogue(
                self.collections['participants'].find_one({'phone':participant_phone,
                                                          'session-id':{'$ne':None}}),
                self.get_current_dialogue(action['dialogue-id']))
        elif (action.get_type() == 'remove-question'):
            self.collections['schedules'].remove({
                'participant-phone': participant_phone,
                'dialogue-id': action['dialogue-id'],
                'interaction-id': action['interaction-id'],
                'object-type': 'dialogue-schedule'})
        elif (action.get_type() == 'remove-reminders'):
            self.collections['schedules'].remove({
                'participant-phone': participant_phone,
                'dialogue-id': action['dialogue-id'],
                'interaction-id': action['interaction-id'],
                'object-type': 'reminder-schedule'})
        elif (action.get_type() == 'remove-deadline'):
            self.collections['schedules'].remove({
                'participant-phone': participant_phone,
                'dialogue-id': action['dialogue-id'],
                'interaction-id': action['interaction-id'],
                'object-type': 'deadline-schedule'})
        elif (action.get_type() == 'reset'):
            self.run_action(participant_phone, OptoutAction())
            self.run_action(participant_phone, OptinAction())
        else:
            self.log("The action is not supported %s" % action.get_type())

    def consume_user_message(self, message):
        self.log("User message received from %s '%s'" % (message['from_addr'],
                                                         message['content']))
        try:
            ref = None
            actions = Actions()
            active_dialogues = self.get_active_dialogues()
            for dialogue in active_dialogues:
                ref, actions = dialogue.get_matching_reference_and_actions(
                    message['content'], actions)
                if ref:
                    break
            if ref is None:
                ref, actions = self.get_matching_request_actions(
                    message['content'],
                    actions)
            # High priority to run an optin or enrolling action to get sessionId 
            if (self.get_participant_session_id(message['from_addr']) is None 
                    and (actions.contains('optin') or actions.contains('enrolling'))):
                self.run_action(message['from_addr'], actions.get_priority_action())
            participant = self.collections['participants'].find_one(
                {'phone': message['from_addr']})
            self.save_history(
                message_content=message['content'],
                participant_phone=message['from_addr'],
                participant_session_id=(participant['session-id'] if participant else None),
                message_direction='incoming',
                reference_metadata=ref)
            if (not ref is None):
                if ('interaction' in ref):
                    if self.participant_has_max_unmatching_answers(participant, ref['dialogue-id'], ref['interaction']):
                        ref['interaction'].get_max_unmatching_action(ref['dialogue-id'], actions)
                self.get_program_actions(participant, ref, actions)
                self.run_actions(participant, ref, actions)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    def run_actions(self, participant, ref, actions):
        if ((not 'request-id' in ref)
            and (participant['session-id'] is None
                 or not self.is_enrolled(participant, ref['dialogue-id']))):
            return
        for action in actions.items():
            self.run_action(participant['phone'], action, ref)

    def get_program_actions(self, participant, context, actions):
        if self.properties['unmatching-answer-remove-reminder']==1:
            if ('interaction' in context 
                and context['interaction'].has_reminder()
                and context['matching-answer'] is None):
                actions.append(RemoveRemindersAction(**{
                    'dialogue-id': context['dialogue-id'],
                    'interaction-id': context['interaction-id']}))
        if ('matching-answer' in context 
            and self.has_already_valid_answer(participant, context['dialogue-id'], context['interaction-id'])):
            actions.clear_all()
            if self.properties['double-matching-answer-feedback'] is not None:
                actions.append(FeedbackAction(**{'content': self.properties['double-matching-answer-feedback']}))

    def is_enrolled(self, participant, dialogue_id):
        for enrolled in participant['enrolled']:
            if enrolled['dialogue-id']==dialogue_id:
                return True
        return False

    def has_already_valid_answer(self, participant, dialogue_id, interaction_id):
        query = {'participant-phone': participant['phone'],
                 'participant-session-id':participant['session-id'],
                 'message-direction': 'incoming',
                 'matching-answer': {'$ne': None},
                 'dialogue-id': dialogue_id,
                 'interaction-id': interaction_id}
        history = self.collections['history'].find(query)
        if history is None or history.count() <= 1:
            return False
        return True
    
    def participant_has_max_unmatching_answers(self, participant, dialogue_id, interaction):
        if (not interaction.has_max_unmatching_answers()):
            return False
        query = {'participant-phone': participant['phone'],
                 'participant-session-id':participant['session-id'],
                 'message-direction': 'incoming',
                 'dialogue-id': dialogue_id,
                 'interaction-id': interaction['interaction-id'],
                 'matching-answer': None}
        history = self.collections['history'].find(query)
        if history.count() < int(interaction['max-unmatching-answer-number']):
            return False
        return True
    
    def get_max_unmatching_answers_interaction(self, dialogue_id, interaction_id):
        dialogue = self.get_current_dialogue(dialogue_id)
        returned_interaction = dialogue.get_interaction(interaction_id)
        if returned_interaction.has_max_unmatching_answers():
            return returned_interaction
        return None

    @inlineCallbacks
    def daemon_process(self):
        #self.log('Starting daemon_process()')
        previous_shortcode = self.properties['shortcode']
        self.load_data()
        if previous_shortcode != self.properties['shortcode']:
            self.register_keywords_in_dispatcher()
        if not self.is_ready():
            return
        yield self.send_scheduled()

    def load_data(self):
        program_settings = self.collections['program_settings'].find()
        for program_setting in program_settings:
            self.properties[program_setting['key']] = (
                program_setting['value'] if (program_setting['value'] is not None and program_setting['value'] != '') else self.properties[program_setting['key']])

    def is_ready(self):
        if not 'shortcode' in self.properties:
            self.log('Shortcode not defined')
            return False
        if ((not 'timezone' in self.properties)
                or (not self.properties['timezone'] in pytz.all_timezones)):
            self.log('Timezone not defined or not supported')
            return False
        return True

    def schedule(self):
        #Schedule the dialogues
        active_dialogues = self.get_active_dialogues()
        for dialogue in active_dialogues:
            participants = self.collections['participants'].find(
                {'enrolled.dialogue-id':dialogue['dialogue-id'],
                 'session-id': {'$ne': None}})
            self.schedule_participants_dialogue(
                participants, dialogue)
        #Schedule the nonattached messages
        self.schedule_participants_unattach_messages(
            self.collections['participants'].find({'session-id': {'$ne': None}}))

    def get_future_unattach_messages(self):
        return self.collections['unattached_messages'].find({
            'fixed-time': {
                '$gt': time_to_vusion_format(self.get_local_time())
            }})

    def schedule_participants_unattach_messages(self, participants):
        for participant in participants:
            self.schedule_participant_unattach_messages(participant)

    def schedule_participant_unattach_messages(self, participant):
        unattach_messages = self.get_future_unattach_messages()
        for unattach_message in unattach_messages:
            schedule = self.collections['schedules'].find_one({
                'participant-phone': participant['phone'],
                'unattach-id': unattach_message['_id']})
            history = self.collections['history'].find_one({
                'participant-phone': participant['phone'],
                'unattach-id': unattach_message['_id']})
            if history is not None:
                continue
            if schedule is None:
                schedule = {
                    '_id': None,
                    'participant-phone': participant['phone'],
                    'unattach-id': unattach_message['_id'],
                }
            self.save_schedule(schedule['participant-phone'],
                               unattach_message['fixed-time'],
                               'unattach-schedule',
                               unattach_id=schedule['unattach-id'],
                               _id=schedule['_id'])

    def schedule_participants_dialogue(self, participants, dialogue):
        for participant in participants:
            self.schedule_participant_dialogue(participant, dialogue)

    def get_enrollment_time(self, participant, dialogue):
        return ((enroll for enroll in participant['enrolled'] 
                 if enroll['dialogue-id'] == dialogue['dialogue-id']).next())

    #TODO: decide which id should be in an schedule object
    def schedule_participant_dialogue(self, participant, dialogue):
        try:
            for interaction in dialogue['interactions']:
                self.log("Scheduling %r" % interaction)
                schedule = self.collections['schedules'].find_one({
                    "participant-phone": participant['phone'],
                    "object-type": 'dialogue-schedule',
                    "dialogue-id": dialogue["dialogue-id"],
                    "interaction-id": interaction["interaction-id"]})
                history = self.collections['history'].find_one(
                    {"participant-phone": participant['phone'],
                     "participant-session-id": participant['session-id'],
                     "dialogue-id": dialogue["dialogue-id"],
                     "interaction-id": interaction["interaction-id"],
                     "$or": [{"message-direction": self.OUTGOING},
                             {"message-direction": self.INCOMING,
                              "matching-answer": {"$ne":None}}]},
                    sort=[("timestamp", pymongo.ASCENDING)])

                if history:
                    previousSendDateTime = time_from_vusion_format(history["timestamp"])
                    previousSendDay = previousSendDateTime.date()
                    continue

                if (interaction['type-schedule'] == 'offset-days'):
                    enrolled = self.get_enrollment_time(participant, dialogue)
                    sendingDateTime = get_offset_date_time(
                        time_from_vusion_format(enrolled['date-time']),
                        interaction['days'],
                        interaction['at-time'])
                elif (interaction['type-schedule'] == 'offset-time'):
                    enrolled = self.get_enrollment_time(participant, dialogue)
                    sendingDateTime = time_from_vusion_format(enrolled['date-time']) + timedelta(minutes=int(interaction['minutes']))
                elif (interaction['type-schedule'] == 'fixed-time'):
                    sendingDateTime = time_from_vusion_format(interaction['date-time'])
                elif (interaction['type-schedule'] == 'offset-condition'):
                    previous = self.collections['history'].find_one(
                        {"participant-phone": participant['phone'],
                         "participant-session-id": participant['session-id'],
                         "message-direction": self.INCOMING,
                         "dialogue-id": dialogue["dialogue-id"],
                         "interaction-id": interaction["offset-condition-interaction-id"],
                         "$or": [{'matching-answer': {'$exists': False}},
                                 {'matching-answer': {'$ne': None}}]})
                    # if the answer 
                    if  previous is None:
                        continue
                    sendingDateTime = self.get_local_time()

                #Scheduling a date already in the past is forbidden.
                if (sendingDateTime + timedelta(minutes=5) < self.get_local_time()):
                    self.save_history(
                        message_content=interaction['content'],
                        participant_phone=participant['phone'],
                        participant_session_id=participant['session-id'],
                        message_direction='outgoing',
                        message_status='Expired Interaction',
                        reference_metadata={
                            'dialogue-id': dialogue['dialogue-id'],
                            'interaction-id': interaction["interaction-id"]})
                    if (schedule):
                        self.collections['schedules'].remove(schedule['_id'])
                    continue

                if (not schedule):
                    schedule = {
                        "_id": None,
                        "participant-phone": participant['phone'],
                        "dialogue-id": dialogue['dialogue-id'],
                        "interaction-id": interaction["interaction-id"]}
                self.save_schedule(schedule['participant-phone'],
                                   self.to_vusion_format(sendingDateTime),
                                   'dialogue-schedule',
                                   _id=schedule['_id'],
                                   dialogue_id=schedule['dialogue-id'],
                                   interaction_id=schedule['interaction-id'])
                if 'set-reminder' in interaction:
                    self.schedule_participant_reminders(participant, dialogue, interaction, sendingDateTime)
        except:
            self.log("Scheduling dialogue exception: %s" % dialogue['dialogue-id'])
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error during schedule message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    def schedule_participant_reminders(self,participant,dialogue,interaction,initialSendDateTime):
        if not 'type-schedule-reminder' in interaction:
            return
        
        schedules = self.collections['schedules'].find({
            "participant-phone": participant['phone'],
            "$or":[{"object-type":'reminder-schedule'},
                   {"object-type": 'deadline-schedule'}],
            "dialogue-id": dialogue["dialogue-id"],
            "interaction-id": interaction["interaction-id"]})
        for reminder_schedule_to_be_deleted in schedules:
            self.collections['schedules'].remove(reminder_schedule_to_be_deleted['_id'])
        
        if (interaction['type-schedule-reminder'] == 'reminder-offset-days'):
            sendingDay = initialSendDateTime
            timeOfSending = interaction['reminder-at-time'].split(':', 1)
            sendingDateTime = datetime.combine(sendingDay, time(int(timeOfSending[0]), int(timeOfSending[1])))
        elif (interaction['type-schedule-reminder'] == 'reminder-offset-time'):
            sendingDateTime = initialSendDateTime
        for number in range(int(interaction['reminder-number'])+1):                
            if (interaction['type-schedule-reminder'] == 'reminder-offset-time'):
                sendingDateTime += timedelta(minutes=int(interaction['reminder-minutes']))
            elif (interaction['type-schedule-reminder'] == 'reminder-offset-days'):
                sendingDay += timedelta(days=int(interaction['reminder-days']))
                sendingDateTime = datetime.combine(sendingDay, time(int(timeOfSending[0]), int(timeOfSending[1])))
                                                                          
            schedule = {
                "_id": None,
                "participant-phone": participant['phone'],
                "dialogue-id": dialogue['dialogue-id'],
                "interaction-id": interaction["interaction-id"]}                                                                               
            self.save_schedule(schedule['participant-phone'],
                self.to_vusion_format(sendingDateTime),
                'reminder-schedule' if number < int(interaction['reminder-number']) else 'deadline-schedule',
                _id=schedule['_id'],
                dialogue_id=schedule['dialogue-id'],
                interaction_id=schedule['interaction-id'])
            

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
        if (schedule['object-type'] == 'dialogue-schedule' 
                or schedule['object-type'] == 'reminder-schedule'
                or schedule['object-type'] == 'deadline-schedule'):
            dialogue = self.get_current_dialogue(schedule['dialogue-id'])
            interaction = dialogue.get_interaction(schedule['interaction-id'])
            reference_metadata = {
                'dialogue-id': schedule['dialogue-id'],
                'interaction-id': schedule['interaction-id']}
        elif schedule['object-type'] == 'unattach-schedule':
            interaction = self.collections['unattached_messages'].find_one(
                {'_id': ObjectId(schedule['unattach-id'])})
            reference_metadata = {
                'unattach-id': schedule['unattach-id']}
        elif schedule['object-type'] == 'feedback-schedule':
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
    #TODO fire action scheduled by reminder if no reply is sent for any reminder
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

                # delayed action are always run even if there original interaction has been deleted
                if not interaction and toSend['object-type']!='action-schedule':
                    self.log("Sender Failure, schedule without interaction %r" % toSend)
                    continue
                
                if toSend['object-type'] == 'deadline-schedule':
                    actions = Actions()
                    if interaction.has_reminder():
                        for action in interaction['reminder-actions']:
                            actions.append(action_generator(**action))
                    for action in actions.items():
                        self.run_action(toSend['participant-phone'], action, reference_metadata)
                    continue
                elif toSend['object-type'] == 'action-schedule':
                    self.run_action(toSend['participant-phone'], 
                                    action_generator(**toSend['action']))
                    continue

                message_content = self.generate_message(interaction)
                message_content = self.customize_message(
                    toSend['participant-phone'],
                    message_content)

                if (time_from_vusion_format(toSend['date-time']) <
                        (local_time - timedelta(minutes=15))):
                    raise SendingDatePassed(
                        "Message should have been sent at %s" %
                        (toSend['date-time'],))
                
                message = TransportUserMessage(**{
                    'from_addr': self.properties['shortcode'],
                    'to_addr': toSend['participant-phone'],
                    'transport_name': self.transport_name,
                    'transport_type': self.transport_type,
                    'content': message_content})
                
                if ('customized-id' in self.properties 
                        and self.properties['customized-id'] is not None):
                    message['transport_metadata']['customized_id'] = self.properties['customized-id']
                
                yield self.transport_publisher.publish_message(message)
                self.log(
                    "Message has been sent to %s '%s'" % (message['to_addr'],
                                                          message['content']))
                self.save_history(
                    message_content=message['content'],
                    participant_phone=message['to_addr'],
                    participant_session_id=self.get_participant_session_id(message['to_addr']),
                    message_direction='outgoing',
                    message_status='pending',
                    message_id=message['message_id'],
                    reference_metadata=reference_metadata)

            except VusionError as e:
                self.save_history(
                    message_content='',
                    participant_phone=toSend['participant-phone'],
                    participant_session_id=self.get_participant_session_id(toSend['participant-phone']),
                    message_direction=None,
                    failure_reason=('%s' % (e,)),
                    reference_metadata=reference_metadata)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log(
                    "Error during consume user message: %r" %
                    traceback.format_exception(exc_type, exc_value, exc_traceback))

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
    #def get_interaction(self, dialogue, interaction_id):
        #for interaction in dialogue['interactions']:
            #if interaction['interaction-id'] == interaction_id:
                #return interaction
                    
    def log(self, msg, level='msg'):
        timezone = None
        local_time = self.get_local_time()
        if self.r_prefix is None or self.control_name is None:
            return
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
            keywords += dialogue.get_all_keywords()
        for request in self.collections['requests'].find():
            keyphrases = request['keyword'].split(', ')
            for keyphrase in keyphrases:
                if not (keyphrase.split(' ')[0]) in keywords:
                    keywords.append(keyphrase.split(' ')[0])
        to_addr = get_shortcode_value(self.properties['shortcode'])
        rules = []
        self.log("Registering the keywords: %r" % keywords)
        for keyword in keywords:
            rules.append({'app': self.transport_name,
                          'keyword': keyword,
                          'to_addr': ("%s" % to_addr)})
        if (not self.properties['international-prefix'] == 'all'):
            for rule in rules:
                rule['prefix'] = ("+%s" % self.properties['international-prefix'])
       
        msg = DispatcherControl(
            action='add_exposed',
            exposed_name=self.transport_name,
            rules=rules)
        yield self.dispatcher_publisher.publish_message(msg)

    @inlineCallbacks
    def unregister_from_dispatcher(self):
        msg = DispatcherControl(action='remove_exposed',
                                exposed_name=self.transport_name)
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
        if (interaction['type-interaction'] == 'question-answer'
                and interaction['set-use-template'] is not None):
            default_template = None
            if (interaction['type-question'] == 'closed-question'):
                default_template = self.properties['default-template-closed-question']
            elif (interaction['type-question'] == 'open-question'):
                default_template = self.properties['default-template-open-question']
            else:
                pass
            if (default_template is None):
                raise MissingTemplate(
                    "Cannot find default template for %s" %
                    (interaction['type-question'],))
            template = self.collections['templates'].find_one({"_id": ObjectId(default_template)})
            if (template is None):
                raise MissingTemplate(
                    "Cannot find specified template id %s" %
                    (default_template,))
            #replace question
            message = re.sub(regex_QUESTION, interaction['content'], template['template'])
            #replace answers
            if (interaction['type-question'] == 'closed-question'):
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
            message = re.sub(regex_SHORTCODE,
                             get_shortcode_value(self.properties['shortcode']),
                             message)
            if (interaction['type-question'] == 'open-question'):
                message = re.sub(regex_ANSWER,
                                 interaction['answer-label'],
                                 message)
            message = re.sub(regex_Breakline, '\n', message)
        return message

    def get_participant_label_value(self, participant, label):
        label_indexer = dict((p['label'], p['value']) for i, p in enumerate(participant['profile']))
        return label_indexer.get(label, None)

    def customize_message(self, participant_phone, message):
        tags_regexp = re.compile(r'\[(?P<table>\w*)\.(?P<attribute>\w*)\]')
        tags = re.findall(tags_regexp, message)
        for table, attribute in tags:
            participant = self.collections['participants'].find_one(
                {'phone': participant_phone})
            attribute = attribute.lower()
            participant_label_value = self.get_participant_label_value(participant, attribute)
            if not participant_label_value:
                raise MissingData("%s has no attribute %s" %
                                  (participant_phone, attribute))
            message = message.replace('[%s.%s]' %
                                      (table, attribute),
                                      participant_label_value)
        return message
