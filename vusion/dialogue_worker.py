# -*- test-case-name: tests.test_ttc -*-

import sys
import traceback
import re

from uuid import uuid4

from twisted.internet.defer import (inlineCallbacks, Deferred, returnValue)
from twisted.enterprise import adbapi
from twisted.internet import task, reactor

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

from vusion.persist import (Dialogue, FeedbackSchedule, UnattachSchedule,
                            schedule_generator, Participant)
from vusion.utils import (time_to_vusion_format, get_local_time,
                          get_local_time_as_timestamp, time_from_vusion_format,
                          get_shortcode_value, get_offset_date_time,
                          split_keywords)
from vusion.error import (MissingData, SendingDatePassed, VusionError,
                          MissingTemplate)
from vusion.message import DispatcherControl, WorkerControl
from vusion.action import (Actions, action_generator,FeedbackAction,
                           EnrollingAction, OptinAction, OptoutAction,
                           RemoveRemindersAction)
from vusion.context import Context
from vusion.persist import Request, history_generator, schedule_generator


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
            'double-matching-answer-feedback': None,
            'double-optin-error-feedback': None}
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
            message_class=WorkerControl)
        self._consumers.append(self.control_consumer)

        self.sender = reactor.callLater(2, self.daemon_process)
      
    @inlineCallbacks
    def teardown_application(self):
        self.log("Worker is stopped.")
        if self.is_ready():
            yield self.unregister_from_dispatcher()
        if (self.sender.active()):
            self.sender.cancel()

    def save_schedule(self, **kwargs):
        if 'date-time' in kwargs:
            kwargs['date-time'] = time_to_vusion_format(kwargs['date-time'])
        schedule = schedule_generator(**kwargs)
        self.collections['schedules'].save(schedule.get_as_dict())

    def save_history(self, **kwargs):
        if 'timestamp' in kwargs:
            kwargs['timestamp'] = time_to_vusion_format(kwargs['timestamp'])
        else:
            kwargs['timestamp'] = time_to_vusion_format(self.get_local_time())
        if 'interaction' in kwargs:
            kwargs.pop('interaction')
        history = history_generator(**kwargs)
        self.collections['history'].save(history.get_as_dict())

    def get_participant(self, participant_phone, only_optin=False):
        try:
            query = {'phone':participant_phone}
            if only_optin:
                query.update({'session-id':{'$ne': None}})
            return Participant(**self.collections['participants'].find_one(query))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error while retriving participant %s  %r" %
                (participant_phone,
                 traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return None

    def get_participants(self, query):
        participants = []
        for participant in self.collections['participants'].find(query):
            try:
                participants.append(Participant(**participant))
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log(
                    "Error while retriving participant %r" %
                     traceback.format_exception(exc_type, exc_value, exc_traceback))
        return participants

    def get_participant_session_id(self, participant_phone):
        participant = self.get_participant(participant_phone)
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
        self.setup_collections({
            'dialogues': 'dialogue-id',
            'participants': 'phone',
            'history': 'timestamp',
            'schedules': 'date-time',
            'program_settings': None,
            'unattached_messages': None,
            'requests': None})

        self.db = connection[self.vusion_database_name]
        self.setup_collections({'templates': None})

    def setup_collections(self, names):
        for name, index in names.items():
            self.setup_collection(name, index)

    def setup_collection(self, name, index):
        if name in self.db.collection_names():
            self.collections[name] = self.db[name]
        else:
            self.collections[name] = self.db.create_collection(name)
        if index is not None:
            self.collections[name].ensure_index(index, background=True)
        self.log("Collection initialised: %s" % name)

    def consume_control(self, message):
        try:
            self.log("Control message received to %r" % (message,))
            self.load_settings()
            if (not self.is_ready()):
                self.log("Worker is not ready, cannot performe the action.")
                return
            if message['action'] == 'update_schedule':
                if message['schedule_type'] == 'dialogue':
                    self.schedule_dialogue(message['object_id'])
                    self.register_keywords_in_dispatcher()
                elif message['schedule_type'] == 'unattach':
                    self.schedule_unattach(message['object_id'])
                elif message['schedule_type'] == 'participant':
                    self.schedule_participant(message['object_id'])
            elif message['action'] == 'test_send_all_messages':
                dialogue = self.get_dialogue_obj(message['dialogue_obj_id'])
                self.send_all_messages(dialogue, message['phone_number'])
            elif message['action'] == 'update_registered_keywords':
                self.register_keywords_in_dispatcher()
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

    def get_matching_request_actions(self, content, actions, context):
        # exact matching
        exact_regex = re.compile(('(,\s|^)%s($|,)' % content), re.IGNORECASE)
        matching_request = self.collections['requests'].find_one(
            {'keyword': {'$regex': exact_regex}})
        if matching_request:
            request = Request(**matching_request)
            request.append_actions(actions)
            context.update({'request-id': matching_request['_id']})
            return
        # lazy keyword matching
        lazy_regex = re.compile(
            ('(,\s|^)%s(\s.*|$|,)' % get_first_word(content)), re.IGNORECASE)
        matching_request = self.collections['requests'].find_one(
            {'keyword': {'$regex': lazy_regex},
             'set-no-request-matching-try-keyword-only': 'no-request-matching-try-keyword-only'})
        if matching_request:
            request = Request(**matching_request)
            request.append_actions(actions)
            context.update({'request-id': matching_request['_id']})

    def create_participant(self, participant_phone):
        return Participant(**{
            'model-version': '2',
            'phone': participant_phone,
            'session-id': uuid4().get_hex(), 
            'last-optin-date': time_to_vusion_format(self.get_local_time()),
            'tags': [],
            'enrolled':[],
            'profile':[]}).get_as_dict()

    def run_action(self, participant_phone, action, context=Context(),
                   participant_session_id=None):
        regex_ANSWER = re.compile('ANSWER')
        self.log(("Run action for %s %s" % (participant_phone, action,)))
        if (action.get_type() == 'optin'):
            participant = self.get_participant(participant_phone)
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
                self.collections['participants'].save(
                    self.create_participant(participant_phone),
                    safe=True)
            for dialogue in self.get_active_dialogues({'auto-enrollment':'all'}):
                self.run_action(participant_phone,
                                EnrollingAction(**{'enroll': dialogue['dialogue-id']}))
            self.schedule_participant(participant_phone)
        elif (action.get_type() == 'optout'):
            self.collections['participants'].update(
                {'phone': participant_phone},
                {'$set': {'session-id': None,
                          'last-optin-date': None}})
            self.collections['schedules'].remove({
                'participant-phone': participant_phone,
                'object-type': {'$ne': 'feedback-schedule'}})
        elif (action.get_type() == 'feedback'):
            schedule = FeedbackSchedule(**{
                'model-version': '2',
                'participant-phone': participant_phone,
                'participant-session-id': participant_session_id,
                'date-time': time_to_vusion_format(self.get_local_time()),
                'content': action['content'],
                'context': context.get_dict_for_history()})
            self.send_schedule(schedule)
        elif (action.get_type() == 'unmatching-answer'):
            setting = self.collections['program_settings'].find_one({
                'key': 'default-template-unmatching-answer'})
            if setting is None:
                return
            template = self.collections['templates'].find_one({
                '_id': ObjectId(setting['value'])})
            if template is None:
                return
            error_message = re.sub(regex_ANSWER,
                                   action['answer'],
                                   template['template'])
            schedule = FeedbackSchedule(**{
                'model-version': '2',
                'participant-phone': participant_phone,
                'participant-session-id': participant_session_id,
                'date-time': time_to_vusion_format(self.get_local_time()),
                'content': error_message,
                'context': context.get_dict_for_history()})
            self.send_schedule(schedule)
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
            participant = self.get_participant(participant_phone)
            self.schedule_participant_dialogue(participant, dialogue)
        elif (action.get_type() == 'delayed-enrolling'):
            schedule_time = get_offset_date_time(
                self.get_local_time(), 
                action['offset-days']['days'],
                action['offset-days']['at-time'])
            schedule = {
                'object-type': 'action-schedule',
                'model-version': '2',
                'participant-phone': participant_phone,
                'participant-session-id': participant_session_id,
                'date-time': schedule_time,
                'action': EnrollingAction(**{'enroll': action['enroll']}).get_as_dict(),
                'context': context.get_dict_for_history()}
            self.save_schedule(**schedule)
        elif (action.get_type() == 'profiling'):
            self.collections['participants'].update(
                {'phone': participant_phone,
                 'session-id': {'$ne': None}},
                {'$push': {'profile': {'label': action['label'],
                                        'value': action['value'],
                                        'raw': context['message']}}})
        elif (action.get_type() == 'offset-conditioning'):
            participant = self.get_participant(participant_phone, True)
            if participant is None:
                return
            self.schedule_participant_dialogue(
                participant,
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
            history = {'object-type': 'unmatching-history'}
            context = Context(**{'message': message['content']})
            actions = Actions()
            self.get_matching_request_actions(message['content'], actions, context)
            if context.is_matching():
                history = {'object-type': 'request-history'}
            else:
                active_dialogues = self.get_active_dialogues()
                for dialogue in active_dialogues:
                    dialogue.get_matching_reference_and_actions(
                        message['content'], actions, context)
                    if context.is_matching():
                        history = {'object-type': 'dialogue-history'}
                        break
            # High priority to run an optin or enrolling action to get sessionId 
            if (self.get_participant_session_id(message['from_addr']) is None 
                    and (actions.contains('optin') or actions.contains('enrolling'))):
                self.run_action(message['from_addr'], actions.get_priority_action())
            participant = self.get_participant(message['from_addr'])
            history.update({
                'message-content': message['content'],
                'participant-phone': message['from_addr'],
                'participant-session-id': (participant['session-id'] if participant else None),
                'message-direction': 'incoming'})
            history.update(context.get_dict_for_history())
            self.save_history(**history)
            if (context.is_matching() and participant is not None):
                if ('interaction' in context):
                    if self.has_oneway_marker(participant['phone'], participant['session-id'], context):
                        actions.clear_all()
                    else:
                        self.get_program_dialogue_actions(participant, context, actions)
                        if self.participant_has_max_unmatching_answers(participant, context['dialogue-id'], context['interaction']):
                            self.add_oneway_marker(participant['phone'], participant['session-id'], context)
                            context['interaction'].get_max_unmatching_action(context['dialogue-id'], actions)
                elif ('request-id' in context):
                    self.get_program_dialogue_actions(participant, context, actions)                    
                self.run_actions(participant, context, actions)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    def run_actions(self, participant, context, actions):
        if ((not 'request-id' in context)
            and (participant['session-id'] is None
                 or not participant.is_enrolled(context['dialogue-id']))):
            return
        for action in actions.items():
            self.run_action(participant['phone'],
                            action,
                            context,
                            participant['session-id'])

    def get_program_dialogue_actions(self, participant, context, actions):
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
        if (actions.contains('optin')
            and participant['session-id'] is not None):
            actions.clear_all()
            if self.properties['double-optin-error-feedback'] is not None:
                actions.append(FeedbackAction(**{
                    'content': self.properties['double-optin-error-feedback']}))

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
        if history.count() == int(interaction['max-unmatching-answer-number']):
            return True
        return False
    
    def has_oneway_marker(self, participant_phone, participant_session_id,
                          context):
        return self.collections['history'].find_one({
            'object-type': 'oneway-marker-history',
            'participant-phone': participant_phone,
            'participant-session-id':participant_session_id,
            'dialogue-id': context['dialogue-id'],
            'interaction-id': context['interaction-id']}) is not None
    
    def add_oneway_marker(self, participant_phone, participant_session_id,
                          context):
        history = self.collections['history'].find_one({
            'object-type': 'oneway-marker-history',
            'participant-phone': participant_phone,
            'participant-session-id':participant_session_id,
            'dialogue-id': context['dialogue-id'],
            'interaction-id': context['interaction-id']})
        if history is None:
            history = {
                'object-type': 'oneway-marker-history',
                'timestamp': self.get_local_time(),
                'participant-phone': participant_phone,
                'participant-session-id':participant_session_id,
                'dialogue-id': context['dialogue-id'],
                'interaction-id': context['interaction-id']}
            self.save_history(**history)
    
    def get_max_unmatching_answers_interaction(self, dialogue_id, interaction_id):
        dialogue = self.get_current_dialogue(dialogue_id)
        returned_interaction = dialogue.get_interaction(interaction_id)
        if returned_interaction.has_max_unmatching_answers():
            return returned_interaction
        return None

    def daemon_process(self):
        self.load_settings()
        if not self.is_ready():
            return
        self.send_scheduled()
        next_iteration = self.get_time_next_daemon_iteration()
        self.sender = reactor.callLater(
            next_iteration,
            self.daemon_process)

    def get_time_next_daemon_iteration(self):
        try:
            schedule = schedule_generator(**self.collections['schedules'].find(
                sort=[('date-time', 1)],
                limit=1)[0])
            schedule_time = schedule.get_schedule_time()
            delta = schedule_time - self.get_local_time()
            if delta < timedelta():
                return 1
            if delta < timedelta(seconds=60):
                return delta.total_seconds() + 1
            else:
                return 60
        except:
            return 60

    def update_time_next_daemon_iteration(self):
        secondsLater = self.get_time_next_daemon_iteration()
        if secondsLater != 60:
            self.log("reschedule daemon in %s" % secondsLater)
            self.sender.reset(secondsLater)

    def load_data(self):
        program_settings = self.collections['program_settings'].find()
        for program_setting in program_settings:
            self.properties[program_setting['key']] = program_setting['value']

    def load_settings(self):
        previous_shortcode = self.properties['shortcode']
        self.load_data()
        if previous_shortcode != self.properties['shortcode']:
            self.register_keywords_in_dispatcher()


    def is_ready(self):
        if not 'shortcode' in self.properties:
            self.log('Shortcode not defined')
            return False
        if ((not 'timezone' in self.properties)
                or (not self.properties['timezone'] in pytz.all_timezones)):
            self.log('Timezone not defined or not supported')
            return False
        return True

    def schedule_participant(self, participant_phone):
        participant = self.get_participant(participant_phone, True)
        if participant is None:
            return
        for enrolled in participant['enrolled']:
            dialogue = self.get_current_dialogue(enrolled['dialogue-id'])
            if dialogue is not None:
                self.schedule_participant_dialogue(participant, dialogue)
        future_unattachs = self.get_future_unattachs()
        for unattach in future_unattachs:
            self.schedule_participant_unattach(participant, unattach)

    def get_future_unattachs(self):
        return self.collections['unattached_messages'].find({
            'fixed-time': {
                '$gt': time_to_vusion_format(self.get_local_time())}})

    def schedule_dialogue(self, dialogue_id):
        dialogue = self.get_current_dialogue(dialogue_id)
        participants = self.get_participants(
            {'enrolled.dialogue-id': dialogue_id,
             'session-id': {'$ne': None}})
        self.schedule_participants_dialogue(participants, dialogue)

    def schedule_unattach(self, unattach_id):
        unattach = self.collections['unattached_messages'].find_one({
            '_id': ObjectId(unattach_id)})
        participants = self.get_participants({'session-id': {'$ne': None}})
        self.schedule_participants_unattach(participants, unattach)

    def schedule_participants_unattach(self, participants, unattach):
        for participant in participants:
            self.schedule_participant_unattach(participant, unattach)

    def schedule_participant_unattach(self, participant, unattach):
        history = self.collections['history'].find_one({
            'participant-phone': participant['phone'],
            'unattach-id': str(unattach['_id'])})
        if history is not None:
            return
        schedule = self.collections['schedules'].find_one({
            'participant-phone': participant['phone'],
            'unattach-id': str(unattach['_id'])})
        if schedule is None:
            schedule = UnattachSchedule(**{
                    'model-version': '2',
                    'participant-phone': participant['phone'],
                    'participant-session-id': participant['session-id'],
                    'unattach-id': str(unattach['_id']),
                    'date-time': unattach['fixed-time']})
        else:
            schedule = schedule_generator(**schedule)
            schedule['date-time'] = unattach['fixed-time']
        self.collections['schedules'].save(schedule.get_as_dict())
        self.update_time_next_daemon_iteration()

    def schedule_participants_dialogue(self, participants, dialogue):
        for participant in participants:
            self.schedule_participant_dialogue(participant, dialogue)

    def get_enrollment_time(self, participant, dialogue):
        return ((enroll for enroll in participant['enrolled'] 
                 if enroll['dialogue-id'] == dialogue['dialogue-id']).next())

    #TODO: decide which id should be in an schedule object
    def schedule_participant_dialogue(self, participant, dialogue):
        try:
            for interaction in dialogue.interactions:
                self.log("Scheduling %s interaction %s for %s" % 
                         (dialogue['name'], interaction['content'], participant['phone'],))
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
                    sendingDateTime = time_from_vusion_format(enrolled['date-time']) + interaction.get_offset_time_delta()
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

                schedule = self.collections['schedules'].find_one({
                    "participant-phone": participant['phone'],
                    "object-type": 'dialogue-schedule',
                    "dialogue-id": dialogue["dialogue-id"],
                    "interaction-id": interaction["interaction-id"]})        
                
                #Scheduling a date already in the past is forbidden.
                if (sendingDateTime + timedelta(minutes=5) < self.get_local_time()):
                    history = {
                        'object-type': 'datepassed-marker-history',
                        'participant-phone': participant['phone'],
                        'participant-session-id': participant['session-id'],
                        'dialogue-id': dialogue['dialogue-id'],
                        'interaction-id': interaction['interaction-id'],
                        'scheduled-date-time': time_to_vusion_format(sendingDateTime)}
                    self.save_history(**history)
                    if (schedule):
                        self.collections['schedules'].remove(schedule['_id'])
                    continue

                if (not schedule):
                    schedule = {
                        'object-type': 'dialogue-schedule', 
                        'model-version': '2',
                        'participant-phone': participant['phone'],
                        'participant-session-id': participant['session-id'],
                        'dialogue-id': dialogue['dialogue-id'],
                        'interaction-id': interaction["interaction-id"]}
                schedule.update(
                    {'date-time': sendingDateTime})
                self.save_schedule(**schedule)
                if interaction.has_reminder():
                    self.schedule_participant_reminders(participant, dialogue, interaction, sendingDateTime)
            self.update_time_next_daemon_iteration()
        except:
            self.log("Scheduling dialogue exception: %s" % dialogue['dialogue-id'])
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error during schedule message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    def schedule_participant_reminders(self,participant,dialogue,interaction,initialSendDateTime):        
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
                'object-type': 'reminder-schedule' if number < int(interaction['reminder-number']) else 'deadline-schedule',
                'model-version': '2',
                'participant-phone': participant['phone'],
                'participant-session-id': participant['session-id'],
                'date-time': sendingDateTime,
                'dialogue-id': dialogue['dialogue-id'],
                'interaction-id': interaction['interaction-id']}                                                                               
            self.save_schedule(**schedule)
            
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
        if schedule.get_type() in ['dialogue-schedule', 'reminder-schedule', 'deadline-schedule']:
            dialogue = self.get_current_dialogue(schedule['dialogue-id'])
            interaction = dialogue.get_interaction(schedule['interaction-id'])
            context = Context(**{
                'dialogue-id': schedule['dialogue-id'],
                'interaction-id': schedule['interaction-id']})
        elif schedule.get_type() == 'unattach-schedule':
            interaction = self.collections['unattached_messages'].find_one(
                {'_id': ObjectId(schedule['unattach-id'])})
            context = Context(**{
                'unattach-id': schedule['unattach-id']})
        elif schedule.get_type() == 'feedback-schedule':
            interaction = {
                'content': schedule['content'],
                'type-interaction': 'feedback'}
            context = Context()
        else:
            self.log("Error schedule object not supported: %s"
                     % (schedule))
            return None, Context()
        return interaction, context

    #TODO: fire error feedback if the ddialogue do not exit anymore
    #TODO fire action scheduled by reminder if no reply is sent for any reminder
    @inlineCallbacks
    def send_scheduled(self):
        try:
            self.log('Checking the schedule list...')
            local_time = self.get_local_time()
            due_schedules = self.collections['schedules'].find(
                spec={'date-time': {'$lt': time_to_vusion_format(local_time)}},
                sort=[('date-time', 1)])
            for due_schedule in due_schedules:
                self.collections['schedules'].remove(
                    {'_id': due_schedule['_id']})
                message_content = None
                yield self.send_schedule(schedule_generator(**due_schedule))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log("Error send_scheduled: %r" %
                     traceback.format_exception(exc_type, exc_value, exc_traceback))     

    @inlineCallbacks
    def send_schedule(self, schedule):
        try:            
            local_time = self.get_local_time()

            if schedule.get_type() == 'action-schedule':
                if schedule.is_expired(local_time):
                    action = action_generator(**schedule['action'])
                    history = {
                        'object-type': 'datepassed-action-marker-history',
                        'participant-phone': schedule['participant-phone'],
                        'participant-session-id': schedule['participant-session-id'],
                        'action-type': action.get_type(),
                        'scheduled-date-time': schedule['date-time']}
                    self.save_history(**history)
                    return
                self.run_action(
                    schedule['participant-phone'], 
                    action_generator(**schedule['action']),
                    schedule.get_context(),
                    schedule['participant-session-id'])
                return
            
            local_time = self.get_local_time()
            message_content = None
            #participant = self.collections['participants'].find_one({'phone': schedule['participant-phone']})
            interaction, message_ref = self.from_schedule_to_message(schedule)
            
            # delayed action are always run even if there original interaction has been deleted
            if not interaction:
                self.log("Sender failure, cannot build process %r" % schedule)
                return
                
            if schedule.get_type() == 'deadline-schedule':
                actions = Actions()
                if interaction.has_reminder():
                    for action in interaction['reminder-actions']:
                        actions.append(action_generator(**action))
                    self.add_oneway_marker(
                        schedule['participant-phone'],
                        schedule['participant-session-id'],
                        message_ref.get_dict_for_history())
                for action in actions.items():
                    self.run_action(
                        schedule['participant-phone'],
                        action,
                        message_ref,
                        schedule['participant-session-id'])
                return

            message_content = self.generate_message(interaction)
            message_content = self.customize_message(
                schedule['participant-phone'],
                message_content)

            if schedule.is_expired(local_time):
                history = {
                    'object-type': 'datepassed-marker-history',
                    'participant-phone': schedule['participant-phone'],
                    'participant-session-id': schedule['participant-session-id'],
                    'scheduled-date-time': schedule['date-time']}
                history.update(message_ref.get_dict_for_history())
                self.save_history(**history)
                return
                
            message = TransportUserMessage(**{
                'from_addr': self.properties['shortcode'],
                'to_addr': schedule['participant-phone'],
                'transport_name': self.transport_name,
                'transport_type': self.transport_type,
                'content': message_content})
                
            if ('customized-id' in self.properties 
                    and self.properties['customized-id'] is not None):
                message['transport_metadata']['customized_id'] = self.properties['customized-id']
                
            yield self.transport_publisher.publish_message(message)
            self.log("Message has been sent to %s '%s'" % 
                     (message['to_addr'], message['content']))

            if schedule.get_type() in ['dialogue-schedule', 'reminder-schedule']:
                object_type = 'dialogue-history'
            elif schedule.get_type() == 'unattach-schedule':
                object_type = 'unattach-history'
            elif schedule.get_type() == 'feedback-schedule':
                message_ref = schedule.get_context()
                if 'dialogue-id' in message_ref:
                    object_type = 'dialogue-history'
                elif 'request-id' in message_ref:
                    object_type = 'request-history'
            else:
                raise VusionError("%s is not supported" % schedule.get_type())

            history = {
                'object-type': object_type,
                'message-content': message['content'],
                'participant-phone': message['to_addr'],
                'participant-session-id': schedule['participant-session-id'],
                'message-direction': 'outgoing',
                'message-status': 'pending',
                'message-id': message['message_id']}
            history.update(message_ref.get_dict_for_history())
            self.save_history(**history)
            return
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log("Error send schedule: %r" %
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
                    keywords.append(keyphrase.split(' ')[0].lower())
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
        if ('type-interaction' in interaction
                and interaction['type-interaction'] == 'question-answer'
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
        tags_regexp = re.compile(r'\[(?P<table>\w*)\.(?P<attribute>[\s\w]*)\]')
        tags = re.findall(tags_regexp, message)
        for table, attribute in tags:
            participant = self.get_participant(participant_phone)
            #attribute = attribute.lower()
            participant_label_value = participant.get_participant_label_value(attribute)
            if not participant_label_value:
                raise MissingData("%s has no attribute %s" %
                                  (participant_phone, attribute))
            message = message.replace('[%s.%s]' %
                                      (table, attribute),
                                      participant_label_value)
        return message
