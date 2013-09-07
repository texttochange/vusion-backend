# -*- test-case-name: tests.test_ttc -*-
import sys
import traceback
import re

from uuid import uuid4

from twisted.internet.defer import inlineCallbacks, Deferred, returnValue
from twisted.internet import task, reactor

import pymongo
from bson.objectid import ObjectId

from redis import Redis

from datetime import datetime, time, date, timedelta
import pytz

from vumi.application import ApplicationWorker
from vumi.message import Message, TransportUserMessage, TransportEvent
from vumi.application import SessionManager
from vumi import log
from vumi.utils import get_first_word
from vumi.errors import VumiError

from vusion.persist import (Dialogue, FeedbackSchedule, UnattachSchedule,
                            schedule_generator, Participant, UnattachMessage)
from vusion.utils import (time_to_vusion_format, get_local_time,
                          get_local_time_as_timestamp, time_from_vusion_format,
                          get_shortcode_value, get_offset_date_time,
                          split_keywords)
from vusion.error import (MissingData, SendingDatePassed, VusionError,
                          MissingTemplate, MissingProperty)
from vusion.message import DispatcherControl, WorkerControl
from vusion.persist.action import (Actions, action_generator,FeedbackAction,
                                   EnrollingAction, OptinAction, OptoutAction,
                                   RemoveRemindersAction)
from vusion.context import Context
from vusion.persist import Request, history_generator, schedule_generator, ContentVariable
from vusion.component import DialogueWorkerPropertyHelper, CreditManager

from vusion.persist import (Request, history_generator, schedule_generator, 
                            HistoryManager)
from vusion.component import (DialogueWorkerPropertyHelper, CreditManager,
                              LogManager)
	

class DialogueWorker(ApplicationWorker):
    
    INCOMING = "incoming"
    OUTGOING = "outgoing"
    
    def __init__(self, *args, **kwargs):
        super(DialogueWorker, self).__init__(*args, **kwargs)

    def startService(self):
        self._d = Deferred()
        self._consumers = []
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
        #Store basic configuration data
        self.transport_name = self.config['transport_name']
        self.control_name = self.config['control_name']
        self.transport_type = 'sms'
        self.r_config = self.config.get('redis', {})
        self.r_prefix = "%(control_name)s:" % self.config

        #Initializing
        self.program_name = None
        self.last_script_used = None
        self.r_key = 'vusion:programs:' + self.config['database_name']
        self.r_server = Redis(**self.r_config)
        self._d.callback(None)
	
	# Component / Manager initialization
	self.log_manager = LogManager(
	    self.config['database_name'], 
	    self.r_key,
	    self.r_server)
	
        self.collections = {}
        self.init_program_db(
	    self.config['database_name'],
	    self.config['vusion_database_name'])	
	
	self.properties = DialogueWorkerPropertyHelper(
	    self.collections['program_settings'],
	    self.collections['shortcodes'])	

	self.log_manager.startup(self.properties)
	self.collections['history'].set_property_helper(self.properties)

	self.credit_manager = CreditManager(
	    self.r_key, self.r_server, 
	    self.collections['history'], 
	    self.collections['schedules'],
	    self.properties, 
	    self.log_manager)

	self.log_manager.log("Dialogue Worker is starting")
        #Set up dispatcher publisher
        self.dispatcher_publisher = yield self.publish_to(
            '%(dispatcher_name)s.control' % self.config)

        #Will need to register the keywords
        self.load_properties(if_needed_register_keywords=True)

        #Set up control consumer
        self.control_consumer = yield self.consume(
            '%(control_name)s.control' % self.config,
            self.consume_control,
            message_class=Message)
        self._consumers.append(self.control_consumer)

        self.sender = reactor.callLater(2, self.daemon_process)

    @inlineCallbacks
    def teardown_application(self):
        self.log("Worker is stopped.")
	self.log_manager.stop()
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
	return self.collections['history'].save_history(**kwargs)

    def get_participant(self, participant_phone, only_optin=False):
        try:
            query = {'phone':participant_phone}
            if only_optin:
                query.update({'session-id':{'$ne': None}})
            return Participant(**self.collections['participants'].find_one(query))
        except TypeError:
            self.log("Participant %s is either not optin or not in collection." % participant_phone)
            return None
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
        participant = self.get_participant(participant_phone, only_optin=True)
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

    def get_requests(self):
        requests = []
        for request in self.collections['requests'].find():
            try:
                requests.append(Request(**request))
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log(
                    "Error while applying request model %s: %r" %
                    (request['keyword'],
                        traceback.format_exception(exc_type, exc_value, exc_traceback)))
        return requests
  
    def init_program_db(self, database_name, vusion_database_name):
        self.log("Initialization of the program")
        self.database_name = database_name
        self.vusion_database_name = vusion_database_name
        self.log("Connecting to database: %s" % self.database_name)

        #Initilization of the database
        connection = pymongo.Connection(self.config['mongodb_host'],
                                        self.config['mongodb_port'],
                                        safe=self.config.get('mongodb_safe', False))
        self.db = connection[self.database_name]
        self.setup_collections({
            'dialogues': 'dialogue-id',
            'participants': 'phone',
            'history': 'timestamp',
            'schedules': 'date-time',
            'program_settings': None,
            'unattached_messages': None,
            'requests': None,
            'content_variables': None})

        self.collections['schedules'].ensure_index([('participant-phone',1),
                                                    ('interaction-id', 1)])
        self.db = connection[self.vusion_database_name]
        self.setup_collections({'templates': None})
        self.setup_collections({'shortcodes': 'shortcode'})

    def setup_collections(self, names):
        for name, index in names.items():
            self.setup_collection(name, index)

    def setup_collection(self, name, index):
	if name == 'history':
	    self.collections[name] = HistoryManager(self.db, name)
	    return
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
            message = WorkerControl(**message.payload)
            if message['action'] == 'reload_program_settings':
                self.load_properties()
                return
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
        except (VusionError, VumiError) as e:
            self.log('ERROR: %s(%s)' % (e.__class__.__name__, e.message), level='error')
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "UNKNOWN ERROR during consume control message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    def dispatch_event(self, message):
        self.log("Event message received %s" % (message,))
	if (message['event_type'] == 'ack'):
	    status = 'ack'
	elif (message['event_type'] == 'delivery_report'):
            status = message['delivery_status']
            if (message['delivery_status'] == 'failed'):
		status = {
		    'status': message['delivery_status'],
		    'reason': ("Level:%s Code:%s Message:%s" % (
		        message.get('failure_level', 'unknown'),
		        message.get('failure_code', 'unknown'),
		        message.get('failure_reason', 'unknown')))}
	if ('transport_type' in message['transport_metadata'] 
	    and message['transport_metadata']['transport_type'] == 'http_forward'):
	    self.collections['history'].update_forwarded_status(message['user_message_id'], status)
	    return
	self.collections['history'].update_status(message['user_message_id'], status)

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
            'model-version': '3',
            'phone': participant_phone,
            'session-id': uuid4().get_hex(), 
            'last-optin-date': time_to_vusion_format(self.get_local_time()),
            'last-optout-date': None,
            'tags': [],
            'enrolled':[],
            'profile':[]}).get_as_dict()

    def update_participant_transport_metadata(self, message):
        if message['transport_metadata'] is not {}:
            self.collections['participants'].update(
                {'phone': message['from_addr']},
                {'$set': {'transport_metadata': message['transport_metadata']}})

    def run_action(self, participant_phone, action, context=Context(),
                   participant_session_id=None):
        if action.has_condition():
            query = action.get_condition_mongodb_for(participant_phone, participant_session_id)
            if self.collections['participants'].find(query).limit(1).count()==0:
                self.log(("Participant %s doesn't satify the condition for action for %s" % (participant_phone, action,)))
                return
        self.log(("Run action for %s %s" % (participant_phone, action,)))
        if (action.get_type() == 'optin'):
            participant = self.get_participant(participant_phone)
            if not participant:                            
                ## This is the first optin
                self.collections['participants'].save(
                    self.create_participant(participant_phone),
                    safe=True)
            else:
            ## This is a second optin
                if (participant['session-id'] != None):    
                     ## Participant still optin
                     if self.properties['double-optin-error-feedback'] is not None:
                         self.run_action(
                             participant_phone, 
                             FeedbackAction(**{'content': self.properties['double-optin-error-feedback']}),
                             context,
                             participant_session_id)
                         return
                else:
                    ## Participant is re optin
                    self.collections['participants'].update(
                        {'phone': participant_phone},
                        {'$set': {'session-id': uuid4().get_hex(), 
                                  'last-optin-date': time_to_vusion_format(self.get_local_time()),
                                  'last-optout-date': None,
                                  'tags': [],
                                  'enrolled': [],
                                  'profile': [] }},
                        safe=True)
            ## finialise the optin by enrolling and schedulling
            for dialogue in self.get_active_dialogues({'auto-enrollment':'all'}):
                self.run_action(participant_phone,
                                EnrollingAction(**{'enroll': dialogue['dialogue-id']}))
            self.schedule_participant(participant_phone)
        elif (action.get_type() == 'optout'):
            self.collections['participants'].update(
                {'phone': participant_phone},
                {'$set': {'session-id': None,
                          'last-optout-date': time_to_vusion_format(self.get_local_time())}},
                safe=True)
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
            regex_ANSWER = re.compile('ANSWER')    
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
                {'$push': {'tags': action['tag']}},
                safe=True)
        elif (action.get_type() == 'enrolling'):
            if not self.is_optin(participant_phone):
                self.run_action(
                    participant_phone, 
                    OptinAction(), 
                    context, 
                    participant_session_id)
            self.collections['participants'].update(
                {'phone': participant_phone,
                 'enrolled.dialogue-id': {'$ne': action['enroll']}},
                {'$push': {'enrolled': {'dialogue-id': action['enroll'],
                                        'date-time': time_to_vusion_format(self.get_local_time())}}},
                safe=True)
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
                                        'raw': context['message']}}},
                safe=True)
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
	elif (action.get_type() == 'proportional-tagging'):
	    if self.is_tagged(participant_phone, action.get_tags()):
		return
	    for tag in action.get_tags():
		action.set_tag_count(tag, self.collections['participants'].find({'tags': tag}).count())
	    self.run_action(participant_phone, action.get_tagging_action())
	elif (action.get_type() == 'message-forwarding'):
	    self.run_action_message_forwarding(participant_phone, action, context, participant_session_id)
        else:
            self.log("The action is not supported %s" % action.get_type())

    @inlineCallbacks
    def run_action_message_forwarding(self, participant_phone, action, context, participant_session_id):
	if not self.properties.is_sms_forwarding_allowed():
	    self.log('SMS Forwarding not allowed, dump action')
	    return
	history = self.collections['history'].get_history(context['history_id'])
	message = TransportUserMessage(**{
	    'to_addr': action['forward-url'],
	    'from_addr': self.transport_name,
	    'transport_name': self.transport_name,
	    'transport_type': 'http_forward',
	    'content': history['message-content'],
	    'transport_metadata': {
	        'program_shortcode': self.properties['shortcode'],
	        'participant_phone': participant_phone}
	})
	yield self.transport_publisher.publish_message(message)
	self.collections['history'].update_forwarding(context['history_id'], message['message_id'], action['forward-url'])

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
                self.run_action(message['from_addr'], actions.get_priority_action(), context)
            participant = self.get_participant(message['from_addr'], only_optin=True)
            message_credits = self.properties.use_credits(message['content'])
            history.update({
                'participant-phone': message['from_addr'],
                'participant-session-id': (participant['session-id'] if participant else None),        
                'message-content': message['content'],
                'message-direction': 'incoming',
                'message-credits': message_credits})
            history.update(context.get_dict_for_history())
            history_id = self.save_history(**history)
	    context['history_id'] = str(history_id)
            self.credit_manager.received_message(message_credits)
            self.update_participant_transport_metadata(message)
            if (context.is_matching() and participant is not None):
                if ('interaction' in context):
                    if self.has_oneway_marker(participant['phone'], participant['session-id'], context):
                        actions.clear_all()
                    else:
                        self.get_program_actions(participant, context, actions)
                        if self.participant_has_max_unmatching_answers(participant, context['dialogue-id'], context['interaction']):
                            self.add_oneway_marker(participant['phone'], participant['session-id'], context)
                            context['interaction'].get_max_unmatching_action(context['dialogue-id'], actions)
                elif ('request-id' in context):
                    self.get_program_actions(participant, context, actions)                    
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

    def is_tagged(self, participant_phone, tags):
        query = {'phone':participant_phone,
                 'tags': {'$in': tags}}
        result = self.collections['participants'].find(query).limit(1).count()
        return 0 < self.collections['participants'].find(query).limit(1).count()

    def is_enrolled(self, participant, dialogue_id):
        for enrolled in participant['enrolled']:
            if enrolled['dialogue-id']==dialogue_id:
                return True
        return False

    def is_optin(self, participant_phone):
        query = {'phone':participant_phone,
                 'session-id': {'$ne': None}}
        return (0 != self.collections['participants'].find(query).limit(1).count())

    def has_already_valid_answer(self, participant, dialogue_id, interaction_id, number=1):
        query = {'participant-phone': participant['phone'],
                 'participant-session-id':participant['session-id'],
                 'message-direction': 'incoming',
                 'matching-answer': {'$ne': None},
                 'dialogue-id': dialogue_id,
                 'interaction-id': interaction_id}
        history = self.collections['history'].find(query)
        if history is None or history.count() <= number:
            return False
        return True        

    def has_one_way_marker(self, participant, dialogue_id, interaction_id):
        query = {
            'object-type': 'oneway-marker-history',
            'participant-phone': participant['phone'],
            'participant-session-id':participant['session-id'],
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id}
        history = self.collections['history'].find_one(query)
        if history is None:
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
        self.load_properties()
        if self.is_ready():
            self.credit_manager.check_status()
            self.send_scheduled()
        next_iteration = self.get_time_next_daemon_iteration()
        if not self.sender.active():
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
            if self.sender.active():
                self.sender.reset(secondsLater)
            else:
                self.log("Call later not active anymore, schedule a new one")
                reactor.callLater(
                    secondsLater,
                    self.daemon_process)

    def load_properties(self, if_needed_register_keywords=True):
        try:
            was_ready = self.properties.is_ready()
            ## the default callbacks in case the value is changing
            callbacks = {
                'credit-type': self.credit_manager.set_limit,
                'credit-number': self.credit_manager.set_limit,
                'credit-from-date': self.credit_manager.set_limit,
                'credit-to-date': self.credit_manager.set_limit,
	        'timezone': self.log_manager.clear_logs}
            if if_needed_register_keywords == True:
                callbacks.update({'shortcode': self.register_keywords_in_dispatcher})
            self.properties.load(callbacks)
        except MissingProperty as e:
            self.log("Missing Mandatory Property: %s" % e.message)
            if was_ready:
                self.log("Worker is unregistering all keyword from dispatcher")
                self.unregister_from_dispatcher()

    def is_ready(self):
        return self.properties.is_ready()

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
            if unattach.is_selectable(participant):
                self.schedule_participant_unattach(participant, unattach)

    def get_future_unattachs(self):
        query = {'fixed-time': {
            '$gt': time_to_vusion_format(self.get_local_time())}}
        unattachs = []
        for unattach in self.collections['unattached_messages'].find(query):
            try:
                unattachs.append(UnattachMessage(**unattach))
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log("Error while retriving participant %r" %
                         traceback.format_exception(exc_type, exc_value, exc_traceback))
        return unattachs

    def schedule_dialogue(self, dialogue_id):
        dialogue = self.get_current_dialogue(dialogue_id)
        participants = self.get_participants(
            {'enrolled.dialogue-id': dialogue_id,
             'session-id': {'$ne': None}})
        self.schedule_participants_dialogue(participants, dialogue)

    def get_unattach_message(self, unattach_id):
        try:
            return UnattachMessage(**self.collections['unattached_messages'].find_one({
                '_id': ObjectId(unattach_id)}))
        except TypeError:
            self.log("Error unattach message %s cannot be found" % unattach_id)
            return None
        
    def schedule_unattach(self, unattach_id):
        #clear all schedule
        self.collections['schedules'].remove({'unattach-id': unattach_id})
        unattach = self.get_unattach_message(unattach_id)
        if unattach is None:
            return
        selectors = unattach.get_selector_as_query()
        query = {'session-id': {'$ne': None}}
        query.update(selectors)
        participants = self.get_participants(query)
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

                #The iteraction has aleardy been sent.
                if history:
                    previous_sending_date_time = time_from_vusion_format(history["timestamp"])
                    self.schedule_participant_reminders(
                        participant, dialogue, interaction, previous_sending_date_time, True)
                    previous_sending_day = previous_sending_date_time.date()
                    continue
                
                if (interaction['type-schedule'] == 'offset-days'):
                    enrolled = self.get_enrollment_time(participant, dialogue)
                    sending_date_time = get_offset_date_time(
                        time_from_vusion_format(enrolled['date-time']),
                        interaction['days'],
                        interaction['at-time'])
                elif (interaction['type-schedule'] == 'offset-time'):
                    enrolled = self.get_enrollment_time(participant, dialogue)
                    sending_date_time = time_from_vusion_format(enrolled['date-time']) + interaction.get_offset_time_delta()
                elif (interaction['type-schedule'] == 'fixed-time'):
                    sending_date_time = time_from_vusion_format(interaction['date-time'])
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
                    sending_date_time = self.get_local_time()

                schedule = self.collections['schedules'].find_one({
                    "participant-phone": participant['phone'],
                    "object-type": 'dialogue-schedule',
                    "dialogue-id": dialogue["dialogue-id"],
                    "interaction-id": interaction["interaction-id"]})        

                #Scheduling a date already in the past is forbidden.
                if (sending_date_time + timedelta(minutes=5) < self.get_local_time()):
                    history = {
                        'object-type': 'datepassed-marker-history',
                        'participant-phone': participant['phone'],
                        'participant-session-id': participant['session-id'],
                        'dialogue-id': dialogue['dialogue-id'],
                        'interaction-id': interaction['interaction-id'],
                        'scheduled-date-time': time_to_vusion_format(sending_date_time)}
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
                    {'date-time': sending_date_time})
                self.save_schedule(**schedule)
                self.schedule_participant_reminders(participant, dialogue, interaction, sending_date_time)
                self.update_time_next_daemon_iteration()
        except:
            self.log("Scheduling dialogue exception: %s" % dialogue['dialogue-id'])
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error during schedule message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    def schedule_participant_reminders(self, participant, dialogue, interaction,
                                       interaction_date_time, is_interaction_history=False):

        #Do not schedule reminder in case of valide answer or one way marker
        if self.has_one_way_marker(participant, dialogue['dialogue-id'], interaction['interaction-id']):
            return
        if self.has_already_valid_answer(participant, dialogue['dialogue-id'], interaction['interaction-id'], 0):
            return
        
        schedules = self.collections['schedules'].find({
            "participant-phone": participant['phone'],
            "$or":[{"object-type":'reminder-schedule'},
                   {"object-type": 'deadline-schedule'}],
            "dialogue-id": dialogue["dialogue-id"],
            "interaction-id": interaction["interaction-id"]})
        
        #remove all reminder(s)/deadline for this interaction
        has_active_reminders = False
        for reminder_schedule_to_be_deleted in schedules:
            has_active_reminders = True
            self.collections['schedules'].remove(reminder_schedule_to_be_deleted['_id'])
            
        if not interaction.has_reminder():
            return
        if  not has_active_reminders and is_interaction_history:
            return
        
        #get number for already send reminders
        interaction_histories = self.collections['history'].find({
            'participant-phone': participant['phone'],
            'participant-session-id': participant['session-id'],
            'message-direction': 'outgoing',
            'dialogue-id': dialogue['dialogue-id'],
            'interaction-id': interaction['interaction-id']})
        
        already_send_reminder_count = interaction_histories.count() - 1 if interaction_histories.count() > 0 else 0

        #adding reminders
        reminder_times = interaction.get_reminder_times(interaction_date_time)
        for reminder_time in reminder_times[already_send_reminder_count:]:
            reminder = {
                'object-type': 'reminder-schedule',
                'model-version': '2',
                'participant-phone': participant['phone'],
                'participant-session-id': participant['session-id'],
                'date-time': reminder_time,
                'dialogue-id': dialogue['dialogue-id'],
                'interaction-id': interaction['interaction-id']}                                                                               
            self.save_schedule(**reminder)
        
        #adding deadline
        deadline_time = interaction.get_deadline_time(interaction_date_time)
        #We don't schedule deadline in the past
        if deadline_time < self.get_local_time():
            deadline_time = self.get_local_time()
        deadline = {
            'object-type': 'deadline-schedule',
            'model-version': '2',
            'participant-phone': participant['phone'],
            'participant-session-id': participant['session-id'],
            'date-time': interaction.get_deadline_time(interaction_date_time),
            'dialogue-id': dialogue['dialogue-id'],
            'interaction-id': interaction['interaction-id']}                                                                               
        self.save_schedule(**deadline)
        
    def get_local_time(self):
        try:
            return self.properties.get_local_time()
        except:
            return datetime.utcnow()

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
            context = schedule.get_context()
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
                sort=[('date-time', 1)], limit=100)
            for due_schedule in due_schedules:
                self.collections['schedules'].remove(
                    {'_id': due_schedule['_id']})
                yield self.send_schedule(schedule_generator(**due_schedule))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log("Error send_scheduled: %r" %
                     traceback.format_exception(exc_type, exc_value, exc_traceback))     

    @inlineCallbacks
    def send_schedule(self, schedule):
        try:            
            local_time = self.get_local_time()

            ## Delayed action are always run even if there original interaction has been deleted
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

        ## Get source unattached, interaction or request
            interaction, context = self.from_schedule_to_message(schedule)
            
            if not interaction:
                self.log("Sender failure, cannot build process %r" % schedule)
                return

        ## Run the Deadline
            if schedule.get_type() == 'deadline-schedule':
                actions = Actions()
                if interaction.has_reminder():
                    for action in interaction['reminder-actions']:
                        actions.append(action_generator(**action))
                    self.add_oneway_marker(
                        schedule['participant-phone'],
                        schedule['participant-session-id'],
                        context.get_dict_for_history())
                for action in actions.items():
                    self.run_action(
                        schedule['participant-phone'],
                        action,
                        context,
                        schedule['participant-session-id'])
                return

            ## Reaching this line can only be message to be send
            message_content = self.generate_message(interaction)
            message_content = self.customize_message(
                message_content,
                schedule['participant-phone'])

            ## Do not run expired schedule
            if schedule.is_expired(local_time):
                history = {
                    'object-type': 'datepassed-marker-history',
                    'participant-phone': schedule['participant-phone'],
                    'participant-session-id': schedule['participant-session-id'],
                    'scheduled-date-time': schedule['date-time']}
                history.update(context.get_dict_for_history())
                self.save_history(**history)
                return
                 
            message = TransportUserMessage(**{
                'from_addr': self.properties['shortcode'],
                'to_addr': schedule['participant-phone'],
                'transport_name': self.transport_name,
                'transport_type': self.transport_type,
                'content': message_content})

            ## Apply program settings
            if (self.properties['customized-id'] is not None):
                message['transport_metadata']['customized_id'] = self.properties['customized-id']

            ## Check for program properties
            if (schedule.get_type() == 'feedback-schedule'
                and self.properties['request-and-feedback-prioritized'] is not None):
                message['transport_metadata']['priority'] = self.properties['request-and-feedback-prioritized']
            elif ('prioritized' in interaction 
                  and interaction['prioritized'] is not None):
                message['transport_metadata']['priority'] = interaction['prioritized']

            ## Necessary for some transport that require tocken to be reuse for MT message
            #TODO only fetch when participant has transport metadata...
            participant = self.get_participant(schedule['participant-phone'])
            if (participant['transport_metadata'] is not {}):
                message['transport_metadata'].update(participant['transport_metadata'])
            
            message_credits = self.properties.use_credits(message_content)
            if self.credit_manager.is_allowed(message_credits, schedule):
                yield self.transport_publisher.publish_message(message)
                message_status = 'pending'
                self.log("Message has been sent to %s '%s'" % (message['to_addr'], message['content']))
            else: 
                message_credits = 0
                if self.credit_manager.is_timeframed():
                    message_status = 'no-credit'
                else:
                    message_status = 'no-credit-timeframe'
                    self.log("%s, message hasn't been sent to %s '%s'" % (
                        message_status, message['to_addr'], message['content']))

            history = {
                'message-content': message['content'],
                'participant-phone': message['to_addr'],
                'message-direction': 'outgoing',
                'message-status': message_status,
                'message-id': message['message_id'],
                'message-credits': message_credits}
            history.update(context.get_dict_for_history(schedule))
            self.save_history(**history)
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
        self.log_manager.log(msg, level)

    def get_keywords(self):
        keywords = []
        for dialogue in self.get_active_dialogues():
            keywords += dialogue.get_all_keywords()
        for request in self.get_requests():
            keywords += request.get_keywords()
        ## remove potential duplicate due to request exact matching
        return sorted(set(keywords))
    
    @inlineCallbacks
    def register_keywords_in_dispatcher(self):
        self.log('Synchronizing with dispatcher')
        keywords = self.get_keywords()
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

    # TODO more to the Participant class
    def get_participant_label_value(self, participant, label):
        label_indexer = dict((p['label'], p['value']) for i, p in enumerate(participant['profile']))
        return label_indexer.get(label, None)

    def customize_message(self, message, participant_phone=None):
        custom_regexp = re.compile(r'\[(?P<domain>[^\.\]]+)\.(?P<key1>[^\.\]]+)(\.(?P<key2>[^\.\]]+))?(\.(?P<otherkey>[^\.\]]+))?\]')
        matches = re.finditer(custom_regexp, message)
        for match in matches:
            match = match.groupdict() if match is not None else None
            if match is not None:
                if match['domain'].lower() in ['participant', 'participants']:
                    if participant_phone is None:
                        raise MissingData('No participant supplied for this message.')
                    participant = self.get_participant(participant_phone)
                    participant_label_value = participant.get_participant_label_value(match['key1'])
                    if not participant_label_value:
                        raise MissingData("%s has no attribute %s" % 
                                          (participant_phone, match['key1']))
                    message = message.replace('[%s.%s]' %
                                              (match['domain'], match['key1']),
                                              participant_label_value) 
                elif match['domain'] == 'contentVariable':
                    condition = {'keys':[{'key':match['key1']}]}
                    if match['key2'] is not None:
                        condition['keys'].append({'key':match['key2']})
                    condition = {'$and':[condition]}
                    condition['$and'].append({'keys':{'$size': len(condition['$and'][0]['keys'])}})
                    content_variable = self.collections['content_variables'].find_one(condition)
                    if not content_variable:
                        raise MissingData("Program has no content variables [%s.%s]" %
                                              (match['key1'], match['key2']))
                    content_variable = ContentVariable(**content_variable)
                    if match['key2'] is not None:
                        replace_match = '[%s.%s.%s]' % (match['domain'], match['key1'], match['key2'])
                    else:
                        replace_match = '[%s.%s]' % (match['domain'], match['key1'])
                    message = message.replace(replace_match, content_variable['value'])
                else:
                    self.log("Dynamic content domain not supported %s" % domain)
        return message
    
