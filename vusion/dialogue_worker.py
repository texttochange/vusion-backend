# -*- test-case-name: tests.test_ttc -*-
import sys
import traceback
import re

from twisted.internet.defer import inlineCallbacks, Deferred, returnValue
from twisted.internet import task, reactor

from pymongo import MongoClient
from bson.objectid import ObjectId

from redis import Redis

from datetime import datetime, time, date, timedelta
import pytz

from vumi.application import ApplicationWorker
from vumi.message import Message, TransportUserMessage, TransportEvent
from vumi.application import SessionManager
from vumi import log
from vumi.errors import VumiError

from vusion.utils import (
    time_to_vusion_format, get_local_time, get_local_time_as_timestamp,
    time_from_vusion_format, get_shortcode_value, get_offset_date_time,
    split_keywords, add_char_to_pattern, dynamic_content_notation_to_string,
    clean_phone)
from vusion.error import (
    MissingData, SendingDatePassed, VusionError, MissingTemplate,
    MissingProperty)
from vusion.message import DispatcherControl, WorkerControl
from vusion.context import Context
from vusion.component import (
    DialogueWorkerPropertyHelper, CreditManager, RedisLogger)

from vusion.persist.action import (
    Actions, action_generator, FeedbackAction, EnrollingAction, OptinAction,
    OptoutAction, RemoveRemindersAction)
from vusion.persist import (
    FeedbackSchedule, HistoryManager, ContentVariableManager, DialogueManager,
    RequestManager, ParticipantManager, ScheduleManager,
    ProgramCreditLogManager, ShortcodeManager, UnattachedMessageManager)

from vusion.connectors import (
    ReceiveWorkerControlConnector, SendControlConnector)


class DialogueWorker(ApplicationWorker):

    @inlineCallbacks
    def setup_application(self):   
        self.sender = None
        self.r_prefix = None
        self.r_config = {}
        self.control_name = None
        self.transport_name = None
        self.program_name = None
        
        #Store basic configuration data
        self.transport_name = self.config['transport_name']
        self.control_name = self.config['control_name']
        self.r_config = self.config.get('redis_config', {})
        self.r_prefix = "%(control_name)s:" % self.config

        #Initializing
        self.program_name = None
        self.last_script_used = None
        self.r_key = 'vusion:programs:' + self.config['database_name']
        self.r_server = Redis(**self.r_config)

        # Component / Manager initialization
        self.logger = RedisLogger(
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

        self.logger.startup(self.properties)

        #TODO replace by a loop
        for collection in ['history', 'dialogues', 'requests', 'participants', 
                           'content_variables', 'schedules', 'credit_logs',
                           'shortcodes', 'unattached_messages']:
            self.collections[collection].set_property_helper(self.properties)
            self.collections[collection].set_log_helper(self.logger)

        self.credit_manager = CreditManager(
           self.r_key, self.r_server,
           self.collections['credit_logs'],
           self.collections['history'], 
           self.collections['schedules'],
           self.properties, 
           self.logger)

        self.logger.log("Dialogue Worker is starting")
        yield self.setup_dc_connector(self.config['dispatcher_name'])
        #Will need to register the keywords
        self.load_properties(if_needed_register_keywords=True)
        self.sender = reactor.callLater(2, self.daemon_process)
        
    def teardown_application(self):
        self.log("Worker is stopped.")
        self.logger.stop()
        if self.is_ready():
            self.unregister_from_dispatcher()
        if (self.sender.active()):
            self.sender.cancel()

    def setup_connectors(self):
        d = super(DialogueWorker, self).setup_connectors()

        def cb2(connector):
            connector.set_control_handler(self.dispatch_control)
            return connector
        
        return d.addCallback(cb2)

    @inlineCallbacks
    def dispatch_control(self, control):
        yield self.consume_control(control)

    def setup_ri_connector(self, connector_name):
        return self.setup_connector(
            ReceiveWorkerControlConnector,
            self.transport_name)

    def setup_dc_connector(self, connector_name):
        return self.setup_connector(SendControlConnector, connector_name)

    def save_history(self, **kwargs):
        return self.collections['history'].save_history(**kwargs)
  
    def init_program_db(self, database_name, vusion_database_name):
        self.log("Initialization of the program")
        self.database_name = database_name
        self.vusion_database_name = vusion_database_name
        self.log("Connecting to database: %s" % self.database_name)

        #Initilization of the database
        mongo_client = MongoClient(
            self.config['mongodb_host'],
            self.config['mongodb_port'],
            w=1) #write in safe mode by default

        ## Program specific
        program_db = mongo_client[self.database_name]
        self.setup_collections(program_db, {'program_settings': None,
                                            'unattached_messages': None})
        self.collections['history'] = HistoryManager(program_db, 'history', self.r_prefix, self.r_server)
        self.collections['content_variables'] = ContentVariableManager(program_db, 'content_variables')
        self.collections['dialogues'] = DialogueManager(program_db, 'dialogues')
        self.collections['requests'] = RequestManager(program_db, 'requests')
        self.collections['participants'] = ParticipantManager(program_db, 'participants')
        self.collections['schedules'] = ScheduleManager(program_db, 'schedules')
        self.collections['unattached_messages'] = UnattachedMessageManager(program_db, 'unattached_messages')

        ## Vusion 
        vusion_db = mongo_client[self.vusion_database_name]
        self.setup_collections(vusion_db, {'templates': None})
        self.collections['shortcodes'] = ShortcodeManager(
            vusion_db, 'shortcodes')
        self.collections['credit_logs'] = ProgramCreditLogManager(
            vusion_db, 'credit_logs', self.database_name)

    def setup_collections(self, db, names):
        for name, index in names.items():
            self.setup_collection(db, name, index)

    def setup_collection(self, db, name, index):
        if name in db.collection_names():
            self.collections[name] = db[name]
        else:
            self.collections[name] = db.create_collection(name)
        if index is not None:
            self.collections[name].ensure_index(index, background=True)
        self.log("Collection initialised: %s" % name)

    @inlineCallbacks
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
                    self.collections['dialogues'].load_dialogue(message['object_id'])
                    self.schedule_dialogue(message['object_id'])
                    self.register_keywords_in_dispatcher()
                elif message['schedule_type'] == 'unattach':
                    yield self.schedule_unattach(message['object_id'])
                elif message['schedule_type'] == 'participant':
                    yield self.schedule_participant(message['object_id'])
                self.update_time_next_daemon_iteration()
            elif message['action'] == 'mass_tag':
                yield self.schedule_mass_tag(message['tag'], message['selector'])
                self.update_time_next_daemon_iteration()
            elif message['action'] == 'mass_untag':
                yield self.schedule_mass_untag(message['tag'])
            elif message['action'] == 'reload_request':
                self.collections['requests'].load_request(message['object_id'])
                self.register_keywords_in_dispatcher()
            elif message['action'] == 'test_send_all_messages':
                dialogue = self.collections['dialogues'].get_dialogue_obj(message['dialogue_obj_id'])
                self.send_all_messages(dialogue, message['phone_number'])
            elif message['action'] == 'update_registered_keywords':
                self.register_keywords_in_dispatcher()
            elif message['action'] == 'run_actions':
                actions = self.collections['dialogues'].get_actions(
                    message['dialogue_id'],
                    message['interaction_id'],
                    message['answer'])
                for action in actions.items():
                    yield self.run_action(message['participant_phone'], action)
        except (VusionError, VumiError) as e:
            self.log('ERROR: %s(%s)' % (e.__class__.__name__, e.message), level='error')
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "UNKNOWN ERROR during consume control message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    def dispatch_event(self, message):
        self.log("Event message received %s" % (message,))
        new_status, old_status, credits = self.collections['history'].update_status_from_event(message)
        if new_status is None:
            return
        self.collections['credit_logs'].increment_event_counter(old_status, new_status, credits)

    def update_participant_transport_metadata(self, message):
        if message['transport_metadata'] is not {}:
            self.collections['participants'].save_transport_metadata(
                message['from_addr'], message['transport_metadata'])

    @inlineCallbacks
    def run_action(self, participant_phone, action, context=Context(),
                   participant_session_id=None):
        if action.has_condition():
            query = action.get_condition_mongodb_for(participant_phone, participant_session_id)
            if not self.collections['participants'].is_matching(query):
                self.log(("Participant %s doesn't satify the condition for action for %s" % (participant_phone, action,)))
                return
        self.log(("Run action for %s action %s" % (participant_phone, action,)))
        if (action.get_type() == 'optin'):
            if self.collections['participants'].opting_in(participant_phone):
                self.schedule_participant(participant_phone)
            else:
                ## The participant is still optin and opting in again
                if self.properties['double-optin-error-feedback'] is not None:
                    self.run_action(
                        participant_phone, 
                        FeedbackAction(**{'content': self.properties['double-optin-error-feedback']}),
                        context,
                        participant_session_id)
        elif (action.get_type() == 'optout'):
            self.collections['participants'].opting_out(participant_phone)
            self.collections['schedules'].remove_participant_schedules(participant_phone)
        elif (action.get_type() == 'feedback'):
            schedule = FeedbackSchedule(**{
                'participant-phone': participant_phone,
                'participant-session-id': participant_session_id,
                'date-time': time_to_vusion_format(self.get_local_time()),
                'content': self.customize_message(action['content'], participant_phone, context, False),
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
                'participant-phone': participant_phone,
                'participant-session-id': participant_session_id,
                'date-time': time_to_vusion_format(self.get_local_time()),
                'content': error_message,
                'context': context.get_dict_for_history()})
            self.send_schedule(schedule)
        elif (action.get_type() == 'tagging'):
            self.collections['participants'].tagging(participant_phone, action['tag'])
            yield self.schedule_participant(participant_phone)
        elif (action.get_type() == 'enrolling'):
            if not self.collections['participants'].is_optin(participant_phone):
                self.run_action(
                    participant_phone, 
                    OptinAction(), 
                    context, 
                    participant_session_id)
            self.collections['participants'].enrolling(
                participant_phone, action['enroll'])
            dialogue = self.collections['dialogues'].get_current_dialogue(action['enroll'])
            if dialogue is None:
                self.log(("Enrolling error: Missing Dialogue %s" % action['enroll']))
                return
            participant = self.collections['participants'].get_participant(participant_phone)
            self.schedule_participant_dialogue(participant, dialogue)
        elif (action.get_type() == 'delayed-enrolling'):
            schedule_time = get_offset_date_time(
                self.get_local_time(), 
                action['offset-days']['days'],
                action['offset-days']['at-time'])
            action = EnrollingAction(**{'enroll': action['enroll']})
            self.collections['schedules'].add_action(
                participant_phone, participant_session_id,
                schedule_time, action, context)
        elif (action.get_type() == 'profiling'):
            self.collections['participants'].labelling(
                participant_phone, action['label'], action['value'], context['message'])
            yield self.schedule_participant(participant_phone)
        elif (action.get_type() == 'offset-conditioning'):
            participant = self.collections['participants'].get_participant(participant_phone, True)
            if participant is None:
                return
            self.schedule_participant_dialogue(
                participant,
                self.collections['dialogues'].get_current_dialogue(action['dialogue-id']))
        elif (action.get_type() == 'remove-question'):
            self.collections['schedules'].remove_participant_interaction(
                participant_phone, action['dialogue-id'], action['interaction-id'])
        elif (action.get_type() == 'remove-reminders'):
            self.collections['schedules'].remove_participant_reminders(
                participant_phone, action['dialogue-id'], action['interaction-id'])
        elif (action.get_type() == 'remove-deadline'):
            self.collections['schedules'].remove_participant_deadline(
                participant_phone, action['dialogue-id'], action['interaction-id'])
        elif (action.get_type() == 'reset'):
            self.run_action(participant_phone, OptoutAction())
            self.run_action(participant_phone, OptinAction())
        elif (action.get_type() == 'proportional-tagging'):
            yield self.run_action_proportional_tagging(participant_phone, action)
        elif (action.get_type() == 'proportional-labelling'):
            yield self.run_action_proportional_labelling(participant_phone, action)
        elif (action.get_type() == 'url-forwarding'):
            yield self.run_action_url_forwarding(participant_phone, action, context, participant_session_id)
        elif (action.get_type() == 'sms-forwarding'):
            yield self.run_action_sms_forwarding(participant_phone, action, context)
        elif (action.get_type() == 'sms-invite'):
            yield self.run_action_sms_invite(participant_phone, action, context)
        else:
            self.log("The action is not supported %s" % action.get_type())

    @inlineCallbacks
    def run_action_url_forwarding(self, participant_phone, action, context, participant_session_id):
        if not self.properties.is_sms_forwarding_allowed():
            self.log('SMS Forwarding not allowed, dump action')
            return
        history = self.collections['history'].get_history(context['history_id'])
        participant = self.collections['participants'].get_participant(participant_phone)
        options = {
           'from_addr': self.transport_name,
           'transport_name': self.transport_name,
           'transport_type': 'http_api',
           'transport_metadata': {
               'program_shortcode': self.properties['shortcode'],
               'participant_phone': participant_phone,
               'participant_profile': participant['profile']}}
        message = yield self.send_to(
            action['forward-url'], history['message-content'], **options)
        self.collections['history'].update_forwarding(
            context['history_id'], message['message_id'], action['forward-url'])

    @inlineCallbacks
    def run_action_proportional_tagging(self, participant_phone, action, context=None):
        if self.collections['participants'].is_tagged(participant_phone, action.get_tags()):
            return
        for tag in action.get_tags():
            count = yield self.collections['participants'].count_tag_async(tag)
            action.set_tag_count(tag, count)
        self.run_action(participant_phone, action.get_tagging_action())

    @inlineCallbacks
    def run_action_proportional_labelling(self, participant_phone, action, context=None):
        if self.collections['participants'].is_labelled(participant_phone, action.get_label_name()):
            return
        for label in action.get_labels():
            count = yield self.collections['participants'].count_label_async(label)
            action.set_count(label['value'], count)
        self.run_action(participant_phone, action.get_labelling_action())

    @inlineCallbacks
    def run_action_sms_forwarding(self, participant_phone, action, context):
        sender = self.collections['participants'].get_participant(participant_phone)
        query = action.get_query_selector(sender, context)
        participants = self.collections['participants'].get_participants(query)
        if participants.count() == 0 and action.has_no_participant_feedback():
            content = self.customize_message(
                action.get_no_participant_feedback(),
                sender['phone'], 
                context)
            schedule = FeedbackSchedule(**{
                'participant-phone': sender['phone'],
                'participant-session-id': sender['session-id'],
                'date-time': self.get_local_time('vusion'),
                'content': content,
                'context': context.payload})
            yield self.send_schedule(schedule)
            return
        for participant in participants:
            content = self.customize_message(
                action['forward-content'],
                participant_phone,
                context)
            schedule = FeedbackSchedule(**{
                'participant-phone': participant['phone'],
                'participant-session-id': participant['session-id'],
                'date-time': self.get_local_time('vusion'),
                'content': content,
                'context': context.payload})
            yield self.send_schedule(schedule)

    @inlineCallbacks
    def run_action_sms_invite(self, participant_phone, action, context):
        sender = self.collections['participants'].get_participant(participant_phone)
        invitee_phone = clean_phone(context.get_message_second_word())

        if (invitee_phone is None):
            schedule = FeedbackSchedule(**{
                'participant-phone': sender['phone'],
                'participant-session-id': sender['session-id'],
                'date-time': self.get_local_time('vusion'),
                'content': action['feedback-inviter'],
                'context': context.payload})
            yield self.send_schedule(schedule)
            return
        if not self.collections['participants'].opting_in(invitee_phone):
            schedule = FeedbackSchedule(**{
                'participant-phone': sender['phone'],
                'participant-session-id': sender['session-id'],
                'date-time': self.get_local_time('vusion'),
                'content': action['feedback-inviter'],
                'context': context.payload})
            yield self.send_schedule(schedule)
            return
        else:
            ## The participant is opting
            invitee = self.collections['participants'].get_participant(invitee_phone)
            self.collections['participants'].tagging(
                invitee['phone'], action['invitee-tag'])
            content = self.customize_message(
                action['invite-content'],
                participant_phone,
                context)
            schedule = FeedbackSchedule(**{
                'participant-phone': invitee['phone'],
                'participant-session-id': invitee['session-id'],
                'date-time': self.get_local_time('vusion'),
                'content': content,
                'context': context.payload})
            yield self.send_schedule(schedule)

    def consume_user_message(self, message):
        self.log("User message received from %s '%s'" % (message['from_addr'],
                                                         message['content']))
        try:
            history = {'object-type': 'unmatching-history'}
            context = Context(**{'message': message['content']})
            actions = Actions()
            self.collections['requests'].get_matching_request_actions(
                message['content'], actions, context)
            if context.is_matching():
                history = {'object-type': 'request-history'}
            else:
                self.collections['dialogues'].get_matching_dialogue_actions(
                    message['content'], actions, context)
                if context.is_matching():
                    history = {'object-type': 'dialogue-history'}
            # High priority to run an optin or enrolling action to get sessionId 
            if (not self.collections['participants'].is_optin(message['from_addr']) 
                    and (actions.contains('optin') or actions.contains('enrolling'))):
                self.run_action(message['from_addr'], actions.get_priority_action(), context)
            participant = self.collections['participants'].get_participant(message['from_addr'], only_optin=True)
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
                    if self.collections['history'].has_oneway_marker(
                        participant['phone'], participant['session-id'],
                        context['dialogue-id'], context['interaction-id']):
                        actions.clear_all()
                    else:
                        self.get_program_actions(participant, context, actions)
                        if self.collections['history'].participant_has_max_unmatching_answers(participant, context['dialogue-id'], context['interaction']):
                            self.collections['history'].add_oneway_marker(
                                participant['phone'], participant['session-id'], context)
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
            and self.collections['history'].has_already_valid_answer(participant, context['dialogue-id'], context['interaction-id'])):
            actions.clear_all()
            if self.properties['double-matching-answer-feedback'] is not None:
                actions.append(FeedbackAction(**{'content': self.properties['double-matching-answer-feedback']}))

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
            schedule_time = self.collections['schedules'].get_next_schedule_time()
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
               'timezone': self.logger.clear_logs}
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

    @inlineCallbacks
    def schedule_mass_untag(self, tag):
        unattacheds = self.collections['unattached_messages'].get_unattached_messages_selector_tag(tag)
        for unattached in unattacheds:
            yield self.schedule_unattach(unattached['_id'])

    @inlineCallbacks
    def schedule_mass_tag(self, tag, query):
        participants = self.collections['participants'].get_participants(query)
        for participant in participants:
            yield self._schedule_participant(participant)

    @inlineCallbacks
    def schedule_participant(self, participant_phone):
        participant = self.collections['participants'].get_participant(participant_phone, True)
        if participant is None:
            return
        yield self._schedule_participant(participant)

    @inlineCallbacks
    def _schedule_participant(self, participant):
        ## schedule dialogues
        for dialogue in self.collections['dialogues'].get_active_dialogues():
            if dialogue.is_enrollable(participant):
                self.collections['participants'].enrolling(
                    participant['phone'], dialogue['dialogue-id'])
                #Require to load again the participant in order to get the enrollment time
                participant = self.collections['participants'].get_participant(participant['phone'])
            # participant could be manually enrolled
            if participant.is_enrolled(dialogue['dialogue-id']):
                self.schedule_participant_dialogue(participant, dialogue)
        ## schedule unattach message s       
        unattacheds = self.collections['unattached_messages'].get_unattached_messages()
        for unattached in unattacheds:
            yield self.collections['schedules'].unattach_schedule(
                participant, unattached)

    ## Scheduling of unattach messages
    @inlineCallbacks
    def schedule_unattach(self, unattach_id):
        #clear all schedule
        self.collections['schedules'].remove_unattach(unattach_id)
        unattach = self.collections['unattached_messages'].get_unattached_message(unattach_id)
        if unattach is None:
            return
        selectors = unattach.get_selector_as_query()
        query = {'session-id': {'$ne': None}}
        query.update(selectors)
        participants = self.collections['participants'].get_participants(query)
        yield self.schedule_participants_unattach(participants, unattach)

    @inlineCallbacks
    def schedule_participants_unattach(self, participants, unattach):
        for participant in participants:
            yield self.schedule_participant_unattach(participant, unattach)

    @inlineCallbacks
    def schedule_participant_unattach(self, participant, unattach):
        yield self.collections['schedules'].save_unattach_schedule(
            participant, unattach)

    def schedule_dialogue(self, dialogue_id):
        dialogue = self.collections['dialogues'].get_current_dialogue(dialogue_id)
        #enroll if they are not already enrolled in auto-enrollment
        query = dialogue.get_auto_enrollment_as_query()
        if query is not None:
            self.collections['participants'].enrolling_participants(query, dialogue_id)
        participants = self.collections['participants'].get_participants(
            {'enrolled.dialogue-id': dialogue_id,
             'session-id': {'$ne': None}})
        self.schedule_participants_dialogue(participants, dialogue)

    def schedule_participants_dialogue(self, participants, dialogue):
        for participant in participants:
            self.schedule_participant_dialogue(participant, dialogue)

    def schedule_participant_dialogue(self, participant, dialogue):
        try:
            for interaction in dialogue.interactions:
                self.log("Scheduling %s interaction %s for %s" % 
                         (dialogue['name'], interaction['content'], participant['phone'],))

                ##If we have any marker associate with this interaction,
                ##no schedule is done.
                has_marker = self.collections['history'].has_marker(
                    participant,
                    dialogue["dialogue-id"],
                    interaction["interaction-id"])
                if has_marker:
                    continue

                ##The iteraction has aleardy been sent,
                ##the reminders might need to be updated.
                history = self.collections['history'].get_history_of_interaction(
                    participant,
                    dialogue["dialogue-id"],
                    interaction["interaction-id"])
                if history is not None:
                    previous_sending_date_time = history.get_timestamp()
                    self.schedule_participant_reminders(
                        participant,
                        dialogue,
                        interaction,
                        previous_sending_date_time,
                        True)
                    #previous_sending_day = previous_sending_date_time.date()
                    continue

                ##Compute the sending date time for the interaction
                if (interaction['type-schedule'] == 'offset-days'):
                    enrolled_time = participant.get_enrolled_time(dialogue['dialogue-id'])
                    sending_date_time = get_offset_date_time(
                        time_from_vusion_format(enrolled_time),
                        interaction['days'],
                        interaction['at-time'])
                elif (interaction['type-schedule'] == 'offset-time'):
                    enrolled_time = participant.get_enrolled_time(dialogue['dialogue-id'])
                    sending_date_time = time_from_vusion_format(enrolled_time) + interaction.get_offset_time_delta()
                elif (interaction['type-schedule'] == 'fixed-time'):
                    sending_date_time = time_from_vusion_format(interaction['date-time'])
                elif (interaction['type-schedule'] == 'offset-condition'):
                    previous = self.collections['history'].get_history_of_offset_condition_answer(
                        participant,
                        dialogue["dialogue-id"],
                        interaction["offset-condition-interaction-id"])
                    if  previous is None:
                        continue
                    sending_date_time = self.get_local_time() + timedelta(minutes=int(interaction['offset-condition-delay']))

                ##Retrived a schedule associate with the interaction
                schedule = self.collections['schedules'].get_participant_interaction(
                    participant['phone'], dialogue["dialogue-id"], interaction["interaction-id"])
                if (not schedule):
                    self.collections['schedules'].add_dialogue(
                        participant, sending_date_time,
                        dialogue['dialogue-id'], interaction['interaction-id'])
                else:
                    schedule.set_time(sending_date_time)
                    self.collections['schedules'].save_schedule(schedule)
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
        if self.collections['history'].has_oneway_marker(
            participant['phone'], participant['session-id'],
            dialogue['dialogue-id'], interaction['interaction-id']):
            return
        if self.collections['history'].has_already_valid_answer(
            participant, dialogue['dialogue-id'], interaction['interaction-id'], 0):
            return
        
        schedules = self.collections['schedules'].get_participant_reminder_tail(
            participant['phone'], dialogue["dialogue-id"], interaction["interaction-id"])
        
        #remove all reminder(s)/deadline for this interaction
        has_active_reminders = False
        for reminder_schedule_to_be_deleted in schedules:
            has_active_reminders = True
            self.collections['schedules'].remove_schedule(reminder_schedule_to_be_deleted)
            
        if not interaction.has_reminder():
            return
        if  not has_active_reminders and is_interaction_history:
            return
        
        #get number for already send reminders
        already_send_reminder_count = self.collections['history'].count_reminders(
            participant, dialogue['dialogue-id'], interaction['interaction-id'])

        #adding reminders
        reminder_times = interaction.get_reminder_times(interaction_date_time)
        for reminder_time in reminder_times[already_send_reminder_count:]:
            self.collections['schedules'].add_reminder(
                participant, reminder_time,
                dialogue['dialogue-id'], interaction['interaction-id'])
        
        #adding deadline
        deadline_time = interaction.get_deadline_time(interaction_date_time)
        #We don't schedule deadline in the past
        if deadline_time < self.get_local_time():
            deadline_time = self.get_local_time()
        self.collections['schedules'].add_deadline(
            participant, deadline_time, 
            dialogue['dialogue-id'], interaction['interaction-id'])
    
    # TODO move to the properties helper
    def get_local_time(self, date_format='datetime'):
        try:
            return self.properties.get_local_time(date_format)
        except:
            return datetime.utcnow()

    def from_schedule_to_message(self, schedule):
        if schedule.get_type() in ['dialogue-schedule', 'reminder-schedule', 'deadline-schedule']:
            interaction = self.collections['dialogues'].get_dialogue_interaction(
                schedule['dialogue-id'], schedule['interaction-id'])
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

    #TODO: fire error feedback if the dialogue do not exit anymore
    #TODO fire action scheduled by reminder if no reply is sent for any reminder
    @inlineCallbacks
    def send_scheduled(self):
        try:
            self.log('Checking the schedule list...')
            due_schedules = self.collections['schedules'].get_due_schedules()
            for due_schedule in due_schedules:
                self.collections['schedules'].remove_schedule(due_schedule)
                yield self.send_schedule(due_schedule)
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
                action = action_generator(**schedule['action'])
                if schedule.is_expired(local_time):
                    self.collections['history'].add_datepassed_action_marker(action, schedule)
                    return
                self.run_action(
                    schedule['participant-phone'], 
                    action,
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
                    self.collections['history'].add_oneway_marker(
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
                schedule['participant-phone'],
                context)

            ## Do not run expired schedule
            if schedule.is_expired(local_time):
                self.collections['history'].add_datepassed_marker(schedule, context)
                return
                 
            options = {
                'from_addr': self.properties['shortcode'],
                'transport_name': self.transport_name,
                'transport_type': 'sms',
                'transport_metadata': {}}

            ## Apply program settings
            if (self.properties['customized-id'] is not None):
                options['transport_metadata']['customized_id'] = self.properties['customized-id']

            ## Check for program properties
            if (schedule.get_type() == 'feedback-schedule'
                and self.properties['request-and-feedback-prioritized'] is not None):
                options['transport_metadata']['priority'] = self.properties['request-and-feedback-prioritized']
            elif ('prioritized' in interaction 
                  and interaction['prioritized'] is not None):
                options['transport_metadata']['priority'] = interaction['prioritized']

            ## Necessary for some transport that require tocken to be reuse for MT message
            #TODO only fetch when participant has transport metadata...
            participant = self.collections['participants'].get_participant(schedule['participant-phone'])
            if (participant['transport_metadata'] is not {}):
                options['transport_metadata'].update(participant['transport_metadata'])
            
            message_credits = self.properties.use_credits(message_content)
            if self.credit_manager.is_allowed(message_credits, schedule):
                message = yield self.send_to(schedule['participant-phone'], message_content, **options)
                self.collections['history'].add_outgoing(
                    message, message_credits, context, schedule)
                return
            if self.credit_manager.is_timeframed():
                self.collections['history'].add_nocredit(
                    message_content, context, schedule)
            else:
                self.collections['history'].add_nocredittimeframe(
                    message_content, context, schedule) 
        except MissingData as e:
            self.collections['history'].add_missingdata(
                message_content, e.message, context, schedule)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log("Error send schedule: %r" %
                     traceback.format_exception(exc_type, exc_value, exc_traceback))
            
    #@inlineCallbacks
    def send_all_messages(self, dialogue, phone_number):
        self.log("Sending all dialogue %s messages to %s"
                 % (dialogue['name'], phone_number,))
        for interaction in dialogue['interactions']:
            message_content = self.generate_message(interaction)
            options = {
                'from_addr': self.properties['shortcode'],
                'transport_name': self.transport_name,
                'transport_type': 'sms'}
            self.send_to(phone_number, message_content, **options)

    def log(self, msg, level='msg'):
        if hasattr(self, 'logger'):
            self.logger.log(msg, level)

    def get_keywords(self):
        keywords = self.collections['dialogues'].get_all_keywords()
        keywords += self.collections['requests'].get_all_keywords()
        return sorted(set(keywords))
    
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
        return self.publish_dispatcher_message(msg)

    def unregister_from_dispatcher(self):
        msg = DispatcherControl(action='remove_exposed',
                                exposed_name=self.transport_name)
        return self.publish_dispatcher_message(msg)

    def publish_dispatcher_message(self, message, endpoint_name=None):
        publisher = self.connectors[self.config['dispatcher_name']]
        return publisher.publish_control(message, endpoint_name)

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

    def customize_message(self, message, participant_phone=None, context=None, fail=True):        
        participant = None
        custom_regexp = re.compile(r'\[(?P<domain>[^\.\]]+)\.(?P<key1>[^\.\]]+)(\.(?P<key2>[^\.\]]+))?(\.(?P<key3>[^\.\]]+))?(\.(?P<otherkey>[^\.\]]+))?\]')
        matches = re.finditer(custom_regexp, message)
        for match in matches:
            match = match.groupdict() if match is not None else None
            if match is None:
                continue
            try:
                domain = match['domain']
                keys = {k: match[k] for k in ('key1', 'key2', 'key3') if match[k] is not None}
                replace_match = dynamic_content_notation_to_string(domain, keys)
                if domain.lower() in ['participant', 'participants']:
                    if participant_phone is None:
                        raise MissingData('No participant supplied for this message.')
                    if participant is None:
                        participant = self.collections['participants'].get_participant(participant_phone)
                    participant_label_value = participant.get_data(match['key1'])
                    if not participant_label_value:
                        raise MissingData("Participant %s doesn't have a label %s" % 
                                          (participant_phone, match['key1']))
                    message = message.replace(replace_match, participant_label_value) 
                elif domain == 'contentVariable':
                    content_variable = self.collections['content_variables'].get_content_variable_from_match(match)
                    if content_variable is None:
                        raise MissingData("The program doesn't have a content variable %s" % replace_match)                    
                    message = message.replace(replace_match, content_variable['value'])
                elif domain == 'context':
                    if context is None:
                        raise MissingData("No context for customization")
                    context_data = context.get_data_from_notation(**keys)
                    if context_data is None:
                        raise MissingData("No context data for %s" % replace_match)
                    message = message.replace(replace_match, context_data)
                elif domain == 'time':
                    local_time = self.get_local_time()
                    replace_time = local_time.strftime(add_char_to_pattern(match['key1'], '[a-zA-Z]'))
                    message = message.replace(replace_match, replace_time)
                else:
                    self.log("Customized message domain not supported %s" % match['domain'])
            except Exception, e:
                if fail:
                    raise e
        return message
