#encoding=utf-8
from copy import deepcopy
from datetime import datetime
import time
from copy import deepcopy

from bson.timestamp import Timestamp

from vumi.message import (TransportEvent, TransportMessage,
                          TransportUserMessage, Message)
from vumi.transports.failures import FailureMessage
from vumi.tests.utils import RegexMatcher, UTCNearNow

from vusion.message import DispatcherControl, WorkerControl

from vusion.persist import (Dialogue, DialogueHistory, UnattachHistory,
                            history_generator, schedule_generator, Participant,
                            UnattachMessage, Request, Interaction)


class DataLayerUtils:

    def __init__(self):
        self.collections = {}

    def setup_collections(self, names):
        for name in names:
            self.setup_collection(name)

    def setup_collection(self, name):
        if name in self.db.collection_names():
            self.collections[name] = self.db[name]
        else:
            self.collections[name] = self.db.create_collection(name)

    def drop_collections(self):
        for name, collection in self.collections.items():
            collection.drop()


class DumbLogManager(object):
    
    def __init__(self):
        self.logs = []
    
    def log(self, msg, level='msg'):
        self.logs.append("%s - %s" % (level, msg))


#TODO use UTC time rather than datetime
class MessageMaker:

    def mkmsg_ack(self, user_message_id='1', sent_message_id='abc',
                  transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
        return TransportEvent(
            event_id=RegexMatcher(r'^[0-9a-fA-F]{32}$'),
            event_type='ack',
            user_message_id=user_message_id,
            sent_message_id=sent_message_id,
            timestamp=UTCNearNow(),
            transport_name=self.transport_name,
            transport_metadata=transport_metadata
        )

    def mkmsg_delivery_for_send(self, event_type='delivery_report',
                                user_message_id='1', sent_message_id='abc',
                                delivery_status='delivered',
                                transport_name=None, transport_metadata=None,
                                **kwargs):
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            event_type=event_type,
            user_message_id=user_message_id,
            sent_message_id=sent_message_id,
            delivery_status=delivery_status,
            transport_name=transport_name,
            transport_metadata=transport_metadata,
        )
        for key in kwargs:
            params[key] = kwargs[key]
        return TransportEvent(**params)

    def mkmsg_delivery(self, event_type='delivery_report', user_message_id='1',
                       sent_message_id='abc', delivery_status='delivered',
                       transport_name=None, transport_metadata=None, **kwargs):
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            event_id=RegexMatcher(r'^[0-9a-fA-F]{32}$'),
            timestamp=UTCNearNow(),
            event_type=event_type,
            user_message_id=user_message_id,
#            sent_message_id=sent_message_id,
            delivery_status=delivery_status,
            transport_name=transport_name,
            transport_metadata=transport_metadata,
        )
        for key in kwargs:
            params[key] = kwargs[key]
        return TransportEvent(**params)

    def mkmsg_in(self, content='hello world',
                 from_addr='+41791234567',
                 to_addr='9292',
                 session_event=TransportUserMessage.SESSION_NONE,
                 message_id='abc', transport_type=None,
                 transport_metadata=None, transport_name=None,
                 timestamp=None):
        if timestamp is None:
            timestamp = datetime.now()
        if transport_type is None:
            transport_type = transport_type
        if transport_metadata is None:
            transport_metadata = {}
        return TransportUserMessage(
            from_addr=from_addr,
            to_addr=to_addr,
            group=None,
            message_id=message_id,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            timestamp=timestamp)

    def mkmsg_out(self, content='hello world',
                  session_event=TransportUserMessage.SESSION_NONE,
                  message_id='1', to_addr='+41791234567', from_addr='9292',
                  group=None, in_reply_to=None, transport_type=None,
                  transport_metadata=None, helper_metadata=None,
                  transport_name=None):
        if transport_type is None:
            transport_type = transport_type
        if transport_metadata is None:
            transport_metadata = {}
        if helper_metadata is None:
            helper_metadata = {}
        params = dict(
            to_addr=to_addr,
            from_addr=from_addr,
            group=group,
            message_id=message_id,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            in_reply_to=in_reply_to,
            helper_metadata=helper_metadata)
        return TransportUserMessage(**params)

    def mkmsg_fail(self, message, reason,
                   failure_code=FailureMessage.FC_UNSPECIFIED):
        return FailureMessage(
            timestamp=datetime.now(),
            failure_code=failure_code,
            message=message,
            reason=reason)

    def mkmsg_transport_fail(self, user_message_id='1',
                             failure_level='', failure_code=0,
                             failure_reason='',
                             transport_metadata={}):
        if transport_metadata is None:
            transport_metadata = {}
        return TransportEvent(
            event_id=RegexMatcher(r'^[0-9a-fA-F]{32}$'),
            event_type='delivery_report',
            delivery_status='failed',
            failure_level=failure_level,
            failure_code=failure_code,
            failure_reason=failure_reason,
            user_message_id=user_message_id,
            timestamp=UTCNearNow(),
            transport_name=self.transport_name,
            transport_metadata=transport_metadata)

    def mkmsg_dispatcher_control(self, action='add_exposed',
                                 exposed_name='app2', rules=None):
        if rules is None:
            rules = []
        return DispatcherControl(
            action=action,
            exposed_name=exposed_name,
            rules=rules)

    def mkmsg_multiworker_control(self, message_type='add_worker',
                                  worker_name='m4h', worker_class=None,
                                  config=None):
        if config is None:
            config = []
        return Message(
            message_type=message_type,
            worker_name=worker_name,
            worker_class=worker_class,
            config=config)

    def mkmsg_dialogueworker_control(self, **kwargs):
        return WorkerControl(**kwargs)


class ObjectMaker:

    def mkobj_template_unmatching_keyword(self,
                                          message='KEYWORD does not match any keyword.'):
        return {
            'name': 'error template',
            'type-template': 'unmatching-keyword',
            'template': message}

    shortcodes = {
        'country': 'Uganda',
        'international-prefix': '256',
        'shortcode': '8181',
        'error-template': None,
        'support-customized-id': 0,
        'supported-internationally': 0,
        'max-character-per-sms': 140
    }

    def mkobj_shortcode(self, code='8181', international_prefix='256',
                        error_template=None):
        shortcode = deepcopy(self.shortcodes)
        shortcode['shortcode'] = code
        shortcode['international-prefix'] = international_prefix
        shortcode['error-template'] = error_template        
        return shortcode

    def mkobj_shortcode_international(self, code='+318181'):
        shortcode = deepcopy(self.shortcodes)
        shortcode['country'] = 'Netherlands'
        shortcode['international-prefix'] = '31'
        shortcode['shortcode'] = code
        shortcode['supported-internationally'] = 1
        return shortcode

    def mkobj_shortcode_local(self):
        return deepcopy(self.shortcodes)

    time_format = '%Y-%m-%dT%H:%M:%S'

    simple_config = {
        'database_name': 'test',
        'dispatcher': 'dispatcher',
        'transport_name': 'app'}

    settings = {
        'shortcode': '256-8181',
        'international-prefix': '256',
        'timezone': 'Africa/Kampala',
        'customized-id': 'myid',
        'request-and-feedback-prioritized': 'prioritized',
        'credit-type': 'none',
        'credit-number': None,
        'credit-date-from': None,
        'credit-date-to': None}

    def mk_program_settings(self, shortcode='256-8181',
                            default_template_open_question= None,
                            default_template_closed_question=None,
                            double_matching_answer_feedback=None,
                            double_optin_error_feedback=None,
                            sms_limit_type='none',
                            sms_limit_number=None,
                            sms_limit_from_date=None,
                            sms_limit_to_date=None,
                            sms_forwarding_allowed='full'
                            ):
        settings = deepcopy(self.settings)
        settings.update({
            'shortcode': shortcode,
            'default-template-open-question': default_template_open_question,
            'default-template-closed-question': default_template_closed_question,
            'double-matching-answer-feedback': double_matching_answer_feedback,
            'double-optin-error-feedback': double_optin_error_feedback,
            'credit-type': sms_limit_type,
            'credit-number': sms_limit_number,
            'credit-from-date': sms_limit_from_date,
            'credit-to-date': sms_limit_to_date,
            'sms-forwarding-allowed': sms_forwarding_allowed
        })
        program_settings = []
        for key, value in settings.iteritems():
            program_settings.append({'key': key, 'value': value})
        return program_settings

    def mk_program_settings_international_shortcode(self, shortcode='+318181',
                                                    default_template_open_question=None,
                                                    default_template_closed_question=None):
        settings = deepcopy(self.settings)
        settings['shortcode'] = shortcode
        settings['international-prefix'] = 'all'
        settings['default-template-open-question'] = default_template_open_question
        settings['default-template-closed-question'] = default_template_closed_question
        program_settings = []
        for key, value in settings.iteritems():
            program_settings.append({'key': key, 'value': value})
        return program_settings

    dialogue_announcement = {
        'name': 'test dialogue',
        'activated': 1,
        'auto-enrollment': 'all',
        'dialogue-id': '0',
        'interactions': [
            {'activated': 0,
             'type-interaction': 'announcement',
             'interaction-id': '0',
             'content': 'Hello',
             'type-schedule': 'offset-days',
             'days': '1',
             'at-time': '22:30'},
            {'activated': 0,
             'type-interaction': 'announcement',
             'interaction-id': '1',
             'content': 'How are you',
             'type-schedule': 'offset-days',
             'days': '2',
             'at-time': '22:30'}]}

    def mkobj_dialogue_announcement_offset_days(self):
        dialogue = deepcopy(self.dialogue_announcement)
        return Dialogue(**dialogue).get_as_dict()

    def mkobj_dialogue_announcement_offset_time(self):
        dialogue = deepcopy(self.dialogue_announcement)
        dialogue['created'] = Timestamp(datetime.now(), 0)
        dialogue['modified'] = Timestamp(datetime.now(), 0)
        dialogue['interactions'][0] = {
            'activated': 1,
            'interaction-id': '1',
            'type-interaction': 'announcement',
            'content': 'How are you?',
            'type-schedule': 'offset-time',
            'minutes': '00:10'}        
        dialogue['interactions'][1] = {
            'activated': 1,
            'interaction-id': '2',
            'type-interaction': 'announcement',
            'content': 'How is your day?',
            'type-schedule': 'offset-time',
            'minutes': '10'}
        dialogue['interactions'].append({
            'activated': 1,
            'interaction-id': '3',
            'type-interaction': 'announcement',
            'content': 'Bye bye',
            'type-schedule': 'offset-time',
            'minutes': '50'})
        return Dialogue(**dialogue).get_as_dict()

    def mkobj_dialogue_annoucement(self):
        return deepcopy({
            'activated': 1,
            'auto-enrollment': 'all',
            'name': 'test dialogue',
            'dialogue-id': '0',
            'interactions': [
                {'activated': 0,
                 'type-interaction': 'announcement',
                 'interaction-id': '0',
                 'content': 'Hello',
                 'type-schedule': 'offset-days',
                 'days': '1',
                 'at-time': '22:30',
                 'model-version':'2',
                 'object-type': 'interaction'}],
            'created': Timestamp(datetime.now(), 0),
            'modified': Timestamp(datetime.now(), 0),
        })

    dialogue_annoucement_2 = {
        'name': 'test dialogue',
        'auto-enrollment': None,
        "activated": 1,
        "dialogue-id": "2",
        "interactions": [
            {'activated': 0,
             "type-interaction": "announcement",
             "interaction-id": "0",
             "content": "Hello",
             'type-schedule': 'offset-time',
             'minutes': '1'},
            {'activated': 0,
             "type-interaction": "announcement",
             "interaction-id": "1",
             "content": "Today will be sunny",
             'type-schedule': 'offset-time',
             'minutes': '2'},
            {'activated': 0,
             "type-interaction": "announcement",
             "interaction-id": "2",
             "content": "Today is the special day",
             'type-schedule': 'offset-time',
             'minutes': '3'}]}

    def mkobj_dialogue_announcement_2(self):
        return Dialogue(**self.dialogue_annoucement_2).get_as_dict()
    
    def mkobj_dialogue_announcement_prioritized(self):
        dialogue = deepcopy(self.dialogue_annoucement_2)
        dialogue['interactions'][0]['prioritized'] = '1'
        dialogue['interactions'][1]['prioritized'] = '1'
        return Dialogue(**dialogue).get_as_dict()

    dialogue_question = {
        'name': 'test dialogue',
        'auto-enrollment': None,
        'activated': 1,
        'dialogue-id': '01',
        'interactions': [
            {'activated': 0,
             'interaction-id': '01-01',
             'type-interaction': 'question-answer',
             'type-question': 'closed-question',
             'content': 'How are you?',
             'keyword': 'FEEL, FEL',
             'type-schedule': 'offset-days',
             'set-use-template': 'use-template',
             'days': '1',
             'at-time': '22:30',            
             'answers': [
                 {'choice': 'Fine',
                  'feedbacks': None,
                  'answer-actions': None},
                 {'choice': 'Ok',
                  'feedbacks': [{'content': 'Thank you'}],
                  'answer-actions':[{'type-answer-action': 'enrolling',
                                     'enroll': '2'}]}]}]}

    def mkobj_dialogue_closed_question_index(self):
        return {
            'name': 'test dialogue',
            'auto-enrollment': None,
            'activated': 1,
            'dialogue-id': '01',
            'interactions': [
                {'activated': 0,
                 'interaction-id': '01-01',
                 'type-interaction': 'question-answer',
                 'type-question': 'closed-question',
                 'content': 'What is your choice?',
                 'keyword': 'Choice',
                 'type-schedule': 'offset-days',
                 'set-use-template': 'use-template',
                 'days': '1',
                 'at-time': '22:30',            
                 'answers': [
                     {'choice': '0',
                      'feedbacks': None,
                      'answer-actions': None},
                     {'choice': '1',
                      'feedbacks': None,
                      'answer-actions': None}]}]}        
    
    def mkobj_dialogue_question_max_unmatching(self):
        dialogue = deepcopy(self.dialogue_question)
        dialogue['interactions'][0]['model-version'] = '2'
        dialogue['interactions'][0]['object-type'] = 'interaction'
        dialogue['interactions'][0]['set-max-unmatching-answers'] = 'max-unmatching-answers'
        dialogue['interactions'][0]['max-unmatching-answer-number'] = '5'
        dialogue['interactions'][0]['max-unmatching-answer-actions'] = [{'type-action':'feedback', 'content': 'You reached the limit'}]
        dialogue['interactions'][0]['type-unmatching-feedback'] = 'program-unmatching-feedback'
        dialogue['interactions'][0]['set-reminder'] = 'reminder'
        dialogue['interactions'][0]['type-schedule-reminder'] = 'reminder-offset-time'
        dialogue['interactions'][0]['reminder-minutes'] = '10'
        dialogue['interactions'][0]['reminder-number'] = '2'
        dialogue['interactions'][0]['reminder-actions'] = None
        dialogue['interactions'][0]['label-for-participant-profiling'] = None
        dialogue['interactions'][0]['set-answer-accept-no-space'] = None
        return Dialogue(**dialogue).get_as_dict()

    def mkobj_dialogue_question_offset_days(self):
        return Dialogue(**{
            'activated': 1,
            'dialogue-id': '01',
            'auto-enrollment': None,
            'name': 'test dialogue',
            'interactions': [
                {'activated': 1,
                 'interaction-id': '01-01',
                 'type-interaction': 'question-answer',
                 'content': 'How are you?',
                 'keyword': 'FEEL, FEL',
                 'set-use-template': 'use-template',
                 'type-question': 'closed-question',
                 'type-schedule': 'offset-days',
                 'days': '1',
                 'label-for-participant-profiling': None,
                 'set-answer-accept-no-space': None,
                 'at-time': '22:30',
                 'answers': [
                     {'choice': 'Fine'},
                     {'choice': 'Ok',
                      'feedbacks': [{'content': 'Thank you'}],
                      'answer-actions':[{'type-answer-action': 'enrolling',
                                         'enroll': '2'}]}]}]
        }).get_as_dict()

    def mkobj_dialogue_question_offset_conditional(self):
        dialogue = deepcopy(self.dialogue_question)
        dialogue['created'] = Timestamp(datetime.now(), 0)
        dialogue['modified'] = Timestamp(datetime.now(), 0)
        dialogue['interactions'].append(
            {'activated': 1,
             'interaction-id': '01-02',
             'type-interaction': 'announcement',
             'content': 'Message received',
             'type-schedule': 'offset-condition',
             'offset-condition-interaction-id': '01-01'})
        dialogue['interactions'].append(
            {'activated': 1,
             'interaction-id': '01-03',
             'type-interaction': 'announcement',
             'content': 'Another message',
             'type-schedule': 'offset-condition',
             'offset-condition-interaction-id': '01-01'})
        return dialogue

    dialogue_open_question = {
        'name': 'test dialogue',
        'auto-enrollment': None,
        'activated': 1,
        'dialogue-id': '04',
        'interactions': [
            {'activated': 1,
             'interaction-id': '01-01',
             'type-interaction': 'question-answer',
             'content': 'What is your name?',
             'keyword': 'name',
             'set-use-template': 'use-template',
             'type-question': 'open-question',
             'answer-label': 'name',
             'type-schedule': 'offset-days',
             'feedbacks': None,
             'days': '1',
            'at-time': '22:30'}]}

    def mkobj_dialogue_question_multi_keyword(self):
        return Dialogue(**{
            'activated': 1,
            'name': 'test dialogue',
            'auto-enrollment': None,
            'dialogue-id': '05',
            'interactions': [
                {'activated': 1,
                 'interaction-id': '05',
                 'type-interaction': 'question-answer-keyword',
                 'type-schedule': 'offset-time',
                 'minutes': '15',
                 'content': 'What is your gender?\n male or female',
                 'label-for-participant-profiling': 'gender',
                 "answer-keywords": [
                     {"keyword": "maLe"},
                     {"keyword": "female"}]
                 }]}).get_as_dict()

    def mkobj_dialogue_open_question(self):
        return deepcopy(self.dialogue_open_question)

    def mkobj_dialogue_open_question_enroll_action(self, dialogue_id):
        dialogue = deepcopy(self.dialogue_open_question)
        dialogue['interactions'][0]['answer-actions'] = [
            {'type-answer-action': 'enrolling',
             'enroll': dialogue_id}]
        return dialogue
    
    dialogue_open_question_with_reminder_offset_time = {
        'activated': 1,
        'name': 'test dialogue',
        'auto-enrollment': None,
        'dialogue-id': '04',
        'interactions': [
            {'activated': 1,
             'interaction-id': '01-01',
             'type-interaction': 'question-answer',
             'content': 'How are you?',
             'keyword': 'name',
             'type-question': 'open-question',
             'answer-label': 'name',
             'set-use-template': 'use-template',
             'type-schedule': 'fixed-time',
             'date-time': '2012-03-12T12:30:00',
             'set-reminder': 'reminder',
             'reminder-number': '2',
             'type-schedule-reminder': 'reminder-offset-time',
             'reminder-minutes': '30',
             'reminder-actions': [
                 {'type-action': 'optout'}]}]}

    def mkobj_dialogue_open_question_reminder_offset_time(self):
        dialogue = Dialogue(**deepcopy(self.dialogue_open_question_with_reminder_offset_time))
        return dialogue.get_as_dict()
    
    def mkobj_dialogue_open_question_reminder_offset_days(self):
        dialogue_raw = deepcopy(self.dialogue_open_question_with_reminder_offset_time)
        interaction = {
            'type-schedule-reminder': 'reminder-offset-days',
            'reminder-days': '2',
            'reminder-at-time': '09:00'}
        dialogue_raw['interactions'][0].update(interaction)
        dialogue = Dialogue(**deepcopy(dialogue_raw))
        return dialogue.get_as_dict()
   
    def mkobj_dialogue_open_question_reminder(self):
        dialogue_raw = deepcopy(self.dialogue_open_question_with_reminder_offset_time)
        interaction = {'type-schedule': 'offset-time',
                       'minutes': '3',
                       'set-reminder': 'reminder',
                       'reminder-number': '2',
                       'type-schedule-reminder': 'reminder-offset-time',
                       'reminder-minutes': '3',
                       'reminder-actions': [
                           {'type-action': 'feedback',
                            'content': 'You are going to be optout.'},
                           {'type-action': 'optout'}]}
        dialogue_raw['interactions'][0].update(interaction)
        dialogue = Dialogue(**deepcopy(dialogue_raw))
        return dialogue.get_as_dict()    

    def mkobj_dialogue_open_question_offset_conditional(self):
        dialogue = deepcopy(self.dialogue_open_question)
        dialogue['created'] = Timestamp(datetime.now(), 0)
        dialogue['modified'] = Timestamp(datetime.now(), 0)
        dialogue['interactions'].append(
            {'activated': 1,
             'interaction-id': '01-02',
             'type-interaction': 'announcement',
             'content': 'Message received',
             'type-schedule': 'offset-condition',
             'offset-condition-interaction-id': '01-01'})
        return Dialogue(**dialogue).get_as_dict()

    dialogue_announcement_fixedtime = {
        'activated': 1,
        'name': 'test dialogue',
        'auto-enrollment': None,
        'dialogue-id': '1',
        'interactions': [
            {'activated': 1,
             'interaction-id':'1',
             'type-interaction': 'announcement',
             'content': 'Hello',
             'type-schedule': 'fixed-time',
             'date-time': '2012-03-12T12:30:00'
             }]}

    def mkobj_dialogue_announcement_fixedtime(self):
        dialogue = deepcopy(self.dialogue_announcement_fixedtime)
        return Dialogue(**dialogue).get_as_dict()

    dialogue_question_answer = {
        'dialogue-id': '01',
        'name': 'test dialogue',
        'auto-enrollment': None,
        'activated': 1,
        'interactions': [
            {'activated': 1,
             'interaction-id': '01-01',
             'type-interaction': 'question-answer',
             "content": 'How are you?',
             'keyword': 'FEEL, Fel',
             'type-question': 'closed-question',
             'set-use-template': None,
             'answers': [
                 {'choice': 'Fine',
                  'feedbacks': [
                      {'content':'thank you'},
                      {'content':'thank you again'}]
                  },
                 {'choice': 'Ok'}],
             'type-schedule': 'fixed-time',
             'date-time': '2012-03-12T12:30:00'},
            {'activated': 1,
             'interaction-id': '01-02',
             'type-interaction': 'question-answer',
             "content": 'What is your name?',
             'keyword': 'name',
             'type-question': 'open-question',
             'answer-label': 'name',
             'set-use-template': None,
             'feedbacks': [
                 {'content':'thank you for this answer'}],
             'type-schedule': 'fixed-time',
             'date-time': '2012-03-12T12:30:00'
             }
        ]
    }
    
    def mkobj_interaction_question_answer_nospace(self, keywords='GEN'):
        return Interaction(**{'activated': 1,
             'type-schedule': 'offset-time',
             'minutes': '2',
             "type-interaction": "question-answer",
             "type-question": "closed-question",
             "set-use-template": "use-template",
             "content": "What is your gender?",
             'label-for-participant-profiling': 'gender',
             "keyword": keywords,
             "set-answer-accept-no-space": "answer-accept-no-space",
             "answers": [
                 {"choice": "Mâle"},
                 {"choice": "Bad"}],
             "interaction-id": "script.dialogues[0].interactions[2]"
             }).get_as_dict()

    def mkobj_interaction_question_multikeyword(self):
        return Interaction(**{
            'activated': 1,
            'interaction-id': '05',
            'type-interaction': 'question-answer-keyword',
            'type-schedule': 'offset-time',
            'minutes': '15',
            'content': 'What is your gender?\n male or female',
            'label-for-participant-profiling': 'gender',
            "answer-keywords": [
                {"keyword": "maLe"},
                {"keyword": "fÉmale"}]}).get_as_dict()

    def mkobj_interaction_question_answer(self):
        return Interaction(**{
            'activated': 1,
            'type-schedule': 'offset-time',
            'minutes': '2',
            "type-interaction": "question-answer",
            "type-question": "closed-question",
            "content": "How are you [participant.name]?",
            "keyword": "Feel",
            "set-use-template": "use-template",
            "type-reminder": "no-reminder",
            "type-question": "closed-question",
            "answers": [
                {"choice": "Good",
                 "feedbacks": [
                     {"content": "So have a nice day [participant.name]"}]},
                {"choice": "Bâd",
                 "feedbacks": [
                     {"content": "Come one [participant.name], you can get over it!"}]}
                ],
            "interaction-id": "script.dialogues[0].interactions[0]"
        }).get_as_dict()
    
    def mkobj_dialogue_question_answer(self):
        return Dialogue(**deepcopy(self.dialogue_question_answer)).get_as_dict()

    dialogue_other_question_answer = {
        "name": "something",
        'auto-enrollment': None,
        'activated': 1,
        "interactions": [
            {'activated': 1,
             'type-schedule': 'offset-time',
             'minutes': '2',
             "type-interaction": "question-answer",
             "type-question": "closed-question",
             "content": "How are you [participant.name]?",
             "keyword": "Fool",
             "set-use-template": "use-template",
             "type-reminder": "no-reminder",
             "type-question": "closed-question",
             "answers": [
                 {"choice": "Good",
                  "feedbacks": [
                      {"content": "So have a nice day [participant.name]"}
                  ]
                  },
                 {"choice": "Bad",
                  "feedbacks": [
                      {"content": "Come one [participant.name], you can get over it!"}]}
                 ],
             "interaction-id": "script.dialogues[0].interactions[0]"
             },
            {'activated': 1,
             'type-schedule': 'offset-time',
             'minutes': '2',
             "type-interaction": "question-answer",
             "type-question": "closed-question",
             "set-use-template": "use-template",
             "content": "What is your gender?",
             'label-for-participant-profiling': 'gender',
             "keyword": "GEN",
             "answers": [
                 {"choice": "Male"},
                 {"choice": "Bad"}],
             "interaction-id": "script.dialogues[0].interactions[2]"
             },
            ],
        "dialogue-id": "script.dialogues[0]"
    }

    def mkobj_dialogue_answer_not_space_supported(self):
        dialogue = deepcopy(self.dialogue_other_question_answer)
        dialogue['interactions'][1]['set-answer-accept-no-space'] = 'answer-accept-no-space'
        return dialogue

    def mkobj_request_join(self):
        return Request(**{
            'keyword': 'www join, www',
            'responses': [
                {'content': 'thankyou of joining'},
                {'content': 'soon more is coming'}],
            'actions': [
                {'type-action': 'optin'},
                {'type-action': 'enrolling',
                 'enroll': '01'}],
            'object-type': 'request',
            'model-version': '1'}).get_as_dict()
  
    def mkobj_request_tag(self):
        return Request(**{
            'keyword': 'www tagme',
            'actions': [
                {'type-action': 'tagging',
                 'tag': 'onetag'}],
            'responses': [ 
                {'content': 'you are tagged'}],
            'object-type': 'request',
            'model-version': '1'}).get_as_dict()

    def mkobj_request_reponse_lazy_matching(self, keyword):
        request = self.mkobj_request_response(keyword)
        request['set-no-request-matching-try-keyword-only'] = 'no-request-matching-try-keyword-only'
        return request

    def mkobj_request_response(self, keyword='www info'):
        return Request(**{
            'keyword': keyword,
            'responses': [
                {'content': 'a response'}],
            'actions': [],
            'object-type': 'request',
            'model-version': '1'}).get_as_dict()

    def mkobj_request_leave(self):
        return Request(**{
            'keyword': 'www quit, Quit, quit now, quitnow',
            'responses': [],
            'actions': [
                {'type-action': 'optout'}],
            'object-type': 'request',
            'model-version': '1'}).get_as_dict()

    def mkobj_unattach_message_1(self, recipient='all participants',
                               content='Hello everyone',
                               type_schedule='fixed-time',
                               fixed_time='2100-03-12T12:30:00'):
        return UnattachMessage(**{
            'name': 'test',
            'to': recipient,
            'content': content,
            'type-schedule': type_schedule,
            'fixed-time': fixed_time}).get_as_dict()
    
    def mkobj_unattach_message_2(self, recipient=['all-participants'],
                                 content='Hello everyone',
                                 type_schedule='fixed-time',
                                 fixed_time='2100-03-12T12:30:00'):
        return UnattachMessage(**{
            'object-type': 'unattached-message',
            'model-version': '2',
            'name': 'test',
            'to': recipient,
            'content': content,
            'type-schedule': type_schedule,
            'fixed-time': fixed_time}).get_as_dict()
    
    def mkobj_unattach_message(self,
                               send_to_type='all',
                               send_to_match_operator=None,
                               send_to_match_conditions=None,
                               send_to_phone=None,
                               content='Hello everyone',
                               type_schedule='fixed-time',
                               fixed_time='2100-03-12T12:30:00'):
        unattach_message = {
                'object-type': 'unattached-message',
                'model-version': '3',
                'name': 'test',
                'send-to-type': send_to_type,
                'content': content,
                'type-schedule': type_schedule,
                'fixed-time': fixed_time}
        if (send_to_type=='match'):
            unattach_message.update({
                'send-to-match-operator': send_to_match_operator,
                'send-to-match-conditions': send_to_match_conditions,
            })
        elif (send_to_type=='phone'):
            unattach_message.update({
                'send-to-phone': send_to_phone
            })
        return UnattachMessage(**unattach_message).get_as_dict()    

    template_closed_question = {
        'name': 'my template',
        'type-question': 'closed-question',
        'template': 'QUESTION\r\nANSWERS To reply send: KEYWORD<space><AnswerNb> to SHORTCODE'}

    template_open_question = {
        'name': 'my other template',
        'type-question': 'open-question',
        'template': 'QUESTION\r\n To reply send: KEYWORD<space><ANSWER> to SHORTCODE'}

    template_unmatching_answer = {
        'name': 'unmatching answers template',
        'type-action': 'unmatching-answer',
        'template': 'ANSWER does not match any answer'}

    def mkobj_history_unattach(self, unattach_id, timestamp,
                               participant_phone='06',
                               participant_session_id="1", 
                               message_direction='outgoing',
                               message_status='delivered',
                               message_id='1',
                               message_credits=1):
        return history_generator(**{
            'timestamp': timestamp,
            'participant-phone': participant_phone,
            'participant-session-id': participant_session_id,
            'message-direction': message_direction,
            'message-status': message_status,
            'message-id': message_id,
            'message-content': 'A message',
            'message-credits': message_credits,
            'unattach-id': unattach_id}).get_as_dict()

    def mkobj_history_request(self, 
                              request_id, 
                              timestamp,
                              participant_phone='06',
                              participant_session_id="1", 
                              message_direction='outgoing',
                              message_status='delivered',
                              message_id='1',
                              message_credits=1):
        return history_generator(**{
            'timestamp': timestamp,
            'participant-phone': participant_phone,
            'participant-session-id': participant_session_id,
            'message-direction': message_direction,
            'message-status': message_status,
            'message-id': message_id,
            'message-content': 'A message',
            'message-credits': message_credits,
            'request-id': request_id}).get_as_dict()    

    def mkobj_history_dialogue(self, dialogue_id, interaction_id,
                               timestamp, participant_phone='06',
                               participant_session_id="1", direction='outgoing',
                               matching_answer=None, message_content='', 
                               message_credits=1, message_status='delivered',
                               forwards=None, message_id='1'):
        history = {
            'timestamp': timestamp,
            'participant-phone': participant_phone,
            'participant-session-id': participant_session_id,
            'message-id': message_id,
            'message-direction': direction,
            'message-status': message_status,
            'message-content': message_content,
            'message-credits': message_credits,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id,
            'matching-answer': matching_answer
        }
        if forwards is not None:
            history.update({'forwards': forwards})
        return history_generator(**history).get_as_dict()

    def mkobj_history_one_way_marker(self, dialogue_id, interaction_id, timestamp,
                                     participant_phone='06', participant_session_id='1'):
        return history_generator(**{
            'object-type': 'oneway-marker-history',
            'participant-phone': participant_phone,
            'participant-session-id':participant_session_id,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id,
            'timestamp': timestamp}).get_as_dict()

    def mkobj_history_dialogue_open_question(self, dialogue_id, interaction_id,
                               timestamp, participant_phone='06',
                               participant_session_id="1", direction='outgoing'):
        return history_generator(**{
            'timestamp': timestamp,
            'participant-phone': participant_phone,
            'participant-session-id': participant_session_id,
            'message-direction': direction,
            'message-status': 'delivered',
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id}).get_as_dict()

    def mkobj_participant(self, participant_phone='06',
                          last_optin_date='2012-02-01T18:30', session_id='1',
                          enrolled=[], tags=[], profile=[],
                          transport_metadata={}):
        return Participant(**{ 
            'phone': participant_phone,
            'session-id': session_id,
            'last-optin-date': last_optin_date,            
            'enrolled': enrolled,
            'tags': tags,
            'profile': profile,
            'transport_metadata': transport_metadata}).get_as_dict()
    
    def mkobj_participant_v2(self, participant_phone='06',
                          last_optin_date='2012-02-01T18:30', session_id='1',
                          enrolled=[], tags=[], profile=[]):
        return Participant(**{
            'object-type': 'participant',
            'model-version': '2',
            'phone': participant_phone,
            'session-id': session_id,
            'last-optin-date': last_optin_date,            
            'enrolled': enrolled,
            'tags': tags,
            'profile': profile}).get_as_dict()    

    def mkobj_schedule(self, participant_phone='06',
                       participant_session_id='1',
                       date_time='2116-12-06T16:24:12',
                       object_type='dialogue-schedule',
                       dialogue_id='1',interaction_id='2'):
        return schedule_generator(**{
            'model-version': '2',
            'participant-phone': participant_phone,
            'participant-session-id': participant_session_id,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id,
            'object-type': object_type,
            'date-time': date_time
            }).get_as_dict()

    def mkobj_schedule_unattach(self, participant_phone='06',
                                participant_session_id='1',
                                date_time='2013-12-20T08:00:00',
                                unattach_id='1'):
        return schedule_generator(**{
            'object-type': 'unattach-schedule',
            'model-version': '2',
            'participant-phone': participant_phone,
            'participant-session-id': participant_session_id,
            'unattach-id': unattach_id,
            'date-time': date_time
            }).get_as_dict()

    def mkobj_schedule_feedback(self, participant_phone='06',
                                participant_session_id='1',
                                date_time=None,
                                content='Thank You',
                                context={}):
        schedule = {
            'object-type': 'feedback-schedule',
            'model-version': '2',
            'participant-phone': participant_phone,
            'participant-session-id': participant_session_id,
            'date-time': date_time,
            'content': content,
            'context': context}
        return schedule_generator(**schedule).get_as_dict()
    
    def mkobj_content_variables_three_keys(self, key1='mombasa', key2='chicken',
                                           key3='price', value='30 C'):
            return {'keys':[{'key' : key1},
                            {'key' : key2},
                            {'key': key3}],
                    'value' : value}    
    
    def mkobj_content_variables_two_keys(self, key1='program', key2='weather', value='30 C'):
        return {'keys':[{'key' : key1},
                        {'key' : key2}],
                'value' : value}
    
    def mkobj_content_variables_one_key(self, key1='temperature', value='100 C'):
        return {'keys':[{'key' : key1}],
                'value' : value}

    def mk_content(self, length=160, keyword=None):
        content = keyword if keyword is not None else ""
        template = " This is a %s char message." % length
        while len(content) < length:
            content = content + template
        return content[0:length]

