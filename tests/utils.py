from copy import deepcopy
from datetime import datetime
import time
from copy import deepcopy

from bson.timestamp import Timestamp

#from vumi.tests.utils import UTCNearNow
from vumi.message import (TransportEvent, TransportMessage,
                          TransportUserMessage, Message)
from vumi.transports.failures import FailureMessage
from vumi.tests.utils import RegexMatcher, UTCNearNow

from vusion.message import DispatcherControl, WorkerControl

from vusion.persist import (Dialogue, DialogueHistory, UnattachHistory,
                            history_generator, schedule_generator)


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
                timestamp=datetime.now(),
                transport_name=self.transport_name,
                transport_metadata=transport_metadata,
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
            sent_message_id=sent_message_id,
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
    }

    def mkobj_shortcode(self, error_template=None, code='8181'):
        shortcode = deepcopy(self.shortcodes)
        shortcode['error-template'] = error_template
        shortcode['shortcode'] = code
        return shortcode

    def mkobj_shortcode_international(self):
        shortcode = deepcopy(self.shortcodes)
        shortcode['country'] = 'Netherlands'
        shortcode['international-prefix'] = '31'
        shortcode['shortcode'] = '+318181'
        shortcode['supported-internationally'] = 1
        return shortcode

    def mkobj_shortcode_local(self):
        return deepcopy(self.shortcodes)

    time_format = '%Y-%m-%dT%H:%M:%S'

    simple_config = {
        'database_name': 'test',
        'dispatcher': 'dispatcher',
        'transport_name': 'app'}

    program_settings = [
        {'key': 'shortcode',
         'value': '256-8181'},
        {'key': 'international-prefix',
         'value': '256'},
        {'key': 'timezone',
         'value': 'Africa/Kampala'},
        {'key': 'customized-id',
         'value': 'myid'}]

    def mkobj_program_settings(self):
        program_settings = deepcopy(self.program_settings)
        return program_settings

    def mkobj_program_settings_international_shortcode(self):
        program_settings = deepcopy(self.program_settings)
        program_settings[2] = {'key': 'shortcode',
                               'value': '+318181'}
        program_settings[1] = {'key': 'international-prefix',
                               'value': 'all'}
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
            'activated': 0,
            'interaction-id': '2',
            'type-interaction': 'announcement',
            'content': 'How is your day?',
            'type-schedule': 'offset-time',
            'minutes': '10'}
        dialogue['interactions'][1] = {
            'activated': 0,
            'interaction-id': '3',
            'type-interaction': 'announcement',
            'content': 'Bye bye',
            'type-schedule': 'offset-time',
            'minutes': '50'}
        return Dialogue(**dialogue).get_as_dict()

    def mkobj_dialogue_annoucement(self):
        return {
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
        }

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
        dialogue['interactions'][0]['set-max-unmatching-answers'] = 'max-unamatching-answers'
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
    
    dialogue_open_question_with_reminder = {
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

    def mkobj_dialogue_open_question_reminder(self):
        dialogue = Dialogue(**deepcopy(self.dialogue_open_question_with_reminder))
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
        return {
            'keyword': 'www join, www',
            'responses': [
                {'content': 'thankyou of joining'},
                {'content': 'soon more is coming'}],
            'actions': [
                {'type-action': 'optin'},
                {'type-action': 'enrolling',
                 'enroll': '01'}],
            'object-type': 'request',
            'model-version': '1'}

    request_tag = {
        'keyword': 'www tagme',
        'actions': [
            {'type-action': 'tagging',
             'tag': 'onetag'}],
        'responses': [ 
            {'content': 'you are tagged'}],
        'object-type': 'request',
        'model-version': '1'}
    
    def mkobj_request_tag(self):
        return deepcopy(self.request_tag)

    def mkobj_request_reponse_lazy_matching(self, keyword):
        request = self.mkobj_request_response(keyword)
        request['set-no-request-matching-try-keyword-only'] = 'no-request-matching-try-keyword-only'
        request['model-version'] = '2'
        return request

    def mkobj_request_response(self, keyword='www info'):
        return {
            'keyword': keyword,
            'responses': [
                {'content': 'a response'}],
            'actions': [],
            'object-type': 'request',
            'model-version': '1'}

    request_leave = {
        'keyword': 'www quit, Quit, quit now, quitnow',
        'responses': [],
        'actions': [
            {'type-action': 'optout'}],
        'object-type': 'request',
        'model-version': '1'}

    def mkobj_request_leave(self):
        return deepcopy(self.request_leave)

    def mkobj_unattach_message(self, recipient='all participants',
                               content='Hello everyone',
                               type_schedule='fixed-time',
                               fixed_time='2100-03-12T12:30:00'):
        return {
            'to': recipient,
            'content': content,
            'type-schedule': type_schedule,
            'fixed-time': fixed_time}

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
                               participant_session_id="1"):
        return history_generator(**{
            'timestamp': timestamp,
            'participant-phone': participant_phone,
            'participant-session-id': participant_session_id,
            'message-direction': 'outgoing',
            'message-status': 'delivered',
            'message-id': '1',
            'message-content': 'A message',
            'unattach-id': unattach_id}).get_as_dict()

    def mkobj_history_dialogue(self, dialogue_id, interaction_id,
                               timestamp, participant_phone='06',
                               participant_session_id="1", direction='outgoing',
                               matching_answer=None, message_content=''):
        return history_generator(**{
            'timestamp': timestamp,
            'participant-phone': participant_phone,
            'participant-session-id': participant_session_id,
            'message-id': '1',
            'message-direction': direction,
            'message-status': 'delivered',
            'message-content': message_content,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id,
            'matching-answer': matching_answer}).get_as_dict()

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
                          enrolled=[], tags=[], profile=[]):
        return { 
            'phone': participant_phone,
            'session-id': session_id,
            'last-optin-date': last_optin_date,            
            'enrolled': enrolled,
            'tags': tags,
            'profile': profile}

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
                                date_time=None,
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
