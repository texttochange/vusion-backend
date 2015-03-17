import re
from copy import copy
from datetime import timedelta, datetime, time

from vusion.utils import clean_keyword, get_first_msg_word, clean_msg
from vusion.persist import Model
from vusion.error import InvalidField, MissingField
from vusion.persist.action import (action_generator, FeedbackAction,
                                   UnMatchingAnswerAction, ProfilingAction,
                                   RemoveDeadlineAction, RemoveQuestionAction,
                                   RemoveRemindersAction, Actions)
from vusion.utils import (time_from_vusion_format, time_to_vusion_format,
                          get_default)


class Interaction(Model):
    
    MODEL_TYPE = 'interaction'
    MODEL_VERSION = '6'
    
    fields = {
        'interaction-id': {
            'required': True,
            },
        'type-schedule': {
            'required': True,
            'valid_value': lambda v: v['type-schedule'] in [
                'fixed-time',
                'offset-time',
                'offset-days',
                'offset-condition'],
            'required_subfield': lambda v: getattr(v, 'required_subfields') (
                v['type-schedule'],
                {'fixed-time':['date-time'],
                 'offset-time': ['minutes'],
                 'offset-days': ['days', 'at-time'],
                 'offset-condition': ['offset-condition-interaction-id', 
                                      'offset-condition-delay']})
            },
        'type-interaction': {
            'required': True,
            'valid_value': lambda v: v['type-interaction'] in [
                'announcement',
                'question-answer',
                'question-answer-keyword'],
            'required_subfield': lambda v: getattr(v, 'required_subfields') (
                v['type-interaction'],
                {'announcement': ['content',
                                  'announcement-actions'],
                 'question-answer': ['content',
                                     'keyword',
                                     'set-use-template',
                                     'type-question',
                                     'set-matching-answer-actions',
                                     'set-max-unmatching-answers',
                                     'type-unmatching-feedback',
                                     'set-reminder'],
                 'question-answer-keyword': ['content',
                                             'label-for-participant-profiling',
                                             'answer-keywords',
                                             'set-reminder']})
            },
        'activated': {
            'required': True,
            },
        'prioritized': {
            'required': True,
            },
        'question-type': {
            'required': False,
            'valid_value': lambda v: v['question-type'] in [
                'closed-question',
                'open-question'],
            'required_subfield': lambda v: getattr(v, 'required_subfields') (
                v['question-type'],
                {'closed-question': ['label-for-participant-profiling',
                                     'set-answer-accept-no-space',
                                     'answers'],
                 'open-question': ['answer-label',
                                   'feedbacks']})
            },
        'set-matching-answer-actions': {
            'required': False,
            'valid_value': lambda v: v['set-matching-answer-action'] in [
                None,
                'matching-answer-actions'],
            },
        'matching-answer-actions': {
            'required': False,
            'valid_actions': lambda v: Interaction.validate_actions(v['matching-answer-actions'])
            },
        'type-unmatching-feedback': {
            'required': False,
            'value_value': lambda v: v['type-unmatching-feedback'] in [
                'no-unmatching-feedback',
                'program-unmatching-feedback',
                'interaction-unmatching-feedback']
            },
        'set-max-unmatching-answers': {
            'required': False,
            'value_value': lambda v: v['set-max-unmatching-answers'] in [
                None, 
                '', 
                'max-unmatching-answers']},
        'answers': {
            'required': False,
            'valid_type': lambda v: isinstance(v['answers'], list),
            'valid_answers': lambda v: getattr(v, 'validate_answers') (v['answers'])
            },
        'answer-keywords': {
            'required': False,
            'valid_type': lambda v: isinstance(v['answer-keywords'], list),
            'valid_answer-keywords': lambda v: getattr(v, 'validate_answer_keywords') (v['answer-keywords'])
            },
        'content': {
            'required': False,
            'is_not_null': lambda v: v['content'] is not None
            },
        'keyword': {
            'required': False,
            'is_not_null': lambda v: v['keyword'] is not None
            },
        'feedbacks': {
            'required': False,
            'valid_type': lambda v: isinstance(v['feedbacks'], list)
            },
        'max-unmatching-answer-number': {
            'required': False,
            'valid_value': lambda v: v['max-unmatching-answer-number']>=1
        },
        'max-unmatching-answer-actions': {
            'required': False,
            'valid_actions': lambda v: Interaction.validate_actions(v['max-unmatching-answer-actions'])
        },
        'set-reminder': {
            'required': False,
            'valid_value': lambda v: v['set-reminder'] in [
                None,
                '',
                'reminder'],
            'valid_subfields': lambda v: getattr(v, 'required_subfields') (
                v['set-reminder'],
                {'reminder': [
                    'type-schedule-reminder',
                    'reminder-number',
                    'reminder-actions']})
            },
        'type-schedule-reminder': {
            'required': False,
            'valid_value': lambda v: v['type-schedule-reminder'] in [
                'reminder-offset-days',
                'reminder-offset-time'],
            'valid_subfields': lambda v: getattr(v, 'required_subfields') (
                v['type-schedule-reminder'],
                {'reminder-offset-days': ['reminder-days',
                                          'reminder-at-time'],
                 'reminder-offset-time': ['reminder-minutes']})
            },
        'reminder-days': {
            'required': False,
            'valid_value': lambda v: v['reminder-days'] >=1,
            },
        'reminder-at-time':{
            'required': False,
            'valid_value': lambda v: v['reminder-at-time'] is not None
            },
        'reminder-minutes': {
            'required': False,
            'valid_value': lambda v: v['reminder-minutes']>=1
            },
        'reminder-number': {
            'required': False,
            'valid_value': lambda v: v['reminder-number'] >=1
            },
        'reminder-actions': {
            'required': False,
            'valid_actions': lambda v: Interaction.validate_actions(v['reminder-actions'])
            },
        'announcement-actions': {
            'required': False,
            'valid_actions': lambda v: Interaction.validate_actions(v['announcement-actions'])
        }
    }

    answer_fields = {
        'choice': {
            'required': True,
            'valid_value': lambda v: v['choice'] is not None,
            },
        'feedbacks': {
            'required': True,
            'valid_value': lambda v: isinstance(v['feedbacks'], list)
            },
        'answer-actions': {
            'required': True,
            'valid_actions': lambda v: Interaction.validate_actions(v['answer-actions'])
        },
    }
    
    answer_keyword_fields = {
        'keyword': {
            'required': True,
            'valid_value': lambda v: v['keyword'] is not None,
            },
        'feedbacks': {
            'required': True,
            'valid_value': lambda v: isinstance(v['feedbacks'], list),
            },
        'answer-actions': {
            'required': True,
            'valid_actions': lambda v: Interaction.validate_actions(v['answer-actions'])
        }
    }

    FIELD_DEFAULT_VALUE = {
        'feedbacks': [],
        'answers': [],
        'answer-keywords': [],
        'answer-actions': [],
        'max-unmatching-answer-actions': [],
        'reminder-actions':[]
        }
    
    def __init__(self, **kwargs):
        super(Interaction, self).__init__(**kwargs)
        self.keywords = self._get_keywords()
    
    @staticmethod
    def validate_actions(actions):
        if not isinstance(actions, list):
            return False
        for action in actions:
            action_generator(**action)
        return True

    def validate_answers(self, answers):
        for answer in answers:
            self._validate(answer, self.answer_fields)
        return True
    
    def validate_answer_keywords(self, answer_keywords):
        for answer_keyword in answer_keywords:
            self._validate(answer_keyword, self.answer_keyword_fields)
        return True

    def _modify_none_to_default(self, obj):
        for key in obj:
            if isinstance(obj[key], list):
                for elt in obj[key]:
                    self._modify_none_to_default(elt)
            if isinstance(obj[key], dict):
                self._modify_none_to_default(obj[key])
            if key in self.FIELD_DEFAULT_VALUE and obj[key] is None:
                obj[key] = self.FIELD_DEFAULT_VALUE[key]

    def before_validate(self):
        #fix mongodb that change [] into None
        self._modify_none_to_default(self.payload)

    def validate_fields(self):
        self._validate(self, self.fields)

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            if (kwargs['type-interaction'] == 'question-answer' or 
                    kwargs['type-interaction'] == 'question-answer-keyword'):
                if kwargs['type-interaction'] == 'question-answer':
                    kwargs['type-unmatching-feedback'] = 'program-unmatching-feedback'
                    kwargs['set-max-unmatching-answers'] = None
                kwargs['set-reminder'] = kwargs['set-reminder'] if 'set-reminder' in kwargs else None
                kwargs['set-use-template'] = kwargs['set-use-template'] if 'set-use-template' in kwargs else None
                if ('type-question' in kwargs and 
                        kwargs['type-question'] == 'closed-question'):
                    kwargs['label-for-participant-profiling'] = kwargs['label-for-participant-profiling'] if 'label-for-participant-profiling' in kwargs else None
                    kwargs['set-answer-accept-no-space'] = kwargs['set-answer-accept-no-space'] if 'set-answer-accept-no-space' in kwargs else None
                if ('type-question' in kwargs and 
                        kwargs['type-question'] == 'open-question'):
                    kwargs['feedbacks'] = kwargs['feedbacks'] if 'feedbacks' in kwargs else None
                if (kwargs['type-interaction'] == 'question-answer-keyword'):
                    kwargs['label-for-participant-profiling'] = kwargs['label-for-participant-profiling'] if 'label-for-participant-profiling' in kwargs else None
                if kwargs['type-interaction'] == 'question-answer-keyword':
                    kwargs['answer-keywords'] = kwargs['answer-keywords'] if 'answer-keywords' in kwargs else None
                    for answer in kwargs['answer-keywords']:
                        answer['feedbacks'] = answer['feedbacks'] if 'feedbacks' in answer else None
                        answer['answer-actions'] = answer['answer-actions'] if 'answer-actions' in answer else None
                if kwargs['type-interaction'] == 'question-answer' and kwargs['type-question'] == 'closed-question':
                    kwargs['answers'] = kwargs['answers'] if 'answers' in kwargs else None
                    for answer in kwargs['answers']:
                        answer['feedbacks'] = answer['feedbacks'] if 'feedbacks' in answer else None
                        answer['answer-actions'] = answer['answer-actions'] if 'answer-actions' in answer else None
            kwargs['model-version'] = '2'
            return self.upgrade(**kwargs)
        elif kwargs['model-version'] == '2':
            kwargs['prioritized'] = kwargs['prioritized'] if 'prioritized' in kwargs else None
            kwargs['model-version'] = '3'
            return self.upgrade(**kwargs)
        elif kwargs['model-version'] == '3':
            kwargs['set-matching-answer-actions'] = get_default(kwargs, 'set-matching-answer-actions', None)
            kwargs['model-version'] = '4'
            return self.upgrade(**kwargs)
        elif kwargs['model-version'] == '4':
            if kwargs['type-schedule'] == 'offset-condition':
                kwargs['offset-condition-delay'] = get_default(kwargs, 'offset-condition-delay', 0)
            kwargs['model-version'] = '5'
            return self.upgrade(**kwargs)
        elif kwargs['model-version'] == '5':
            if kwargs['type-interaction'] == 'announcement':
                kwargs['announcement-actions'] = get_default(
                    kwargs, 'announcement-actions', [])
            kwargs['model-version'] = '6'
        return kwargs

    def has_reminder(self):
        if 'set-reminder' not in self.payload:
            return False
        return self.payload['set-reminder'] == 'reminder'

    def is_prioritized(self):
        if 'prioritized' not in self.payload:
            return False
        return self['prioritized']

    def is_matching(self, keyword):
        return keyword in self.keywords

    def is_multi_keyword(self):
        if 'answer-keywords' in self.payload:
            return True
        return False

    def is_open_question(self):
        if 'type-question' in self.payload and self['type-question'] == 'open-question':
            return True
        return False

    def is_closed_question(self):
        if 'type-question' in self.payload and self['type-question'] == 'closed-question':
            return True
        return False

    def generate_reminder_times(self, interaction_date_time):
        if not self.has_reminder:
            return None
        generate_times = []
        generate_number = int(self['reminder-number']) + 1
        if (self['type-schedule-reminder'] == 'reminder-offset-time'):
            running_date_time = interaction_date_time
            for number in range(0, generate_number):            
                running_date_time += timedelta(minutes=int(self['reminder-minutes']))
                generate_times.append(running_date_time)
        elif (self['type-schedule-reminder'] == 'reminder-offset-days'):
            sending_day = interaction_date_time
            sending_time = self['reminder-at-time'].split(':', 1)                            
            for number in range(0, generate_number):                        
                sending_day += timedelta(days=int(self['reminder-days']))
                sending_date_time = datetime.combine(sending_day, time(int(sending_time[0]), int(sending_time[1])))
                generate_times.append(sending_date_time)
        return generate_times

    def get_reminder_times(self, interaction_date_time):
        times = self.generate_reminder_times(interaction_date_time)
        return times[:-1] if times is not None else None

    def get_deadline_time(self, interaction_date_time):
        times = self.generate_reminder_times(interaction_date_time)
        return times[-1] if times is not None else None

    def has_max_unmatching_answers(self):
        if 'set-max-unmatching-answers' not in self.payload:
            return False
        return self.payload['set-max-unmatching-answers'] == 'max-unmatching-answers'

    def get_unmatching_action(self, answer, actions):
        # case of question-answer-keyword
        if 'type-unmatching-feedback' not in self.payload:
            return
        if self.payload['type-unmatching-feedback'] == 'interaction-unmatching-feedback':
            actions.append(FeedbackAction(**{'content': self.payload['unmatching-feedback-content']}))
        elif self.payload['type-unmatching-feedback'] == 'program-unmatching-feedback':
            if answer is None:
                answer = ''
            actions.append(UnMatchingAnswerAction(**{'answer': answer}))

    def get_max_unmatching_action(self, dialogue_id, actions):
        if self.has_reminder():
            actions.append(RemoveRemindersAction(**{
                'dialogue-id': dialogue_id,
                'interaction-id': self.payload['interaction-id']}))
            actions.append(RemoveDeadlineAction(**{
                'dialogue-id': dialogue_id,
                'interaction-id': self.payload['interaction-id']}))
        for action in self.payload['max-unmatching-answer-actions']:
            actions.append(action_generator(**action))

    def get_interaction_keywords(self):
        keywords = self.split_keywords(self.payload['keyword'])
        if (not 'set-answer-accept-no-space' in self.payload 
                or self.payload['set-answer-accept-no-space'] is None):
            return keywords
        generated_answer = copy(keywords)
        for answer in self.payload['answers']:
            generated_answer += self.get_answer_keywords(keywords, answer)
        return generated_answer

    def split_keywords(self, keywords):
        return [clean_keyword(k) for k in (keywords or '').split(', ')]

    def get_answer_keywords(self, keywords, answer):
        return [clean_keyword("%s%s" % (keyword, answer['choice'].replace(" ",""))) for keyword in keywords]

    def get_actions_from_matching_answer(self, dialogue_id, matching_answer, matching_value, actions):
        if self.is_open_question():
            return actions
        for feedback in matching_answer['feedbacks']:
            action = FeedbackAction(**{'content': feedback['content']})
            actions.append(action)
        if 'answer-actions' in matching_answer:
            for matching_answer_action in matching_answer['answer-actions']:
                actions.append(action_generator(**matching_answer_action))
        return actions

    def get_actions_from_interaction(self, dialogue_id, matching_value, actions):
        if self.has_reminder():
            actions.append(RemoveRemindersAction(**{
                'dialogue-id': dialogue_id,
                'interaction-id': self.payload['interaction-id']}))
            actions.append(RemoveDeadlineAction(**{
                'dialogue-id': dialogue_id,
                'interaction-id': self.payload['interaction-id']}))
        if self.has_closed_question_profiling():
            action = ProfilingAction(**{
                'label': self.payload['label-for-participant-profiling'],
                'value': matching_value})
            actions.append(action)
        elif self.has_open_question_profiling():
            action = ProfilingAction(**{
                'label': self.payload['answer-label'],
                'value': matching_value})
            actions.append(action)            
        if self.has_feedbacks():
            for feedback in self.payload['feedbacks']:
                action = FeedbackAction(**{'content': feedback['content']})
                actions.append(action)
        if self.has_matching_answer_actions():
            for matching_answer_action in self['matching-answer-actions']:
                actions.append(action_generator(**matching_answer_action))

    def has_closed_question_profiling(self):
        return ('label-for-participant-profiling' in self.payload 
                and not self['label-for-participant-profiling'] in [None, ''])

    def has_open_question_profiling(self):
        return 'answer-label' in self.payload and not self['answer-label'] in [None, '']

    def has_feedbacks(self):
        return 'feedbacks' in self.payload and self['feedbacks'] is not None
    
    def has_matching_answer_actions(self):
        return self['set-matching-answer-actions'] == 'matching-answer-actions'

    def get_reply(self, content, delimiter=' '):
        content = clean_msg(content)
        return content.partition(delimiter)[2]

    def get_actions(self, dialogue_id, msg, msg_keyword, reference_metadata, actions):
        actions.append(RemoveQuestionAction(**{
            'dialogue-id': dialogue_id,
            'interaction-id': self.payload['interaction-id']}))
        string_answer, answer = self.get_matching_answer(msg_keyword, msg)
        if answer is None:
            self.get_unmatching_action(string_answer, actions)
        else:
            reference_metadata['matching-answer'] = string_answer
            self.get_actions_from_interaction(
                dialogue_id,
                reference_metadata['matching-answer'],
                actions)
            self.get_actions_from_matching_answer(
                dialogue_id,
                answer,
                reference_metadata['matching-answer'],
                actions)
        return reference_metadata, actions

    def get_matching_answer(self, keyword, msg):
        ## multikeyword question
        if 'answer-keywords' in self.payload:
            multikeyword_answer = self.get_matching_answer_multikeyword(
                self.payload['answer-keywords'], keyword)
            if multikeyword_answer is not None:
                return multikeyword_answer['keyword'], multikeyword_answer
        ## close question
        elif 'answers' in self.payload:
            msg_reply = clean_keyword(self.get_reply(msg))
            answer = self.get_matching_answer_closed_question(keyword, msg_reply)
            if answer is not None:
                return answer['choice'], answer
        ## open question
        else:
            msg_reply = self.get_reply(msg)
            if msg_reply != '':
                return msg_reply, True
        return msg, None

    def get_matching_answer_multikeyword(self, answer_keywords, msg_keyword):
        for answer_keyword in answer_keywords:
            if msg_keyword in self.split_keywords(answer_keyword['keyword']):
                return answer_keyword
        return None

    def get_matching_answer_closed_question(self, keyword, reply):
        answers = self.payload['answers']
        if self.payload['set-answer-accept-no-space'] is not None:
            keywords = self.split_keywords(self.payload['keyword'])
            for answer in answers:
                if keyword in self.get_answer_keywords(keywords, answer):
                    return answer
        if reply is None:
            return None
        for answer in answers:
            regex_CHOICE = re.compile(("^%s(\s|$)" % clean_keyword(answer['choice'])))
            if re.match(regex_CHOICE, reply) is not None:
                return answer
        try:
            probable_index = get_first_msg_word(reply)
            index = int(probable_index) - 1
            if index < 0 or index > len(answers):
                return None
            return answers[index]
        except:
            return None

    def _get_keywords(self):
        keywords = []
        if self.payload['type-interaction'] == 'announcement':
            return keywords
        elif self.payload['type-interaction'] == 'question-answer-keyword':
            for answer_keyword in self.payload['answer-keywords']:
                keywords += self.split_keywords(answer_keyword['keyword'])
            return keywords
        keywords = self.split_keywords(self.payload['keyword'])
        if (not 'set-answer-accept-no-space' in self.payload 
                or self.payload['set-answer-accept-no-space'] is None):
            return keywords
        generated_answer = copy(keywords)
        for answer in self.payload['answers']:
            generated_answer += self.get_answer_keywords(keywords, answer)
        return generated_answer    

    def get_keywords(self):
        return self.keywords

    def get_offset_time_delta(self):
        if self['type-schedule'] != 'offset-time':
            return None
        regex_MinutesSeconds = re.compile(r'(?P<minutes>\d{1,4}):?(?P<seconds>\d{2})?')
        for minutes, seconds in re.findall(regex_MinutesSeconds, self['minutes']):
            return timedelta(minutes=int(minutes), seconds=int(seconds) if seconds!='' else 0)

    def has_sending_actions(self):
        if self['type-interaction'] != 'announcement':
            return False
        if self['announcement-actions'] in [None, []]:
            return False
        return True

    def get_sending_actions(self):
        actions = Actions()
        if not self.has_sending_actions():
            return actions
        for action in self['announcement-actions']:
            actions.append(action_generator(**action))
        return actions;
