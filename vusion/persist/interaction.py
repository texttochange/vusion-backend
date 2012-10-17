from copy import copy
from vusion.persist.vusion_model import VusionModel
from vusion.error import InvalidField, MissingField
from vusion.action import (action_generator, FeedbackAction,
                           UnMatchingAnswerAction, ProfilingAction,
                           RemoveDeadlineAction)

class Interaction(VusionModel):
    
    MODEL_TYPE = 'interaction'
    MODEL_VERSION = '2'
    
    fields = [
        'interaction-id',
        'type-schedule',
        'type-interaction',
        'activated']
              
    SCHEDULE_TYPES = {
        'fixed-time': {
            'date-time': lambda v: v is not None},
        'offset-days': {
            'days': lambda v: v>=1,
            'at-time': lambda v: v is not None},
        'offset-time': {
            'minutes': lambda v: v>=1},
        'offset-condition': {
            'offset-condition-interaction-id': lambda v: v is not None}}
    
    UNMATCHING_FEEDBACK_TYPE = frozenset((
        'no-unmatching-feedback',
        'program-unmatching-feedback',
        'interaction-unmatching-feedback'))
    
    INTERACTION_TYPE = {
        'announcement': {
            'content': lambda v: v is not None},
        'question-answer': {
            'content': lambda v: v is not None,
            'keyword': lambda v: v is not None,
            'set-use-template': lambda v: True,
            'type-question': lambda v: True,
            'type-unmatching-feedback': lambda v: v in Interaction.UNMATCHING_FEEDBACK_TYPE,
            'set-reminder': lambda v: True},
        'question-answer-keyword': {
            'content': lambda v: v is not None,
            'label-for-participant-profiling': lambda v: v is not None,
            'answer-keywords': lambda v: isinstance(v, list),
            'type-unmatching-feedback': lambda v: v in Interaction.UNMATCHING_FEEDBACK_TYPE,
            'set-reminder': lambda v: True}}

    QUESTION_TYPE = {
        'closed-question': {
            'label-for-participant-profiling': lambda v: True,
            'set-answer-accept-no-space': lambda v: True,
            'answers': lambda v: isinstance(v, list)},
        'open-question': {
            'answer-label': lambda v: v is not None,
            'feedbacks': lambda v: isinstance(v, list)}}

    REMINDER_FIELDS = {
         'type-schedule-reminder': lambda v: True,
         'reminder-number': lambda v: v >=1,
         'reminder-actions': lambda v: Interaction.validate_actions(v)}

    REMINDER_SCHEDULE_TYPE = {
        'reminder-offset-days': {
            'reminder-days': lambda v: v >=1,
            'reminder-at-time': lambda v: v is not None},
        'reminder-offset-time': {
            'reminder-minutes': lambda v: v>=1}}
            
    ANSWER_KEYWORD = {
        'keyword': lambda v: v is not None,
        'feedbacks': lambda v: v is not None,
        'answer-actions': lambda v: Interaction.validate_actions(v)}
    
    ANSWER = {
        'choice' : lambda v: v is not None,
        'feedbacks': lambda v: v is not None,
        'answer-actions': lambda v: Interaction.validate_actions(v)}


    def validate_fields(self):
        super(Interaction, self).validate_fields()
        # Check schedule type
        type_schedule = self.payload['type-schedule']
        if type_schedule not in self.SCHEDULE_TYPES:
            raise InvalidField("Unknown schedule_type %r" % (type_schedule,))
        for extra_field, check in self.SCHEDULE_TYPES[type_schedule].items():
            self.assert_field_present(extra_field)
            if not check(self[extra_field]):
                raise InvalidField(extra_field)
        # Check interaction type
        type_interaction = self.payload['type-interaction']
        if type_interaction not in self.INTERACTION_TYPE:
            raise InvalidField("Unknow interaction type %r" % (type_interaction,))
        for extra_field, check in self.INTERACTION_TYPE[type_interaction].items():
            self.assert_field_present(extra_field)
            if not check(self[extra_field]):
                raise InvalidField(extra_field)
        if self.payload['type-interaction'] == 'announcement':
            return
        # Check question type
        if self.payload['type-interaction'] == 'question-answer':
            question_type = self.payload['type-question'] 
            if question_type not in self.QUESTION_TYPE:
                raise InvalidField("Unknow question type %r" % (question_type,))
            for extra_field, check in self.QUESTION_TYPE[question_type].items():
                self.assert_field_present(extra_field)
            if not check(self[extra_field]):
                raise InvalidField(extra_field)
        # Check reminder
        if self.payload['set-reminder'] == 'reminder':
            for extra_field, check in self.REMINDER_FIELDS.items():
                self.assert_field_present(extra_field)
                if not check(self[extra_field]):
                    raise InvalidField(extra_field)
            for extra_field, check in self.REMINDER_SCHEDULE_TYPE[self[extra_field]].items():
                self.assert_field_present(extra_field)
                if not check(self[extra_field]):
                    raise InvalidField(extra_field)
        # Check answers
        if self.payload['type-interaction'] == 'question-answer-keyword':
            if self.payload['answer-keywords'] == None:
                self.payload['answer-keywords'] = []
            self.validate_answers(self.payload['answer-keywords'], self.ANSWER_KEYWORD)
        elif (self.payload['type-interaction'] == 'question-answer'
                  and self.payload['type-question'] == 'closed-question'):
            if self.payload['answers'] == None:
                self.payload['answers'] = []
            self.validate_answers(self.payload['answers'], self.ANSWER)
            
    def validate_answers(self, answers, validation_rules):
        for answer in answers:
            for extra_field, check in validation_rules.items():
                if not extra_field in answer:
                    raise MissingField(extra_field)
                if not check(answer[extra_field]):
                    raise InvalidField(extra_field)

    @staticmethod
    def validate_actions(actions):
        if actions is None:
            return True
        for action in actions:
            action_generator(**action)
        return True

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            if (kwargs['type-interaction'] == 'question-answer' or 
                    kwargs['type-interaction'] == 'question-answer-keyword'):
                kwargs['type-unmatching-feedback'] = 'program-unmatching-feedback'
                kwargs['set-reminder'] = kwargs['set-reminder'] if 'set-reminder' in kwargs else None
                kwargs['set-use-template'] = kwargs['set-use-template'] if 'set-use-template' in kwargs else None
                if ('type-question' in kwargs and 
                        kwargs['type-question'] == 'closed-question'):
                    kwargs['label-for-participant-profiling'] = kwargs['label-for-participant-profiling'] if 'label-for-participant-profiling' in kwargs else None
                    kwargs['set-answer-accept-no-space'] = kwargs['set-answer-accept-no-space'] if 'set-answer-accept-no-space' in kwargs else None
                if ('type-question' in kwargs and 
                        kwargs['type-question'] == 'open-question'):
                    kwargs['feedbacks'] = kwargs['feedbacks'] if 'feedbacks' in kwargs else []
                if (kwargs['type-interaction'] == 'question-answer-keyword'):
                    kwargs['label-for-participant-profiling'] = kwargs['label-for-participant-profiling'] if 'label-for-participant-profiling' in kwargs else None
                if kwargs['type-interaction'] == 'question-answer-keyword':
                    kwargs['answer-keywords'] = kwargs['answer-keywords'] if 'answer-keywords' in kwargs else []
                    for answer in kwargs['answer-keywords']:
                        answer['feedbacks'] = answer['feedbacks'] if 'feedbacks' in answer else []
                        answer['answer-actions'] = answer['answer-actions'] if 'answer-actions' in answer else []
                if kwargs['type-interaction'] == 'question-answer' and kwargs['type-question'] == 'closed-question':
                    kwargs['answers'] = kwargs['answers'] if 'answers' in kwargs else []
                    for answer in kwargs['answers']:
                        answer['feedbacks'] = answer['feedbacks'] if 'feedbacks' in answer else []
                        answer['answer-actions'] = answer['answer-actions'] if 'answer-actions' in answer else []
            kwargs['model-version'] = '2'
        return kwargs

    def has_reminder(self):
        return self.payload['set-reminder'] is not None

    def get_unmatching_action(self, answer, actions):
        if self.payload['type-unmatching-feedback'] == 'interaction-unmatching-feedback':
            actions.append(FeedbackAction(**{'content': self.payload['unmatching-feedback-content']}))
        elif self.payload['type-unmatching-feedback'] == 'program-unmatching-feedback':
            actions.append(UnMatchingAnswerAction(**{'answer': answer}))

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
        return [k.lower() for k in (keywords or '').split(', ')]

    def get_answer_keywords(self, keywords, answer):
        return [("%s%s" % (keyword, answer['choice'])).lower() for keyword in keywords]

    def get_actions_from_matching_answer(self, dialogue_id, matching_answer, matching_value, actions):
        for feedback in matching_answer['feedbacks']:
            action = FeedbackAction(**{'content': feedback['content']})
            actions.append(action)
        if 'answer-actions' in matching_answer:
            for matching_answer_action in matching_answer['answer-actions']:
                actions.append(action_generator(**matching_answer_action))
        return actions
    
    def get_actions_from_interaction(self, dialogue_id, matching_value, actions):
        if self.has_reminder():
            actions.append(RemoveDeadlineAction(**{
                'dialogue-id': dialogue_id,
                'interaction-id': self.payload['interaction-id']}))
        if ('label-for-participant-profiling' in self.payload 
                and self.payload['label-for-participant-profiling'] is not None):
            action = ProfilingAction(**{
                'label': self.payload['label-for-participant-profiling'],
                'value': matching_value})
            actions.append(action)
        elif ('answer-label' in self.payload 
                and self.payload['answer-label'] is not None):
            action = ProfilingAction(**{
                'label': self.payload['answer-label'],
                'value': matching_value})
            actions.append(action)            
        if 'feedbacks' in self.payload:
            for feedback in self.payload['feedbacks']:
                action = FeedbackAction(**{'content': feedback['content']})
                actions.append(action)
        
    def get_actions(self, dialogue_id, msg, msg_keyword, msg_reply, reference_metadata, actions):
        if 'answer-keywords' in self.payload:
            # Multi keyword question
            matching_answer_keyword = self.get_matching_answer_keyword(self.payload['answer-keywords'], msg_keyword)
            if matching_answer_keyword is None:
                self.get_unmatching_action(msg, actions)
            else:                
                reference_metadata['matching-answer'] = matching_answer_keyword['keyword']
                self.get_actions_from_interaction(
                    dialogue_id,
                    reference_metadata['matching-answer'],
                    actions)
                self.get_actions_from_matching_answer(
                    dialogue_id,
                    matching_answer_keyword,
                    reference_metadata['matching-answer'],
                    actions)
                return reference_metadata, actions
        elif 'answers' in self.payload:
            # Closed questions
            matching_answer = self.get_matching_answer(msg_keyword, msg_reply)
            if matching_answer is None:
                self.get_unmatching_action(msg, actions)
            else:
                reference_metadata['matching-answer'] = matching_answer['choice']
                self.get_actions_from_interaction(
                    dialogue_id,
                    reference_metadata['matching-answer'],
                    actions)
                self.get_actions_from_matching_answer(
                    dialogue_id,
                    matching_answer, 
                    reference_metadata['matching-answer'], 
                    actions)
                return reference_metadata, actions
        else:
            # Open questions
            msg_reply_raw = self.get_open_answer(msg)
            if msg_reply_raw == '':
                self.get_unmatching_action(msg_reply, actions)
            else:
                reference_metadata['matching-answer'] = msg_reply_raw
                self.get_actions_from_interaction(
                    dialogue_id,
                    reference_metadata['matching-answer'],
                    actions)
                return reference_metadata, actions
     
    def get_matching_answer_keyword(self, answer_keywords, msg_keyword):
        for answer_keyword in answer_keywords:
            if msg_keyword in self.split_keywords(answer_keyword['keyword']):
                return answer_keyword
        return None
    
    def get_matching_answer(self, keyword, reply):
        answers = self.payload['answers']
        if self.payload['set-answer-accept-no-space'] is not None:
            keywords = self.split_keywords(self.payload['keyword'])
            for answer in answers:
                if keyword in self.get_answer_keywords(keywords, answer):
                    return answer
        try:
            index = int(reply) - 1
            if index < 0 or index > len(answers):
                return None
            return answers[index]
        except:
            pass
        for answer in answers:
            if answer['choice'].lower() == reply:
                return answer
        return None
    
    def get_open_answer(self, message):
        words = (message or '').split(' ')
        return " ".join(words[1:])

    def get_keywords(self):
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

    def get_answer_keywords(self, keywords, answer):
        return [("%s%s" % (keyword, answer['choice'])).lower() for keyword in keywords]
