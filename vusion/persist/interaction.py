from vusion.persist.vusion_model import VusionModel
from vusion.error import InvalidField

class Interaction(VusionModel):
    
    MODEL_TYPE = 'interaction'
    MODEL_VERSION = '2'
    
    fields = [
        'interaction-id',
        'type-schedule',
        'type-interaction']
              
    SCHEDULE_TYPES = {
        'fixed-time': {'date-time': lambda v: v is not None},
        'offset-days': {'days': lambda v: v is not None,
                         'at-time': lambda v: v is not None},
        'offset-time': {'minutes': lambda v: v is not None},
        'offset-condition': {
            'offset-condition-interaction-id': lambda v: v is not None} 
        }
    
    INTERACTION_TYPE = {
        'announcement': {'content': lambda v: v is not None},
        'question-answer': {'content': lambda v: v is not None,
                            'keyword': lambda v: v is not None,
                            'set-use-template': lambda v: True,
                            'type-question': lambda v: True,
                            'type-unmatching-feedback': lambda v:True,
                            'set-reminder': lambda v: True},
        'question-answer-keyword': {'content': lambda v: v is not None,
                                    'label-for-participant-profiling': lambda v: v is not None,
                                    'answer-keywords': lambda v: isinstance(v, list),
                                    'type-unmatching-feedback': lambda v:True,
                                    'set-reminder': lambda v: True}}

    QUESTION_TYPE = {
        'closed-question': {
            'label-for-participant-profiling': lambda v: v is not None,
            'set-answer-accept-no-space': lambda v: v is not None,
            'answers': lambda v: isinstance(v, list)},
        'open-question': {
            'answer-label': lambda v: v is not None,
            'feedbacks': lambda v: isinstance(v, list)}}
        
    def validate_fields(self):
        super(Interaction, self).validate_fields()
        type_schedule = self.payload['type-schedule']
        if type_schedule not in self.SCHEDULE_TYPES:
            raise InvalidField("Unknown event_type %r" % (type_schedule,))
        for extra_field, check in self.SCHEDULE_TYPES[type_schedule].items():
            self.assert_field_present(extra_field)
            if not check(self[extra_field]):
                raise InvalidField(extra_field)
        type_interaction = self.payload['type-interaction']
        for extra_field, check in self.INTERACTION_TYPE[type_interaction].items():
            self.assert_field_present(extra_field)
            if not check(self[extra_field]):
                raise InvalidField(extra_field)

    def validate_answers(self, answer):
        pass

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            if (kwargs['type-interaction'] == 'question-answer' or 
                    kwargs['type-interaction'] == 'question-answer-keyword'):
                kwargs['type-unmatching-feedback'] = 'program-unmatching-error-message'
                kwargs['set-reminder'] = kwargs['set-reminder'] if 'set-reminder' in kwargs else None
            kwargs['model-version'] = '2'
        return kwargs
    
    def has_reminder(self):
        return self.payload['set-reminder'] is not None