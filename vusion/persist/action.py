import re

from vusion.error import MissingField, VusionError, InvalidField
from vusion.persist.vusion_model import VusionModel

class Action(VusionModel):

    MODEL_TYPE = 'action'
    MODEL_VERSION = '2'
    
    ACTION_TYPE = None

    fields = {
        'subconditions': {
            'required': False,
            'valid_conditions': lambda v: getattr(v, 'valid_conditions')(),
            },
        'condition-operator': {
            'required': False,
            'valid_value': lambda v: v['condition-operator'] in ['all', 'any']
            },
        'set-condition': {
            'required': True,
            'valid_value': lambda v: v['set-condition'] in [None, 'condition'],
            'required_subfield': lambda v: getattr(v, 'required_subfields')(
                v['set-condition'],
                {'condition':['subconditions', 'condition-operator']}),
            },        
        'type-action': {
            'required': True,
            'valid_value': lambda v: v['type-action'] in [
                'optin', 
                'optout',
                'reset',
                'feedback',
                'unmatching-answer',
                'tagging',
                'enrolling',
                'profiling',
                'delayed-enrolling',
                'remove-question',
                'remove-reminders',
                'remove-deadline',
                'offset-conditioning']
            },
        }

    subcondition_fields = {
        'subcondition-field': {
            'required': True,
            },
        'subcondition-operator': {
            'required': True,
            },
        'subcondition-parameter': {
            'required': True,
        }
    }
        
    subcondition_values = {
        'tagged':{
            'in': '.*',
            'not-in': '.*'},
        'labelled':{
            'in': '.*:.*',
            'not-in': '.*:.*'},
    }

    def __init__(self, **kwargs):
        kwargs.update({'type-action':self.ACTION_TYPE})
        super(Action, self).__init__(**kwargs)

    def upgrade(self, **kwargs):
        if kwargs['model-version'] is '1':
            kwargs['set-condition'] = None
            kwargs['model-version'] = '2'
            return self.upgrade(**kwargs)
        return kwargs

    def __eq__(self, other):
        if isinstance(other, Action):
            return self.payload == other.payload
        return False

    def __str__(self):
        return "Do:%s payload=%s" % (self.get_type(), repr(self.payload))

    def __repr__(self):
        return str(self)

    def __getitem__(self, key):
        return self.payload[key]

    def __setitem__(self, key, value):
        self.payload[key] = value

    def process_fields(self, fields):
        return fields

    def validate_fields(self):
        self._validate(self, self.fields)

    def valid_conditions(self):
        if not 'subconditions' in self:
            return True
        for subcondition in self['subconditions']:
            self._validate(subcondition, self.subcondition_fields)
            self.valid_subcondition_value(subcondition)
        return True
    
    def _validate(self, data, field_rules):
        for field, rules in field_rules.items():
            for rule_name, rule in rules.items():
                if rule_name is 'required':
                    if not rule and not field in data:
                        break
                    else: 
                        continue
                if not rule(data):
                    raise InvalidField("%s=%s is not %s" % (field, data[field], rule_name))        

    def valid_subcondition_value(self, subconditon):
        if not subconditon['subcondition-field'] in self.subcondition_values:
            raise InvalidField("%s=%s is not valid" % ('subcondition-field', subconditon['subcondition-field']))
        operators = self.subcondition_values[subconditon['subcondition-field']]
        if not subconditon['subcondition-operator'] in operators:
            raise InvalidField("%s=%s is not valid" % ('subcondition-operator', subconditon['subcondition-operator']))
        parameter_regex = re.compile(operators[subconditon['subcondition-operator']])
        if not parameter_regex.match(subconditon['subcondition-parameter']):
            raise InvalidField("%s=%s is not valid" % ('subcondition-parameter', subconditon['subcondition-parameter']))
        
    def required_subfields(self, field, subfields):
        if field is None:
            return True
        for subfield in subfields[field]:
            if not subfield in self:
                raise MissingField(subfield)
        return True

    def get_type(self):
        return self.ACTION_TYPE

    def assert_field_present(self, *fields):
        for field in fields:
            if field not in self.payload:
                raise MissingField(field)

    def assert_subfield_present(self, field, *subfields):
        for subfield in subfields:
            if subfield not in self[field]:
                raise MissingField(subfield)

    def get_as_dict(self):
        action_dict = {'type-action': self.get_type()}
        for key in self.payload:
            action_dict[key] = self.payload[key]
        return action_dict

    def has_condition(self):
        return self['set-condition'] == 'condition'

    def get_condition_mongodb(self):
        if not self.has_condition():
            return {}
        query = []
        for subcondition in self['subconditions']:
            if subcondition['subcondition-field'] == 'tagged':
                if subcondition['subcondition-operator'] == 'in':
                    query.append({
                        'tags': {'$in': subcondition['subcondition-parameter']}
                    })
                elif subcondition['subcondition-operator'] == 'not-in':
                    query.append({
                        'tags': {'$in': {'$ne': subcondition['subcondition-parameter']}}
                    })
            elif subcondition['subcondition-field'] == 'labelled':
                profile = subcondition['subcondition-parameter'].split(':')
                if subcondition['subcondition-operator'] == 'in':
                    query.append({
                        'profile': {'$elemMatch': {'label': profile[0],
                                                    'value': profile[1]}}})
                elif subcondition['subcondition-parameter'] == 'not-in':
                    query.append({
                        'profile': {
                            '$elemMatch': {
                                '$or': [{'label': {'$ne': profile[0]},
                                         'value': {'$ne': profile[1]}}]}}})
        if len(query) == 0:
            return {}
        elif len(query) == 1:
            return query.pop()
        elif len(query) > 1:
            if self['condition-operator'] == 'all':
                return {'$and': query}
            elif self['condition-operator'] == 'any':
                return {'$or': query}
        
    def get_condition_mongodb_for(self, phone, session_id):
        query = self.get_condition_mongodb()
        if '$and' in query:
            query['$and'].insert(0, {'phone': phone, 'session_id': session_id})
        elif '$or' in query:
            query = {'$and': [{'phone': phone,'session_id': session_id},
                              query]}
        else:
            query.update({'phone': phone,
                          'session_id': session_id})
        return query


class OptinAction(Action):

    ACTION_TYPE = 'optin'

    def validate_fields(self):
        super(OptinAction, self).validate_fields()        


class OptoutAction(Action):

    ACTION_TYPE = 'optout'

    def validate_fields(self):
        super(OptoutAction, self).validate_fields()


class ResetAction(Action):

    ACTION_TYPE = 'reset'

    def validate_fields(self):
        super(ResetAction, self).validate_fields()        


class FeedbackAction(Action):

    ACTION_TYPE = 'feedback'

    def validate_fields(self):
        super(FeedbackAction, self).validate_fields()        
        self.assert_field_present('content')


class UnMatchingAnswerAction(Action):

    ACTION_TYPE = 'unmatching-answer'

    def validate_fields(self):
        super(UnMatchingAnswerAction, self).validate_fields()        
        self.assert_field_present('answer')


class TaggingAction(Action):

    ACTION_TYPE = 'tagging'

    def validate_fields(self):
        super(TaggingAction, self).validate_fields()        
        self.assert_field_present('tag')


class EnrollingAction(Action):

    ACTION_TYPE = 'enrolling'

    def validate_fields(self):
        super(EnrollingAction, self).validate_fields()        
        self.assert_field_present('enroll')


class DelayedEnrollingAction(Action):

    ACTION_TYPE = 'delayed-enrolling'

    def validate_fields(self):
        super(DelayedEnrollingAction, self).validate_fields()        
        self.assert_field_present(
            'enroll',
            'offset-days')
        self.assert_subfield_present(
            'offset-days',
            'days',
            'at-time')


class ProfilingAction(Action):

    ACTION_TYPE = 'profiling'

    def validate_fields(self):
        super(ProfilingAction, self).validate_fields()        
        self.assert_field_present('label', 'value')
    

class RemoveQuestionAction(Action):

    ACTION_TYPE = 'remove-question'

    def validate_fields(self):
        super(RemoveQuestionAction, self).validate_fields()        
        self.assert_field_present('dialogue-id', 'interaction-id')


class RemoveRemindersAction(Action):

    ACTION_TYPE = 'remove-reminders'

    def validate_fields(self):
        super(RemoveRemindersAction, self).validate_fields()        
        self.assert_field_present('dialogue-id', 'interaction-id')


class RemoveDeadlineAction(Action):

    ACTION_TYPE = 'remove-deadline'

    def validate_fields(self):
        super(RemoveDeadlineAction, self).validate_fields()        
        self.assert_field_present('dialogue-id', 'interaction-id')


class OffsetConditionAction(Action):

    ACTION_TYPE = 'offset-conditioning'

    def validate_fields(self):
        super(OffsetConditionAction, self).validate_fields()        
        self.assert_field_present('interaction-id', 'dialogue-id')


def action_generator(**kwargs):
    # Condition to be removed when Dialogue structure freezed
    if 'type-action' not in kwargs:
        kwargs['type-action'] = kwargs['type-answer-action']
    if kwargs['type-action'] == 'optin':
        return OptinAction(**kwargs)
    elif kwargs['type-action'] == 'optout':
        return OptoutAction(**kwargs)
    elif kwargs['type-action'] == 'reset':
        return ResetAction(**kwargs)
    elif kwargs['type-action'] == 'enrolling':
        return EnrollingAction(**kwargs)
    elif kwargs['type-action'] == 'delayed-enrolling':
        return DelayedEnrollingAction(**kwargs)
    elif kwargs['type-action'] == 'tagging':
        return TaggingAction(**kwargs)
    elif kwargs['type-action'] == 'profiling':
        return ProfilingAction(**kwargs)
    elif kwargs['type-action'] == 'feedback':
        return FeedbackAction(**kwargs)
    elif kwargs['type-action'] == 'unmatching-answer':
        return UnMatchingAnswerAction(**kwargs)
    elif kwargs['type-action'] == 'remove-reminders':
        return RemoveRemindersAction(**kwargs)
    elif kwargs['type-action'] == 'remove-deadline':
        return RemoveDeadlineAction(**kwargs)
    elif kwargs['type-action'] == 'offset-conditioning':
        return OffsetConditionAction(**kwargs)
    raise VusionError("%r not supported" % kwargs)


class Actions():

    def __init__(self):
        self.actions = []

    def append(self, action):
        if action.get_type == "optin" or action.get_type == "enrolling":
            self.actions.insert(0, action)
        else:
            self.actions.append(action)

    def contains(self, action_type):
        for action in self.actions:
            if action.get_type() == action_type:
                return True
        return False

    def items(self):
        return self.actions.__iter__()

    def __getitem__(self, key):
        return self.actions[key]

    def get_priority_action(self):
        return self.actions.pop(0)

    def __len__(self):
        return len(self.actions)

    def clear_all(self):
        self.actions = []

    def keep_only_remove_action(self):
        for action in self.actions:
            if (action.get_type() != 'remove-reminders' and
                    action.get_type() != 'remove-deadline' and
                    action.get_type() != 'remove-question'):
                self.actions.remove(action)
