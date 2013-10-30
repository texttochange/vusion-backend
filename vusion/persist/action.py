import re
from math import ceil

from vusion.error import MissingField, VusionError, InvalidField
from vusion.persist.vusion_model import VusionModel
from vumi.log import log

class Action(VusionModel):

    MODEL_TYPE = 'action'
    MODEL_VERSION = '2'
    
    ACTION_TYPE = None

    ##TODO add subcondition on type action
    fields = {
        'subconditions': {
            'required': False,
            'valid_conditions': lambda v: getattr(v, 'valid_conditions')(),
            },
        'condition-operator': {
            'required': False,
            'valid_value': lambda v: v['condition-operator'] in ['all-subconditions', 'any-subconditions']
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
                'proportional-tagging',
                'remove-question',
                'remove-reminders',
                'remove-deadline',
                'offset-conditioning',
                'message-forwarding',
                'url-forwarding',
                'sms-forwarding']},
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
            'with': '.*',
            'not-with': '.*'},
        'labelled':{
            'with': '.*:.*',
            'not-with': '.*:.*'},
    }

    def __init__(self, **kwargs):
        kwargs.update({'type-action':self.ACTION_TYPE})
        super(Action, self).__init__(**kwargs)

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['set-condition'] = None
            kwargs['model-version'] = '2'
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

    def valid_subcondition_value(self, subconditon):
        if not subconditon['subcondition-field'] in self.subcondition_values:
            raise InvalidField("%s=%s is not valid" % ('subcondition-field', subconditon['subcondition-field']))
        operators = self.subcondition_values[subconditon['subcondition-field']]
        if not subconditon['subcondition-operator'] in operators:
            raise InvalidField("%s=%s is not valid" % ('subcondition-operator', subconditon['subcondition-operator']))
        parameter_regex = re.compile(operators[subconditon['subcondition-operator']])
        if not parameter_regex.match(subconditon['subcondition-parameter']):
            raise InvalidField("%s=%s is not valid" % ('subcondition-parameter', subconditon['subcondition-parameter']))
        
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

    def assert_list_field_present(self, elements, *fields):
        for element in elements:
            for field in fields:
                if field not in element:
                    raise MissingField(field)

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
                if subcondition['subcondition-operator'] == 'with':
                    query.append({
                        'tags': subcondition['subcondition-parameter']
                    })
                elif subcondition['subcondition-operator'] == 'not-with':
                    query.append({
                        'tags': {'$ne': subcondition['subcondition-parameter']}
                    })
            elif subcondition['subcondition-field'] == 'labelled':
                profile = subcondition['subcondition-parameter'].split(':')
                if subcondition['subcondition-operator'] == 'with':
                    query.append({
                        'profile': {'$elemMatch': {'label': profile[0],
                                                    'value': profile[1]}}})
                elif subcondition['subcondition-operator'] == 'not-with':
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
            if self['condition-operator'] == 'all-subconditions':
                return {'$and': query}
            elif self['condition-operator'] == 'any-subconditions':
                return {'$or': query}
        
    def get_condition_mongodb_for(self, phone, session_id):
        query = self.get_condition_mongodb()
        if '$and' in query:
            query['$and'].insert(0, {'phone': phone, 'session-id': session_id})
        elif '$or' in query:
            query = {'$and': [{'phone': phone,'session-id': session_id},
                              query]}
        else:
            query.update({'phone': phone,
                          'session-id': session_id})
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


class ProportionalTagging(Action):
    
    ACTION_TYPE = 'proportional-tagging'
    
    def validate_fields(self):
        super(ProportionalTagging, self).validate_fields()
        self.assert_field_present('proportional-tags')
        self.assert_list_field_present(self['proportional-tags'], *['tag', 'weight'])

    def get_proportional_tags(self):
        return self['proportional-tags']
    
    def set_tag_count(self, tag, count):
        for i, proportional_tag in enumerate(self['proportional-tags']):
            if proportional_tag['tag'] == tag:
                proportional_tag.update({'count': count})
                self['proportional-tags'][i] = proportional_tag
                break
    
    def get_tags(self):
        tags = []
        for proportional_tag in self['proportional-tags']:
            tags.append(proportional_tag['tag'])
        return tags
    
    def get_totals(self):
        weight_total = 0
        count_total =0
        for proportional_tag in self['proportional-tags']:
            weight_total = weight_total + (int(proportional_tag['weight']) or 0)
            count_total = count_total + (proportional_tag['count'] if 'count' in proportional_tag else 0)
        return weight_total, count_total
    
    def get_tagging_action(self):
        weight_total, count_total = self.get_totals()
        for proportional_tag in self['proportional-tags']:
            weight_tag = int(proportional_tag['weight'])
            count_expected = ceil(count_total * weight_tag / weight_total)
            count_tag = (proportional_tag['count'] if 'count' in proportional_tag else 0)
            if count_expected >= count_tag:
                return TaggingAction(**{'tag': proportional_tag['tag']})
        return TaggingAction(**{'tag': self['proportional-tags'][0]['tag']})


class UrlForwarding(Action):
    
    ACTION_TYPE = 'url-forwarding'
    
    def validate_fields(self):
        super(UrlForwarding, self).validate_fields()
        self.assert_field_present('forward-url')
      
      
class SmsForwarding(Action):
    
    ACTION_TYPE = 'sms-forwarding'

    def validate_field(self):
        super(SmsForwarding, self).validate_fields()
        self.assert_field_present('forward-to', 'forward-content')    
             

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
    elif kwargs['type-action'] == 'proportional-tagging':
        return ProportionalTagging(**kwargs)
    elif kwargs['type-action'] in ['message-forwarding', 'url-forwarding']:
        return UrlForwarding(**kwargs)
    elif kwargs['type-action'] == 'sms-forwarding':
        return SmsForwarding(**kwargs)
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
