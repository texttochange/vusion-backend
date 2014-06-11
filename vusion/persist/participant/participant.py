import re
from datetime import datetime

from vusion.persist import Model
from vusion.error import InvalidField, MissingField
from vusion.utils import time_to_vusion_format

## TODO update the validation
class Participant(Model):

    MODEL_TYPE = 'participant'
    MODEL_VERSION = '4'

    REGEX_RAW = re.compile('.*_raw$')

    #TODO wrong naming convention for transport_metadata
    fields = [
        'phone',
        'session-id', 
        'last-optin-date',
        'last-optout-date',
        'tags',
        'enrolled',
        'profile',
        'transport_metadata']

    PARTICIPANT_FIELDS = {
        'phone': lambda v: v is not None,
        'session-id': lambda v: True,
        'last-optin-date': lambda v: True,
        'last-optout-date': lambda v: True,
        'tags': lambda v: isinstance(v, list),
        'enrolled': lambda v: isinstance(v, list),
        'profile': lambda v: isinstance(v, list),
        'transport_metadata': lambda v: isinstance(v, dict)}

    ENROLLED_FIELDS = {
        'dialogue-id': lambda v: v is not None,
        'date-time': lambda v: v is not None}
    
    PROFILE_FIELDS = {
        'label': lambda v: v is not None,
        'value': lambda v: True,
        'raw': lambda v: True}

    FIELDS_THAT_SHOULD_BE_ARRAY = {
        'tags',
        'enrolled',
        'profile'}

    OPERATORS = {
        'all-subconditions': '$and',
        'any-subconditions': '$or'}

    def validate_fields(self):
        super(Participant, self).validate_fields()
        for field, check in self.PARTICIPANT_FIELDS.items():
            self.assert_field_present(field)
            self.modify_field_that_should_be_array(field)
            if not check(self[field]):
                raise InvalidField(field)
        for field, check in self.ENROLLED_FIELDS.items():
            for enroll in self['enrolled']:
                if not field in enroll:
                    raise MissingField(field)
                if not check(enroll[field]):
                    raise InvalidField(field)
        for field, check in self.PROFILE_FIELDS.items():
            for label in self['profile']:
                if not field in label:
                    raise MissingField(field)
                if not check(label[field]):
                    raise InvalidField(field)        

    def upgrade(self, **kwargs):
        if kwargs['model-version'] in ['1', 1]:
            for label in kwargs['profile']:
                if not 'raw' in label:
                    label.update({'raw': None})
            kwargs['model-version'] = '2'
            return self.upgrade(**kwargs)
        elif kwargs['model-version'] in '2':
            kwargs['last-optout-date'] = kwargs['last-optout-date'] if 'last-optout-date' in kwargs else None
            kwargs['model-version'] = '3'
            return self.upgrade(**kwargs)
        elif kwargs['model-version'] in '3':
            kwargs['transport_metadata'] = kwargs['transport_metadata'] if 'transport_metadata' in kwargs else {}
            kwargs['model-version'] = '4'            
        return kwargs

    def modify_field_that_should_be_array(self, field):
        if field in self.FIELDS_THAT_SHOULD_BE_ARRAY and self[field] is None:
            self[field] = []

    def process_field(self, key, value):
        if key in ['last-optout-date', 'last-optin-date'] and isinstance(value, datetime):
            return time_to_vusion_format(value)
        return value    

    def get_label_value(self, label):
        if re.match(self.REGEX_RAW, label):
            field = 'raw'
            label = label[:-4]
        else:
            field = 'value'
        label_indexer = dict((p['label'], p[field]) for i, p in enumerate(self.payload['profile']))
        return label_indexer.get(label, None)

    def get_data(self, data):
        value = self.get_label_value(data)
        if value is None:
            value = self[data]
        if not isinstance(value, basestring):
            return None
        return value;

    def is_optin(self):
        return self.payload['session-id'] is not None
    
    def has_profile(self, label, value):
        for item in self['profile']:
            if label == item['label'] and value == item['value']:
                return True
        return False

    def has_tag(self, tag):
        if tag in self['tags']:
            return True
        return False

    def get_transport_metadata(self):
        return self['transport_metadata']

    def get_session_id(self):
        return self['session-id']

    def get_enrolled(self, dialogue_id):
        for enrolled in self.payload['enrolled']:
            if enrolled['dialogue-id']==dialogue_id:
                return enrolled
        return None

    def is_enrolled(self, dialogue_id):
        if self.get_enrolled(dialogue_id) is not None:
            return True
        return False

    def get_enrolled_time(self, dialogue_id):
        enrolled = self.get_enrolled(dialogue_id)
        if enrolled is None:
            return None
        return enrolled['date-time']
    
    @staticmethod
    def from_conditions_to_query(condition_operator, subconditions):
        query = None
        operator = Participant.OPERATORS[condition_operator]
        for subcondition in subconditions:
            if subcondition['subcondition-field'] == 'tagged':
                if subcondition['subcondition-operator'] == 'with':
                    tmp_query = {'tags': subcondition['subcondition-parameter']}
                elif subcondition['subcondition-operator'] == 'not-with': 
                    tmp_query = {'tags': {'$ne': subcondition['subcondition-parameter']}}
            if subcondition['subcondition-field'] == 'labelled':
                profile = subcondition['subcondition-parameter'].split(':')                    
                if subcondition['subcondition-operator'] == 'with':
                    tmp_query = {'profile': {'$elemMatch': {'label': profile[0], 'value': profile[1]}}}
                elif subcondition['subcondition-operator'] == 'not-with': 
                    tmp_query = {'profile': {'$not' : {'$elemMatch': {'label': profile[0], 'value': profile[1]}}}}
            if query is None:
                query = tmp_query
            else:
                if operator in query:
                    query[operator].append(tmp_query)
                else:
                    query = {operator: [query, tmp_query]}       
        return query

    def is_matching_conditions(self, condition_operator, subconditions):
        if condition_operator == 'all-subconditions':
            for subcondition in subconditions:
                if not self.is_matching_subcondtion(subcondition):
                    return False
            return True
        elif condition_operator == 'any-subconditions':
            for subcondition in subconditions:
                if self.is_matching_subcondtion(subcondition):
                    return True
            return False
        return False   ##default behavior

    def is_matching_subcondtion(self, subcondition):
        if subcondition['subcondition-field'] == 'tagged':
            if subcondition['subcondition-operator'] == 'with':
                if self.has_tag(subcondition['subcondition-parameter']):
                    return True
            elif subcondition['subcondition-operator'] == 'not-with':
                if not self.has_tag(subcondition['subcondition-parameter']):
                    return True
        elif subcondition['subcondition-field'] == 'labelled':
            profile = subcondition['subcondition-parameter'].split(':')            
            if subcondition['subcondition-operator'] == 'with':
                if self.has_profile(profile[0], profile[1]):
                    return True
            elif subcondition['subcondition-operator'] == 'not-with':
                if not self.has_profile(profile[0], profile[1]):
                    return True
        return False
