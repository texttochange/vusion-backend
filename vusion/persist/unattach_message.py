import re

from vusion.error import InvalidField, MissingField
from vusion_model import VusionModel

class UnattachMessage(VusionModel):
    
    MODEL_TYPE = 'unattach_message'
    MODEL_VERSION = '4'
    
    fields = []
    
    UNATTACH_MESSAGE_FIELDS = {
        'name' : lambda v: v is not None,
        'send-to-type': lambda v: v in ['all', 'match'],
        'content': lambda v: v is not None,
        'type-schedule': lambda v: v in ['fixed-time', 'immediately'],
        'fixed-time': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)T0?(\d+):0?(\d+)(:0?(\d+))$'), v)
    }
    
    tag_regex = re.compile('^[a-zA-Z0-9\s]+$')
    label_regex = re.compile('^[a-zA-Z0-9\s]+:[a-zA-Z0-9\s]+$')    

    SEND_TO_FIELDS = {
        'all': {},
        'match': {
            'send-to-match-operator': lambda v: v in ['all', 'any'],
            'send-to-match-conditions': lambda v: isinstance(v, list)
            },
        }
    
    OPERATORS = {
        'all': '$and',
        'any': '$or'
        }

    def validate_fields(self):
        for field, check in self.UNATTACH_MESSAGE_FIELDS.items():
            self.assert_field_present(field)
            if not check(self[field]):
                raise InvalidField(field)
        for field, check in self.SEND_TO_FIELDS[self['send-to-type']].items():
            self.assert_field_present(field)
            if not check(self[field]):
                raise InvalidField(field)
    
    def upgrade(self, **kwargs):
        if kwargs['model-version'] in ['1', None]:
            kwargs['to'] = ['all-participants']
            kwargs['model-version'] = '2'
            return self.upgrade(**kwargs)
        elif kwargs['model-version'] == '2':
            if 'all-participants' in kwargs['to']:    
                kwargs['send-to-type'] = 'all'
            else:
                kwargs['send-to-type'] = 'match'
                kwargs['send-to-match-operator'] = 'any'
                kwargs['send-to-match-conditions'] = kwargs['to']
            kwargs.pop('to')
            kwargs['model-version'] = '3'
            return self.upgrade(**kwargs)
        elif kwargs['model-version'] == '3':
            kwargs['created-by'] = kwargs['created-by'] if 'created-by' in kwargs else None
            kwargs['model-version'] = '4' 
        return kwargs

    def get_selector_as_query(self):
        query = None
        if self['send-to-type'] == 'all':
            return {}
        elif self['send-to-type'] == 'match':
            operator = self.OPERATORS[self['send-to-match-operator']]
            for selector in self['send-to-match-conditions']:
                if re.match(self.tag_regex, selector):
                    tmp_query = {'tags': selector}
                if re.match(self.label_regex, selector):
                    profile = selector.split(':')
                    tmp_query = {'profile': {'$elemMatch': {'label': profile[0], 'value': profile[1]}}}
                if query is None:
                    query = tmp_query
                else:
                    if operator in query:
                        query[operator].append(tmp_query)
                    else:
                        query = {operator: [query, tmp_query]}
        return query

    def is_selectable(self, participant):
        if 'all' in self['send-to-type']:
            return True
        if self['send-to-match-operator'] == 'any': 
            for selector in self['send-to-match-conditions']:
                if re.match(self.tag_regex, selector):
                    if selector in participant['tags']:
                        return True
                elif re.match(self.label_regex, selector):
                    profile = selector.split(':')
                    if participant.has_profile(profile[0], profile[1]):
                        return True
        elif self['send-to-match-operator'] == 'all':
            for selector in self['send-to-match-conditions']:
                if re.match(self.tag_regex, selector):
                    if not selector in participant['tags']:
                        return False
                elif re.match(self.label_regex, selector):
                    profile = selector.split(':')
                    if not participant.has_profile(profile[0], profile[1]):
                        return False
            return True
        return False
