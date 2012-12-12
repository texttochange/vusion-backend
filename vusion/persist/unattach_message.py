import re

from vusion.error import InvalidField, MissingField
from vusion_model import VusionModel

class UnattachMessage(VusionModel):
    
    MODEL_TYPE = 'unattach_message'
    MODEL_VERSION = '2'
    
    fields = []
    
    UNATTACH_MESSAGE_FIELDS = {
        'name' : lambda v: v is not None,
        'to': lambda v: isinstance(v, list),
        'content': lambda v: v is not None,
        'type-schedule': lambda v: v in ['fixed-time', 'immediately'],
        'fixed-time': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)T0?(\d+):0?(\d+)(:0?(\d+))$'), v)
    }
    
    tag_regex = re.compile('^[a-zA-Z0-9\s]+$')
    label_regex = re.compile('^[a-zA-Z0-9\s]+:[a-zA-Z0-9\s]+$')    
    
    def validate_fields(self):
        for field, check in self.UNATTACH_MESSAGE_FIELDS.items():
            self.assert_field_present(field)
            if not check(self[field]):
                raise InvalidField(field)
    
    def upgrade(self, **kwargs):
        if kwargs['model-version'] in ['1', None]:
            kwargs['to'] = ['all-participants']
            kwargs['model-version'] = '2'
        return kwargs

    def get_selector_as_query(self):
        query = None
        for selector in self['to']:
            if selector == 'all-participants':
                return {}
            if re.match(self.tag_regex, selector):
                tmp_query = {'tags': selector}
            if re.match(self.label_regex, selector):
                profile = selector.split(':')
                tmp_query = {'profile': {'$elemMatch': {'label': profile[0], 'value': profile[1]}}}
            if query is None:
                query = tmp_query
            else:
                if '$or' in query:
                    query['$or'].append(tmp_query)
                else:
                    query = {'$or': [query, tmp_query]}
        return query

    def is_selectable(self, participant):
        if 'all-participants' in self['to']:
            return True
        for selector in self['to']:
            if re.match(self.tag_regex, selector):
                if selector in participant['tags']:
                    return True
            elif re.match(self.label_regex, selector):
                profile = selector.split(':')
                if participant.has_profile(profile[0], profile[1]):
                    return True
        return False
