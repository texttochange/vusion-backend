import re

from vusion.persist.vusion_model import VusionModel
from vusion.error import InvalidField, MissingField

## TODO update the validation
class Participant(VusionModel):

    MODEL_TYPE = 'participant'
    MODEL_VERSION = '4'

    REGEX_RAW = re.compile('.*_raw$')

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

    def enroll(self, dialogue_id, time):
        pass

    def is_enrolled(self, dialogue_id):
        for enrolled in self.payload['enrolled']:
            if enrolled['dialogue-id']==dialogue_id:
                return True
        return False

    def tag(self, tag):
        pass
    
    def get_participant_label_value(self, label):
        if re.match(self.REGEX_RAW, label):
            field = 'raw'
            label = label[:-4]
        else:
            field = 'value'
        label_indexer = dict((p['label'], p[field]) for i, p in enumerate(self.payload['profile']))
        return label_indexer.get(label, None)

    def is_optin(self):
        return self.payload['session-id'] is not None
    
    def has_profile(self, label, value):
        for item in self['profile']:
            if label == item['label'] and value == item['value']:
                return True
        return False

    def get_transport_metadata(self):
        return self['transport_metadata']