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
        'fixed-time': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)T0?(\d+):0?(\d+)$'), v)
    }
    
    def validate_fields(self):
        for field, check in self.UNATTACH_MESSAGE_FIELDS.items():
            self.assert_field_present(field)
            if not check(self[field]):
                raise InvalidField(field)
    
    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['to'] = ['all-participants']
            kwargs['model-version'] = '2'
        return kwargs
