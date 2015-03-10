from vusion.persist import Model
from vusion.utils import escape_nested, unescape_nested


class Export(Model):

    MODEL_TYPE = 'export'
    MODEL_VERSION = '1'

    fields = {
        'timestamp': {
            'required': True,
            '1_valid_string': lambda v: isinstance(v['timestamp'], basestring),
        },
        'database': {
            'required': True,
            '1_valid_string': lambda v: isinstance(v['database'], basestring),
        },
        'collection': {
            'required': True,
            '1_valid_string': lambda v: isinstance(v['collection'], basestring),
            '2_valid_value': lambda v: v['collection'] in ['history',
                                                           'participants',
                                                           'unmatchable_reply'],
        },
        'filters': {
            'required': True,
            '1_valid_string': lambda v: isinstance(v['filters'], dict),
        },        
        'conditions': {
            'required': True,
            #'1_valid_dict': lambda v: isinstance(v['conditions'], dict),
        },
        'order': {
            'required': True,
            #'1_valid_dict': lambda v: isinstance(v['order'], dict),
        },
        'filters': {
            'required': True,
            #'1_valid_string': lambda v: isinstance(v['filters'], dict),
        },
        'status': {
            'required': True,
            'valid_value': lambda v: v['status'] in ['queued',
                                                     'processing',
                                                     'success',
                                                     'failed',
                                                     'no-space'],
        },
        'failure-reason': {
            'required': False,
            #'1_valid_string': lambda v: isinstance(v['failure-reason'], basestring),
        },
        'size': {
            'required': True,
            'valid_int': lambda v: isinstance(v['size'], long),
        },
        'file-full-name': {
            'required': True,
            '1_valid_string': lambda v: isinstance(v['collection'], basestring),
        },
    }

    def validate_fields(self):
        self._validate(self, Export.fields)

    def is_participants_export(self):
        return self['collection'] == 'participants'

    def is_history_export(self):
        return self['collection'] == 'history'

    def is_unmatchable_reply_export(self):
        return self['collection'] == 'unmatchable_reply'

    def get_as_dict(self):
        as_dict = super(Export, self).get_as_dict()
        return escape_nested(as_dict, '\$')

    def before_validate(self):
        if (self.payload['conditions'] == []):
            self.payload['conditions'] = {}
        self.payload['conditions'] = unescape_nested(self.payload['conditions'], '$')
