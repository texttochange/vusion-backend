import re

from vusion.persist import Model


class CreditLog(Model):
    
    MODEL_TYPE = 'credit-log'
    MODEL_VERSION = '1'
    
    fields = {
        'date': {
            'required': True,
            'valid_string': lambda v: isinstance(v['date'], basestring),
            'valid_value': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)$'), v['date'])
            },
        'program-database': {
            'required': True,
            'not_none': lambda v: v['program-database'] is not None
            },
        'code': {
            'required': True,
            'not_none': lambda v: v['code'] is not None
            },
        'incoming': {
            'required': True,
            'valid_int': lambda v: isinstance(v['incoming'], int),
            },
        'outgoing': {
            'required': True,
            'valid_int': lambda v: isinstance(v['outgoing'], int),
            },
        'outgoing-acked': {
            'required': False,
            'valid_int': lambda v: isinstance(v['outgoing-acked'], int),
            },
        'outgoing-nacked': {
            'required': False,
            'valid_int': lambda v: isinstance(v['outgoing-nacked'], int),
            },
        'outgoing-delivered': {
            'required': False,
            'valid_int': lambda v: isinstance(v['outgoing-delivered'], int),
            },
        'outgoing-failed': {
            'required': False,
            'valid_int': lambda v: isinstance(v['outgoing-failed'], int),
            }
        }

    def validate_fields(self):
        self._validate(self, self.fields)
