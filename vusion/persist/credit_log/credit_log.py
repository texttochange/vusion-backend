import re

from vusion.persist import Model
from vusion.error import VusionError


class CreditLog(Model):
    
    fields = {
        'date': {
            'required': True,
            '1_valid_string': lambda v: isinstance(v['date'], basestring),
            '2_valid_value': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)$'), v['date'])
            },
        'code': {
            'required': True,
            '1_not_none': lambda v: v['code'] is not None,
            '2_valid_format': lambda v: re.match(re.compile('^(\+[0-9]*|[0-9]*\-[0-9]*)$'), v['code']),
            },
        'incoming': {
            'required': True,
            'valid_int': lambda v: isinstance(v['incoming'], int),
            },
        'outgoing': {
            'required': True,
            'valid_int': lambda v: isinstance(v['outgoing'], int),
            },
        'outgoing-pending': {
            'required': False,
            'valid_int': lambda v: isinstance(v['outgoing-acked'], int),
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
        self._validate(self, CreditLog.fields)

    @staticmethod
    def instanciate(**kwargs):
        if 'object-type' not in kwargs:
            raise VusionError("CreditLog is abstract subtype not defined %r" % kwargs)
    
        if kwargs['object-type'] == 'garbage-credit-log':
            return GarbageCreditLog(**kwargs)
        elif kwargs['object-type'] == 'program-credit-log':
            return ProgramCreditLog(**kwargs)
        raise VusionError("%s not supported" % kwargs['object-type'])


class GarbageCreditLog(CreditLog):
    
    MODEL_TYPE = 'garbage-credit-log'
    MODEL_VERSION = '1'


class DeletedProgramCreditLog(CreditLog):
    
    MODEL_TYPE = 'deleted-program-credit-log'
    MODEL_VERSION = '1'

    fields = {
        'program-name': {
            'required': True,
            'not_none': lambda v: v['program-name'] is not None
        }
    }

    def validate_fields(self):
        super(ProgramCreditLog, self).validate_fields()
        self._validate(self, ProgramCreditLog.fields)    


class ProgramCreditLog(CreditLog):
    
    MODEL_TYPE = 'program-credit-log'
    MODEL_VERSION = '1'    
    
    fields = {
        'program-database': {
            'required': True,
            'not_none': lambda v: v['program-database'] is not None
        }
    }
    
    def validate_fields(self):
        super(ProgramCreditLog, self).validate_fields()
        self._validate(self, ProgramCreditLog.fields)
