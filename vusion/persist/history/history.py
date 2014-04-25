import re

from vusion.persist import Model
from vusion.error import InvalidField, VusionError


class History(Model):

    fields = {
        'timestamp': {
            'required': True,
            'valid_value': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)T0?(\d+):0?(\d+):0?(\d+)$'), v['timestamp'])
            },
        'participant-phone': {
            'required': True,
            'valid_value': lambda v: v['participant-phone'] is not None
            },
        'participant-session-id': {
            'required': True
            },
        }
        
    def is_message(self):
        return False

    def validate_fields(self):
        self._validate(self, History.fields)


class MessageHistory(History):

    fields = {
        'message-content': {
            'required': True,
            'valid_value': lambda v: v['message-content'] is not None
            },
        'message-direction': {
            'required': True,
            '1_valid_value': lambda v: v['message-direction'] in ['incoming', 'outgoing'],
            '2_required_subfield': lambda v: getattr(v, 'required_subfields')(
                v['message-direction'],
                {'outgoing':['message-id', 'message-status'],
                 'incoming': ['message-status']})
            },
        'message-id':{
            'required': False,
            'valid_value': lambda v: True
            },
        'message-status':{
            'required': False,
            '1_valid_value': lambda v: v['message-status'] in [
                'failed', 
                'pending', 
                'delivered', 
                'ack', 
                'nack', 
                'no-credit',
                'no-credit-timeframe',
                'missing-data',
                'received',
                'forwarded'],
            '2_required_subfield': lambda v: getattr(v, 'required_subfields') (
                v['message-status'],
                {'failed': ['failure-reason'],
                 'pending': [],
                 'delivered': [],
                 'ack': [],
                 'nack': [],
                 'no-credit': [],
                 'no-credit-timeframe': [],
                 'missing-data': ['missing-data'],
                 'received': [],
                 'forwarded': ['forwards']})
            },
        'failure-reason': {
            'required': False,
            'valid_value': lambda v: v['failure-reason'] is not None
            },
        'missing-data': {
            'required': False,
            'valid_value': lambda v: isinstance(v['missing-data'], list) 
            },
        'message-credits': {
            'required': True,
            'valid_value': lambda v: isinstance(v['message-credits'], int)
            },
        'forwards': {
            'required': False,
            'valid_forwards': lambda v: getattr(v, 'valid_forwards')(v['forwards']),
            }
        }
    
    forward_fields = {
        'status': {
            'required': True,
            'valid_value': lambda v: v['status'] in [
                'pending',
                'failed',
                'ack', 
                'nack']
            },
        'message-id': {
            'required': True,
            },
        'timestamp': {
            'required': True,
            'valid_value': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)T0?(\d+):0?(\d+):0?(\d+)$'), v['timestamp'])
            },
        'to-addr': {
            'required': True,
            }
        }

    def is_message(self):
        return True
        
    def validate_fields(self):
        super(MessageHistory, self).validate_fields()        
        self._validate(self, MessageHistory.fields)

    def valid_forwards(self, forwards):
        if len(forwards) < 1:
            raise InvalidField("Field forwards should have at least 1 element.")
        for forward in forwards:
            self._validate(forward, MessageHistory.forward_fields)
        return True

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['message-credits'] = kwargs['message-credits'] if 'message-credits' in kwargs else 1
            kwargs['model-version'] = '2'
            return self.upgrade(**kwargs)
        elif kwargs['model-version'] == '2':
            if kwargs['message-direction'] == 'incoming':
                kwargs['message-status'] = kwargs['message-status'] if 'message-status' in kwargs else 'received'
            kwargs['model-version'] = '3'
        return kwargs


class DialogueHistory(MessageHistory):

    MODEL_TYPE = 'dialogue-history'
    MODEL_VERSION = '3'

    fields = {
        'dialogue-id':{
            'required': True,
            'valid_value': lambda v: v['dialogue-id'] is not None
            },
        'interaction-id':{
            'required': True,
            'valid_value': lambda v: v['interaction-id'] is not None            
            }
        }

    def validate_fields(self):
        super(DialogueHistory, self).validate_fields()


class RequestHistory(MessageHistory):

    MODEL_TYPE = 'request-history'
    MODEL_VERSION = '3'

    fields = {
        'request-id':{
            'required': True,
            'valid_value': lambda v: v['request-id'] is not None
        }
    }

    def validate_fields(self):
        super(RequestHistory, self).validate_fields()
        self._validate(self, RequestHistory.fields)


class UnattachHistory(MessageHistory):

    MODEL_TYPE = 'unattach-history'
    MODEL_VERSION = '3'

    fields = {
        'unattach-id':{
            'required': True,
            'valid_value': lambda v: v['unattach-id'] is not None
        }
    }

    def validate_fields(self):
        super(UnattachHistory, self).validate_fields()
        self._validate(self, UnattachHistory.fields)


class FeedbackHistory(MessageHistory):
    
    MODEL_TYPE = 'feedback-history'
    MODEL_VERSION = '1'
    
    fields = {}
    
    def validate_fields(self):
        super(FeedbackHistory, self).validate_fields()


class UnmatchingHistory(MessageHistory):

    MODEL_TYPE = 'unmatching-history'
    MODEL_VERSION = '3'

    fields = {}

    def is_message(self):
        return True

    def validate_fields(self):
        super(UnmatchingHistory, self).validate_fields()


## Todo update the validation
class OnewayMarkerHistory(History):

    MODEL_TYPE = 'oneway-marker-history'
    MODEL_VERSION = '1'

    fields = ['dialogue-id',
              'interaction-id']

    def validate_fields(self):
        super(OnewayMarkerHistory, self).validate_fields()


## Todo update the validation
class DatePassedMarkerHistory(History):

    MODEL_TYPE = 'datepassed-marker-history'
    MODEL_VERSION = '1'

    fields = ['dialogue-id',
              'interaction-id',
              'scheduled-date-time']

    def validate_fields(self):
        super(DatePassedMarkerHistory, self).validate_fields()


## Todo update the validation
class DatePassedActionMarkerHistory(History):
    
    MODEL_TYPE = 'datepassed-action-marker-history'
    MODEL_VERSION = '1'

    fields = ['action-type',
              'scheduled-date-time']

    def validate_fields(self):
        super(DatePassedActionMarkerHistory, self).validate_fields()


def history_generator(**kwargs):
    if 'object-type' not in kwargs:
        if 'dialogue-id' in kwargs:
            kwargs['object-type'] = 'dialogue-history'
        elif 'request-id' in kwargs:
            kwargs['object-type'] = 'request-history'
        elif 'unattach-id' in kwargs:
            kwargs['object-type'] = 'unattach-history'
        else:
            kwargs['object-type'] = None

    if kwargs['object-type'] == 'dialogue-history':
        return DialogueHistory(**kwargs)
    elif kwargs['object-type'] == 'request-history':
        return RequestHistory(**kwargs)
    elif kwargs['object-type'] == 'unattach-history':
        return UnattachHistory(**kwargs)
    elif kwargs['object-type'] == 'unmatching-history':
        return UnmatchingHistory(**kwargs)
    elif kwargs['object-type'] == 'oneway-marker-history':
        return OnewayMarkerHistory(**kwargs)
    elif kwargs['object-type'] == 'datepassed-marker-history':
        return DatePassedMarkerHistory(**kwargs)
    elif kwargs['object-type'] == 'datepassed-action-marker-history':
        return DatePassedActionMarkerHistory(**kwargs)
    elif kwargs['object-type'] == 'feedback-history':
        return FeedbackHistory(**kwargs)
    raise VusionError("%s not supported" % kwargs['object-type'])
