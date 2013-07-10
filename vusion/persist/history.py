from vusion.persist.vusion_model import VusionModel
from vusion.error import InvalidField
import re


class History(VusionModel):

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

    #def validate_fields(self):
        #super(History, self).validate_fields()
        #for field, check in self.HISTORY_FIELDS.items():
            #self.assert_field_present(field)
            #if not check(self[field]):
                #raise InvalidField(field)
        #if self.is_message():
            #for field, check in self.MESSAGE_FIELDS.items():
                #self.assert_field_present(field)
                #if not check(self[field]):
                    #raise InvalidField(field)
            #for field, check in self.SPECIFIC_DIRECTION_FIELDS[self['message-direction']].items():
                #self.assert_field_present(field)
                #if not check(self[field]):
                    #raise InvalidField(field)
            #if self['message-direction'] == 'outgoing':
                #for field, check in self.SPECIFIC_STATUS_FIELDS[self['message-status']].items():
                    #self.assert_field_present(field)
                    #if not check(self[field]):
                        #raise InvalidField(field)


class MessageHistory(History):

    fields = {
        'message-content': {
            'required': True,
            'valid_value': lambda v: v['message-content'] is not None
            },
        'message-direction': {
            'required': True,
            'valid_value': lambda v: v['message-direction'] in ['incoming', 'outgoing'],
            'required_subfield': lambda v: getattr(v, 'required_subfields')(
                v['message-direction'],
                {'outgoing':['message-id', 'message-status'],
                 'incoming': []})
            },
        'message-id':{
            'required': False,
            'valid_value': lambda v: True
            },
        'message-status':{
            'required': False,
            'valid_value': lambda v: v['message-status'] in ['failed', 'pending', 'delivered', 'ack', 'nack', 'no-credit'],
            'required_subfield': lambda v: getattr(v, 'required_subfields') (
                v['message-status'],
                {'failed': ['failure-reason'],
                 'pending': [],
                 'delivered': [],
                 'ack': [],
                 'nack': [],
                 'no-credit': []})
            },
        'failure-reason': {
            'required': False,
            'valid_value': lambda v: v['failure-reason'] is not None
            },
        'message-credits':{
            'required': True,
            'valid_value': lambda v: isinstance(v['message-credits'], int)
        }
    }

    def is_message(self):
        return True
        
    def validate_fields(self):
        super(MessageHistory, self).validate_fields()        
        self._validate(self, MessageHistory.fields)


class DialogueHistory(MessageHistory):

    MODEL_TYPE = 'dialogue-history'
    MODEL_VERSION = '2'

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

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['message-credits'] = kwargs['message-credits'] if 'message-credits' in kwargs else 1
            kwargs['model-version'] = '2'
        return kwargs


class RequestHistory(MessageHistory):

    MODEL_TYPE = 'request-history'
    MODEL_VERSION = '2'

    fields = {
        'request-id':{
            'required': True,
            'valid_value': lambda v: v['request-id'] is not None
        }
    }

    def validate_fields(self):
        super(RequestHistory, self).validate_fields()
        self._validate(self, RequestHistory.fields)

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['message-credits'] = kwargs['message-credits'] if 'message-credits' in kwargs else 1
            kwargs['model-version'] = '2'
        return kwargs


class UnattachHistory(MessageHistory):

    MODEL_TYPE = 'unattach-history'
    MODEL_VERSION = '2'

    fields = {
        'unattach-id':{
            'required': True,
            'valid_value': lambda v: v['unattach-id'] is not None
        }
    }

    def validate_fields(self):
        super(UnattachHistory, self).validate_fields()
        self._validate(self, UnattachHistory.fields)

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['message-credits'] = kwargs['message-credits'] if 'message-credits' in kwargs else 1
            kwargs['model-version'] = '2'
        return kwargs


class UnmatchingHistory(MessageHistory):

    MODEL_TYPE = 'unmatching-history'
    MODEL_VERSION = '2'

    fields = {}

    def is_message(self):
        return True

    def validate_fields(self):
        super(UnmatchingHistory, self).validate_fields()

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['message-credits'] = kwargs['message-credits'] if 'message-credits' in kwargs else 1
            kwargs['model-version'] = '2'
        return kwargs


class OnewayMarkerHistory(History):

    MODEL_TYPE = 'oneway-marker-history'
    MODEL_VERSION = '1'

    fields = ['dialogue-id',
              'interaction-id']

    def is_message(self):
        return False

    def validate_fields(self):
        super(OnewayMarkerHistory, self).validate_fields()


class DatePassedMarkerHistory(History):

    MODEL_TYPE = 'datepassed-marker-history'
    MODEL_VERSION = '1'

    fields = ['dialogue-id',
              'interaction-id',
              'scheduled-date-time']

    def is_message(self):
        return False

    def validate_fields(self):
        super(DatePassedMarkerHistory, self).validate_fields()


class DatePassedActionMarkerHistory(History):
    
    MODEL_TYPE = 'datepassed-action-marker-history'
    MODEL_VERSION = '1'

    fields = ['action-type',
              'scheduled-date-time']

    def is_message(self):
        return False

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

    raise VusionError("%s not supported" % kwargs['object-type'])
