from vusion.persist.vusion_model import VusionModel
from vusion.error import InvalidField
import re

class History(VusionModel):
        
    HISTORY_FIELDS = {
        'timestamp': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)T0?(\d+):0?(\d+):0?(\d+)$'), v),
        'participant-phone': lambda v: v is not None,
        'participant-session-id': lambda v: True}

    MESSAGE_FIELDS = {
        'message-content': lambda v: v is not None,
        'message-direction': lambda v: v in ['incoming', 'outgoing']}

    SPECIFIC_DIRECTION_FIELDS = {
        'outgoing': {
            'message-id': lambda v: True,
            'message-status': lambda v: v in ['failed', 'pending', 'delivered']},
        'incoming': {}}
    
    SPECIFIC_STATUS_FIELDS = {
        'failed': {'failure-reason': lambda v: v is not None},
        'pending': {},
        'delivered': {}}

    def is_message(self):
        pass

    def validate_fields(self):
        super(History, self).validate_fields()
        for field, check in self.HISTORY_FIELDS.items():
            self.assert_field_present(field)
            if not check(self[field]):
                raise InvalidField(field)
        if self.is_message():
            for field, check in self.MESSAGE_FIELDS.items():
                self.assert_field_present(field)
                if not check(self[field]):
                    raise InvalidField(field)
            for field, check in self.SPECIFIC_DIRECTION_FIELDS[self['message-direction']].items():
                self.assert_field_present(field)
                if not check(self[field]):
                    raise InvalidField(field)
            if self['message-direction'] == 'outgoing':
                for field, check in self.SPECIFIC_STATUS_FIELDS[self['message-status']].items():
                    self.assert_field_present(field)
                    if not check(self[field]):
                        raise InvalidField(field)

class DialogueHistory(History):
    
    MODEL_TYPE = 'dialogue-history'
    MODEL_VERSION = '1'
    
    fields = ['dialogue-id',
              'interaction-id']
    
    def is_message(self):
        return True
    
    def validate_fields(self):
        super(DialogueHistory, self).validate_fields()


class RequestHistory(History):

    MODEL_TYPE = 'request-history'
    MODEL_VERSION = '1'
    
    fields = ['request-id']

    def is_message(self):
        return True

    def validate_fields(self):
        super(RequestHistory, self).validate_fields()


class UnattachHistory(History):

    MODEL_TYPE = 'unattach-history'
    MODEL_VERSION = '1'

    fields = ['unattach-id']

    def is_message(self):
        return True

    def validate_fields(self):
        super(UnattachHistory, self).validate_fields()


class UnmatchingHistory(History):

    MODEL_TYPE = 'unmatching-history'
    MODEL_VERSION = '1'

    fields = []

    def is_message(self):
        return True

    def validate_fields(self):
        super(UnmatchingHistory, self).validate_fields()


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
              'interaction-id']
    
    def is_message(self):
        return False

    def validate_fields(self):
        super(DatePassedMarkerHistory, self).validate_fields()


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
    
    raise VusionError("%s not supported" % kwargs['object-type'])
