import re
from datetime import timedelta

from vusion.error import VusionError, InvalidField
from vusion.persist.action import action_generator
from vusion.persist.vusion_model import VusionModel
from vusion.context import Context
from vusion.utils import time_from_vusion_format


##TODO update the validation
class Schedule(VusionModel):

    fields = {
        'participant-phone': {
            'required': True,
            'valid_value': lambda v: v['participant-phone'] is not None
            },
        'participant-session-id': {
            'required': True
            },
        'date-time': {
            'required': True,
            'valid_value': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)T0?(\d+):0?(\d+)(:0?(\d+))$'), v['date-time'])
            },
        }

    def validate_fields(self):
        self._validate(self, Schedule.fields)

    def get_context(self):
        if 'context' not in self.payload:
            return Context()
        return Context(**self.payload['context'])

    def get_schedule_time(self):
        return time_from_vusion_format(self['date-time'])

    def is_message(self):
        return False

    def is_expired(self, local_time):
        return time_from_vusion_format(self['date-time']) < (local_time - timedelta(hours=1))

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['participant-session-id'] = None
            kwargs['model-version'] = '2'
        return kwargs


class MessageSchedule(Schedule):
    
    def is_message(self):
        return True


class DialogueSchedule(MessageSchedule):

    MODEL_TYPE = 'dialogue-schedule'
    MODEL_VERSION = '2'

    fields = {
        'dialogue-id': {
            'required': True,
            },
        'interaction-id':{
            'required': True,
            },
        }

    def validate_fields(self):
        super(DialogueSchedule, self).validate_fields()        
        self._validate(self, DialogueSchedule.fields)

    def get_history_type(self):
        return 'dialogue-history'


class DeadlineSchedule(Schedule):

    MODEL_TYPE = 'deadline-schedule'
    MODEL_VERSION = '2'

    fields = {
       'dialogue-id': {
            'required': True,
            },
        'interaction-id':{
            'required': True,
            },
        }

    def validate_fields(self):
        super(DeadlineSchedule, self).validate_fields()        
        self._validate(self, DeadlineSchedule.fields)


class ReminderSchedule(Schedule):

    MODEL_TYPE = 'reminder-schedule'
    MODEL_VERSION = '2'

    fields = {
       'dialogue-id': {
            'required': True,
            },
        'interaction-id':{
            'required': True,
            },
        }

    def validate_fields(self):
        super(ReminderSchedule, self).validate_fields()        
        self._validate(self, ReminderSchedule.fields)

    def get_history_type(self):
        return 'dialogue-history'


class UnattachSchedule(MessageSchedule):

    MODEL_TYPE = 'unattach-schedule'
    MODEL_VERSION = '2'

    fields = {
        'unattach-id': {
            'required': True
        }
    }

    def validate_fields(self):
        super(UnattachSchedule, self).validate_fields()
        self._validate(self, UnattachSchedule.fields)

    def get_history_type(self):
        return 'unattach-history'


class FeedbackSchedule(MessageSchedule):

    MODEL_TYPE = 'feedback-schedule'
    MODEL_VERSION = '2'

    fields = {
        'content': {
            'required': True,
            }, 
        'context': {
            'required': True,
            },
        }

    def validate_fields(self):
        super(FeedbackSchedule, self).validate_fields()
        self._validate(self, FeedbackSchedule.fields)

    def get_history_type(self):
        context = self.get_context()
        if 'dialogue-id' in context:
            return 'dialogue-history'
        elif 'request-id' in context:
            return 'request-history'
        return 'feedback-history'


class ActionSchedule(Schedule):

    MODEL_TYPE = 'action-schedule'
    MODEL_VERSION = '2'

    fields = {
        'action': {
            'required': True
            }, 
        'context': {
            'required': True
            },
    }

    def validate_fields(self):
        super(ActionSchedule, self).validate_fields()
        self._validate(self, ActionSchedule.fields)

    def get_action(self):
        return action_generator(**self.payload['action'])


def schedule_generator(**kwargs):
    if kwargs['object-type'] == 'dialogue-schedule':
        return DialogueSchedule(**kwargs)
    elif kwargs['object-type'] == 'deadline-schedule':
        return DeadlineSchedule(**kwargs)
    elif kwargs['object-type'] == 'reminder-schedule':
        return ReminderSchedule(**kwargs)
    elif kwargs['object-type'] == 'unattach-schedule':
        return UnattachSchedule(**kwargs)
    elif kwargs['object-type'] == 'feedback-schedule':
        return FeedbackSchedule(**kwargs)
    elif kwargs['object-type'] == 'action-schedule':
        return ActionSchedule(**kwargs)
    raise VusionError("%s not supported" % kwargs['object-type'])
