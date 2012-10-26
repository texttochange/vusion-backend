from vumi.errors import MissingMessageField, InvalidMessageField
from vumi.message import Message


class DispatcherControl(Message):

    def validate_fields(self):
        self.assert_field_present('action', 'exposed_name')


class WorkerControl(Message):

    SCHEDULE_TYPE = {
        'dialogue': {'dialogue_id': lambda v: v is not None},
        'unattach': {'unattach_id': lambda v: v is not None}}

    ACTION_TYPES = {
        'update_schedule': {'schedule_type': lambda v: WorkerControl.SCHEDULE_TYPE.has_key(v)},
        'test_send_all_messages': {
            'dialogue_obj_id': lambda v: v is not None,
            'phone_number': lambda v: v is not None,
        }}

    def validate_fields(self):
        self.assert_field_present('action')
        if self['action'] not in self.ACTION_TYPES:
            raise MissingMessageField(self['action'])
        for field, check in self.ACTION_TYPES[self['action']].items():
            self.assert_field_present(field)
            if not check(self[field]):
                raise InvalidMessageField(self[field])
            if field == 'schedule_type':
                for field, check in self.SCHEDULE_TYPE[self[field]].items():
                    self.assert_field_present(field)
                    if not check(self[field]):
                        raise InvalidMessageField(self[field])
