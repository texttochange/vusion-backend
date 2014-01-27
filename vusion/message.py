from vumi.errors import MissingMessageField, InvalidMessageField
from vumi.message import Message


class DispatcherControl(Message):

    def validate_fields(self):
        self.assert_field_present('action', 'exposed_name')


class WorkerControl(Message):

    ACTION_TYPES = {
        'update_schedule': {
            'schedule_type': lambda v: v in ['dialogue', 'unattach', 'participant'],
            'object_id': lambda v: v is not None},
        'test_send_all_messages': {
            'dialogue_obj_id': lambda v: v is not None,
            'phone_number': lambda v: v is not None},
        'reload_request': {
            'object_id': lambda v: v is not None},
        'update_registered_keywords': {},
        'reload_program_settings': {}
    }

    def validate_fields(self):
        self.assert_field_present('action')
        if self['action'] not in self.ACTION_TYPES:
            raise MissingMessageField(self['action'])
        for field, check in self.ACTION_TYPES[self['action']].items():
            self.assert_field_present(field)
            if not check(self[field]):
                raise InvalidMessageField(self[field])
