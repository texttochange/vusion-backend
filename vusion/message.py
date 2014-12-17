from vumi.errors import MissingMessageField, InvalidMessageField
from vumi.message import Message


class DispatcherControl(Message):

    def validate_fields(self):
        self.assert_field_present('action', 'exposed_name')

    def get_routing_endpoint(self):
        return 'default'


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
        'reload_program_settings': {},
        'mass_tag':{
            'tag': lambda v: v is not None,
            'selector': lambda v: v is None or isinstance(v, dict)},
        'mass_untag': {
            'tag': lambda v: v is not None},
        'run_actions': {
            'participant_phone': lambda v: v is not None,
            'dialogue_id': lambda v: v is not None,
            'interaction_id': lambda v: v is not None,
            'answer': lambda v: v is not None}
    }

    def validate_fields(self):
        self.assert_field_present('action')
        if self['action'] not in self.ACTION_TYPES:
            raise MissingMessageField(self['action'])
        for field, check in self.ACTION_TYPES[self['action']].items():
            self.assert_field_present(field)
            if not check(self[field]):
                raise InvalidMessageField(self[field])

    def get_routing_endpoint(self):
        return 'default'


class MultiWorkerControl(Message):

    CONTROL_TYPES = {
        'add_worker': {
            'worker_name': lambda v: v is not None,
            'worker_class': lambda v: v is not None,
            'config': lambda v: v is not None},
        'remove_worker': {
            'worker_name': lambda v: v is not None}
    }

    def validate_fields(self):
        self.assert_field_present('message_type')
        if self['message_type'] not in self.CONTROL_TYPES:
            raise MissingMessageField(self['action'])
        for field, check in self.CONTROL_TYPES[self['message_type']].items():
            self.assert_field_present(field)
            if not check(self[field]):
                raise InvalidMessageField(self[field])

    def get_routing_endpoint(self):
        return 'default'
