
from vumi.message import Message


class DispatcherControl(Message):

    def validate_fields(self):
        self.assert_field_present('action', 'exposed_name')


class WorkerControl(Message):
    
    def validate_fields(self):
        self.assert_field_present('action')
    