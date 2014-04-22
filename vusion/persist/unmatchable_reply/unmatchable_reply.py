import re

from vusion.persist import Model


class UnmatchableReply(Model):
    
    MODEL_TYPE = 'unmatchable-reply'
    MODEL_VERSION = '1'
    
    fields = {
        'participant-phone': {
            'required': True
            },
        'to': {
            'required': True
            } ,
        'direction': {
            'required': True,
            'valid_value': lambda v: v['direction'] in ['incoming', 'outgoing']
            },
        'message-content': {
            'required': True,
            },
        'timestamp': {
            'required': True,
            'valid_value': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)T0?(\d+):0?(\d+):0?(\d+)$'), v['timestamp'])
            },
    }
    
    def validate_fields(self):
        self._validate(self, self.fields)    