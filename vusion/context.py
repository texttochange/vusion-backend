

class Context(object):
    
    def __init__(self, **kwargs):
        self.payload = kwargs

    def __eq__(self, other):
        if isinstance(other, Context):
            return self.payload == other.payload
        return False

    def __str__(self):
        return "Context %s" % repr(self.payload)

    def __repr__(self):
        return str(self)

    def __getitem__(self, key):
        if key not in self.payload:
            return None
        return self.payload[key]

    def __contains__(self, key):
        return key in self.payload

    def __setitem__(self, key, value):
        self.payload[key] = value

    def update(self, values):
        for key, value in values.items():
            self.payload[key] = value

    def is_matching_answer(self):
        return ('matching-answer' in self.payload 
                and self.payload['matching-answer'] is not None)
    
    def is_matching(self):
        return ('dialogue-id' in self.payload or
                'request-id' in self.payload)

    def get_message(self):
        return (self.payload['message'] if message in self.payload else None)

    def get_dict_for_history(self):
        if 'dialogue-id' in self.payload:
            result = {'dialogue-id': self.payload['dialogue-id'],
                    'interaction-id': self.payload['interaction-id']}
            if 'matching-answer' in self.payload:
                result.update({'matching-answer': self.payload['matching-answer']})
            return result
        if 'request-id' in self.payload:
            return {'request-id': self.payload['request-id']}
        if 'unattach-id' in self.payload:
            return {'unattach-id': self.payload['unattach-id']}
        return {}