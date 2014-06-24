from vusion.utils import get_word


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
        return str(self.payload)

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
        return (self.payload['message'] if 'message' in self.payload else None)

    def get_message_keyword(self):
        return get_word(self.get_message(), position=0)
    
    def get_message_second_word(self):
        return get_word(self.get_message(), position=1)

    def get_dict_for_history(self, schedule=None):
        dict_for_history = {}
        if schedule is not None:
            dict_for_history.update({
                'object-type': schedule.get_history_type(),
                'participant-session-id': schedule['participant-session-id'],
            })
        if 'dialogue-id' in self.payload:
            dict_for_history.update({
                'dialogue-id': self.payload['dialogue-id'],
                'interaction-id': self.payload['interaction-id']})
            if 'matching-answer' in self.payload:
                dict_for_history.update({
                    'matching-answer': self.payload['matching-answer']})
        if 'request-id' in self.payload:
            dict_for_history.update({'request-id': self.payload['request-id']})
        if 'unattach-id' in self.payload:
            dict_for_history.update({'unattach-id': self.payload['unattach-id']})
        return dict_for_history
