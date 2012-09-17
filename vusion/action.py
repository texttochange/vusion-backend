from vusion.error import MissingField, VusionError

class Action(object):

    ACTION_TYPE = None
    
    def __init__ (self, _process_fields=True, **kwargs):
        if _process_fields:
            kwargs = self.process_fields(kwargs)
        self.payload = kwargs
        self.validate_fields()

    def __eq__(self, other):
        if isinstance(other, Action):
            return self.payload == other.payload
        return False
    
    def __str__(self):
        return "Do:%s payload=%s" % (self.get_type(), repr(self.payload))

    def __repr__(self):
        return str(self)

    def __getitem__(self, key):
        return self.payload[key]

    def __setitem__(self, key, value):
        self.payload[key] = value

    def process_fields(self, fields):
        return fields

    def validate_fields(self):
        pass
    
    def get_type(self):
        return self.ACTION_TYPE
    
    def assert_field_present(self, *fields):
        for field in fields:
            if field not in self.payload:
                raise MissingField(field)
            

class OptinAction(Action):
    
    ACTION_TYPE = 'optin'

    def validate_fields(self):
        pass


class OptoutAction(Action):
    
    ACTION_TYPE = 'optout'
    
    def validate_fields(self):
        pass


class FeedbackAction(Action):
    
    ACTION_TYPE = 'feedback'
    
    def validate_fields(self):
        self.assert_field_present('content')


class UnMatchingAnswerAction(Action):
    
    ACTION_TYPE = 'unmatching-answer'
    
    def validate_fields(self):
        self.assert_field_present('answer')


class TaggingAction(Action):

    ACTION_TYPE = 'tagging'
    
    def validate_fields(self):
        self.assert_field_present('tag')


class EnrollingAction(Action):

    ACTION_TYPE = 'enrolling'
    
    def validate_fields(self):
        self.assert_field_present('enroll')


class ProfilingAction(Action):

    ACTION_TYPE = 'profiling'
    
    def validate_fields(self):
        self.assert_field_present('label', 'value')


class OffsetConditionAction(Action):
    
    ACTION_TYPE = 'offset-conditioning'
    
    def validate_fields(self):
        self.assert_field_present('interaction-id', 'dialogue-id')


def action_generator(**kwargs):
    # Condition to be removed when Dialogue structure freezed
    if 'type-action' not in kwargs:
        kwargs['type-action'] = kwargs['type-answer-action']
    if kwargs['type-action'] == 'optin':
        return OptinAction(**kwargs)
    elif kwargs['type-action'] == 'optout':
        return OptoutAction(**kwargs)
    elif kwargs['type-action'] == 'enrolling':
        return EnrollingAction(**kwargs)
    elif kwargs['type-action'] == 'tagging':
        return TaggingAction(**kwargs)
    elif kwargs['type-action'] == 'profiling':
        return ProfilingAction(**kwargs)
    elif kwargs['type-action'] == 'feedback':
        return FeedbackAction(**kwargs)
    elif kwargs['type-action'] == 'unmatching-answer':
        return UnMatchingAnswerAction(**kwargs)
    elif kwargs['type-action'] == 'offset-conditioning':
        return OffsetConditionAction(**kwargs)
    raise VusionError("%s not supported" % kwargs['type-answer-action'])


class Actions():
    
    def __init__(self):
        self.actions = []

    def append(self, action):
        if action.get_type == "optin" or action.get_type == "enrolling":
            self.actions.insert(0, action)
        else:
            self.actions.append(action)

    def contains(self, action_type):
        for action in self.actions:
            if action.get_type() == action_type:
                return True
        return False
    
    def items(self):
        return self.actions.__iter__()

    def __getitem__(self, key):
        return self.actions[key]