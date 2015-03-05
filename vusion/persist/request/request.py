from vusion.utils import clean_keyword, get_first_msg_word
from vusion.persist import Model
from vusion.persist.action import Actions, action_generator, FeedbackAction


## TODO: update the validation
class Request(Model):

    MODEL_TYPE = 'request'
    MODEL_VERSION = '2'

    fields = {
        'keyword': {
            'required': True,
            'validate_value': lambda v: True
        },
        'set-no-request-matching-try-keyword-only': {
            'required': True,
            'validate_value': lambda v: True
        },
        'actions': {
            'required': True,
            'validate_actions': lambda v: getattr(v, 'validate_actions')(v['actions'])
        },
        'responses': {
            'required': True,
            'validate_responses': lambda v: getattr(v, 'validate_responses')(v['responses'])
        }}

    def validate_fields(self):
        super(Request, self).validate_fields()
        self._validate(self, Request.fields)

    def validate_actions(self, actions):
        for action in actions:
            self.actions.append(action_generator(**action))
        return True

    def validate_responses(self, responses):
        for response in responses:
            self.actions.append(FeedbackAction(**{'content': response['content']}))
        return True

    def __init__(self, **kwargs):
        self.actions = []
        super(Request, self).__init__(**kwargs)
        self.matching_keyphrases = []
        self.matching_keywords = []        
        self._list_matching()

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['set-no-request-matching-try-keyword-only'] = 0
            kwargs['model-version'] = '2'
        return kwargs

    def _list_matching(self):
        keyphrases = self['keyword'].split(',')
        keywords = []
        for keyphrase in keyphrases:
            keyphrase = clean_keyword(keyphrase.strip())
            self.matching_keyphrases.append(keyphrase)
            keyword = keyphrase.split(' ')[0]
            if not keyword in self.matching_keywords:
                self.matching_keywords.append(keyword)

    def is_lazy_matching(self):
        return (self.payload['set-no-request-matching-try-keyword-only']
                == 'no-request-matching-try-keyword-only')

    def append_actions(self, actions):
        actions.extend(self.actions)

    def get_actions(self):
        return self.actions

    def get_keywords(self):
        return self.matching_keywords

    def is_matching(self, msg, keyphrase_only=True):
        if keyphrase_only:
            clean_msg = clean_keyword(msg)
            if clean_msg in self.matching_keyphrases:
                return True
        else:
            msg_keyword = get_first_msg_word(msg)
            clean_msg_keyword = clean_keyword(msg_keyword)
            if self.is_lazy_matching() and clean_msg_keyword in self.matching_keywords:
                return True
        return False
