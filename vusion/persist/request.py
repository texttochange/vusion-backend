from vusion.persist import Model
from vusion.persist.action import Actions, action_generator, FeedbackAction

## TODO: update the validation
class Request(Model):

    MODEL_TYPE = 'request'
    MODEL_VERSION = '2'

    fields = ['keyword',
              'set-no-request-matching-try-keyword-only',
              'actions',
              'responses']

    def validate_fields(self):
        self.assert_field_present(*self.fields)

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['set-no-request-matching-try-keyword-only'] = 0
            kwargs['model-version'] = '2'
        return kwargs

    def is_lazy_matching(self):
        return (self.payload['set-no-request-matching-try-keyword-only']
                == 'no-request-matching-try-keyword-only')

    def append_actions(self, actions):
        for action in self.payload['actions']:
            actions.append(action_generator(**action))
        for response in self.payload['responses']:
            actions.append(FeedbackAction(**{'content': response['content']}))
        return actions

    def get_keywords(self):
        keyphrases = self['keyword'].split(',')
        keywords = []
        for keyphrase in keyphrases:
            keyphrase = keyphrase.strip()
            if not (keyphrase.split(' ')[0]) in keywords:
                keywords.append(keyphrase.split(' ')[0].lower())
        return keywords
