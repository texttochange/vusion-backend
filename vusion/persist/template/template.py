from vusion.persist import Model


class Template(Model):
    
    MODEL_TYPE = 'template'
    MODEL_VERSION = '1'
    
    fields = {
        'name': {
            'required': True
            },
        'type-template': {
            'required': True,
            'valid_value': lambda v: v['type-template'] in [
                'open-question',
                'closed-question',
                'unmatching-answer',
                'unmatching-keyword']
            },
        'template': {
            'required': True,
            }
        }

    def validate_fields(self):
        self._validate(self, self.fields)
