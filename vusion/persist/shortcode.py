from vusion_model import VusionModel


class Shortcode(VusionModel):
    
    MODEL_TYPE = 'shortcode'
    MODEL_VERSION = '2'
    
    fields= {
        'shortcode': {
            'required': True,
            },
        'country': {
            'required': True,
            },
        'international-prefix': {
            'required': True,
            },
        'max-character-per-sms': {
            'required': True,
            'valid_value': lambda v: v['max-character-per-sms'] in [70, 140, 160]
            },
        'error-template': {
            'required': True
            },
        'support-customized-id': {
            'required': True,
            'valid_value': lambda v: v['support-customized-id'] in [0, 1]
            },
        'supported-internationally': {
            'required': True,
            'valid_value': lambda v: v['supported-internationally'] in [0, 1]
            }
        }
    
    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['max-character-per-sms'] = kwargs['max-character-per-sms'] if 'max-character-per-sms' in kwargs else 160
            kwargs['model-version'] = '2'
        return kwargs

    def validate_fields(self):
        self._validate(self, self.fields)
