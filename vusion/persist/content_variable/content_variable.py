from vusion.persist import VusionModel

class ContentVariable(VusionModel):
    
    MODEL_TYPE = 'content_variable'
    MODEL_VERSION = '2'
    
    fields= {
         'keys': {
              'required': True
              },
         'table': {
             'required': True
             },
         'value': {
               'required': True
               }
         }

    def validate_fields(self):
        self._validate(self, self.fields)
        
    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['table'] = kwargs['table'] if 'table' in kwargs else {}
            kwargs['model-version'] = '2'
        return kwargs