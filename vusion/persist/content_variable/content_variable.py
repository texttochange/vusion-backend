from vusion.persist import VusionModel

class ContentVariable(VusionModel):
    
    MODEL_TYPE = 'content_variable'
    MODEL_VERSION = '1'
    
    fields= {
         'keys': {
              'required': True
              },
         'value': {
               'required': True
               }
         }
    
    def validate_fields(self):
        self._validate(self, self.fields)