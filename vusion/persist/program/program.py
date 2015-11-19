from vusion.persist import Model


class Program(Model):

    MODEL_TYPE = 'program'
    MODEL_VERSION = '1'

    fields = {
        'name': {
            'required': True,
        },
        'url': {
            'required': True,
        },
        'database': {
            'required': True,
        },
        'status': {
            'required': True,
        },
    }

    def validate_fields(self):
        self._validate(self, Program.fields)

