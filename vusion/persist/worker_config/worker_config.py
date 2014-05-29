from vusion.persist import Model


class WorkerConfig(Model):

    MODEL_TYPE = 'worker-config'
    MODEL_VERSION = '2'

    fields = ['name', 'class', 'config']

    def validate_fields(self):
        self.assert_field_present(*self.fields)

    def process_field(self, key, value):
        if key == '_id':
            return value
        if key == 'config':
            for config_param in value.keys():
                if not isinstance(value[config_param], int):
                    value[config_param] = value[config_param].encode('utf-8')
        else:
            value = value.encode('utf-8')
        return value

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            if kwargs['class'] == 'vusion.TtcGenericWorker':
                kwargs['class'] = 'vusion.DialogueWorker'
            kwargs['model-version'] = '2'
        return kwargs

    def create_instance(self):
        obj = super(WorkerConfig, self).create_instance()
        obj['config'] = {}
        return obj
