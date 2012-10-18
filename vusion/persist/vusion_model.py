from vusion.error import MissingField, FailingModelUpgrade

class VusionModel(object):

    MODEL_TYPE = None
    MODEL_VERSION = None
    
    fields = []
    
    def __init__ (self, **kwargs):
        if kwargs == {}:
            kwargs = self.create_instance()
        if 'model-version' not in kwargs:
            kwargs['model-version'] = '1'
        if kwargs['model-version'] != self.MODEL_VERSION:
            kwargs = self.upgrade(**kwargs)
            if kwargs['model-version'] != self.MODEL_VERSION:
                raise FailingModelUpgrade()
        for key in kwargs:
            kwargs[key] = self.process_field(key, kwargs[key])
        self.payload = kwargs
        self.validate_fields()

    def __eq__(self, other):
        if isinstance(other, VusionModel):
            return self.payload == other.payload
        return False
    
    def __str__(self):
        return "VusionModel:%s(%s) payload=%s" % (self.get_type(),
                                                  self.get_version(),
                                                  repr(self.payload))

    def __repr__(self):
        return str(self)

    def __getitem__(self, key):
        return self.payload[key]

    def __setitem__(self, key, value):
        self.payload[key] = value

    def process_field(self, key, value):
        return value

    def validate_fields(self):
        self.assert_field_present(*self.fields)
    
    def get_type(self):
        return self.MODEL_TYPE
    
    def get_version(self):
        return self.MODEL_VERSION
    
    def assert_field_present(self, *fields):
        for field in fields:
            if field not in self.payload:
                raise MissingField(field)

    def create_instance(self):
        new_instance = {'object-type': self.get_type(),
                      'model-version': self.get_version()}
        for key in self.fields:
            new_instance[key] = ''
        return new_instance

    def get_as_dict(self):
        instance_dict = {'object-type': self.get_type(),
                      'model-version': self.get_version()}
        for key in self.payload:
            instance_dict[key] = self.payload[key]
        return instance_dict
    
    def upgrade(self, **kwargs):
        return kwargs
    
    def is_already_saved(self):
        return '_id' in self.payload
