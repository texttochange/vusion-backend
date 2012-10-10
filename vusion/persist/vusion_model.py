from vusion.error import MissingField

class VusionModel(object):

    MODEL_TYPE = None
    MODEL_VERSION = None
    
    def __init__ (self, **kwargs):
        if 'model-version' not in kwargs:
            kwargs['model-version'] = '1'
        if kwargs['model-version'] != self.MODEL_VERSION:
            kwargs = self.upgrade(**kwargs)
            if kwargs['model-version'] != self.MODEL_VERSION:
                raise FailModelUpgrade()
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

    def validate_fields(self):
        pass
    
    def get_type(self):
        return self.MODEL_TYPE
    
    def get_version(self):
        return self.MODEL_VERSION
    
    def assert_field_present(self, *fields):
        for field in fields:
            if field not in self.payload:
                raise MissingField(field)

    def get_as_dict(self):
        model_dict = {'object-type': self.get_type(),
                      'model-version': self.get_version()}
        for key in self.payload:
            model_dict[key] = self.payload[key]
        return model_dict
    
    def upgrade(self, **kwargs):
        return kwargs