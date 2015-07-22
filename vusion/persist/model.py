import json
from vusion.error import MissingField, FailingModelUpgrade, InvalidField


class Model(object):

    MODEL_TYPE = None
    MODEL_VERSION = None

    fields = []

    def __init__(self, **kwargs):
        if kwargs == {}:
            kwargs = self.create_instance()
        if 'model-version' not in kwargs:
            kwargs['model-version'] = '1'
        if kwargs['model-version'] != self.MODEL_VERSION:
            kwargs = self.upgrade(**kwargs)
            if kwargs['model-version'] != self.MODEL_VERSION:
                raise FailingModelUpgrade(kwargs)
        for key in kwargs:
            kwargs[key] = self.process_field(key, kwargs[key])
        self.payload = kwargs
        self.before_validate()
        self.validate_fields()

    def __eq__(self, other):
        if isinstance(other, Model):
            return self.payload == other.payload
        return False

    def __str__(self):
        return "VusionModel:%s(%s) payload=%s" % (self.get_type(),
                                                  self.get_version(),
                                                  repr(self.payload))

    def __repr__(self):
        return str(self)

    def __getitem__(self, key):
        try:
            return self.payload[key]
        except KeyError:
            None

    def __setitem__(self, key, value):
        self.payload[key] = value

    def __contains__(self, key):
        return key in self.payload

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
        new_instance = {
            'object-type': self.get_type(),
            'model-version': self.get_version()}
        for key in self.fields:
            new_instance[key] = ''
        return new_instance

    def get_as_dict(self):
        instance_dict = {
            'object-type': self.get_type(),
            'model-version': self.get_version()}
        for key in self.payload:
            instance_dict[key] = self.payload[key]
        return instance_dict

    def get_as_json(self):
        return json.dumps(self.get_as_dict())

    def upgrade(self, **kwargs):
        return kwargs

    def is_already_saved(self):
        return '_id' in self.payload

    def before_validate(self):
        if self.payload['transport_metadata'] == []:
            self.payload['transport_metadata'] = {}
        pass    

    def _validate(self, data, field_rules):
        for field, rules in field_rules.items():
            if rules['required'] is False and not field in data:
                continue
            elif rules['required'] and not field in data:
                raise MissingField("%s is missing" % field)
            for rule_name, rule in iter(sorted(rules.iteritems())):
                if rule_name is 'required':
                    continue
                if not rule(data):
                    raise InvalidField("%s=%s is not %s" % (field, data[field], rule_name))

    def required_subfields(self, field, subfields):
        if field is None:
            return True
        for subfield in subfields[field]:
            if not subfield in self:
                raise MissingField(subfield)
        return True
