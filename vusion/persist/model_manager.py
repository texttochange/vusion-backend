from datetime import datetime


class ModelManager(object):

    def __init__(self, db, collection_name, **kwargs):
        self.property_helper = None
        self.log_helper = None
        if 'logger' in kwargs:
            self.log_helper = kwargs['logger']
        if collection_name in db.collection_names():
            self.collection = db[collection_name]
        else:
            self.collection = db.create_collection(collection_name)

    def close_connection(self):
        pass

    def save_object(self, instance):
        instance.validate_fields()
        return self.collection.save(instance.get_as_dict())

    #deprecated: name is confusing
    def save_document(self, document):
        document.validate_fields()
        return self.collection.save(document.get_as_dict())
    
    def set_property_helper(self, property_helper):
        self.property_helper = property_helper

    def set_log_helper(self, log_helper):
        self.log_helper = log_helper

    def __getattr__(self,attr):
        orig_attr = self.collection.__getattribute__(attr)
        if callable(orig_attr):
            def hooked(*args, **kwargs):
                result = orig_attr(*args, **kwargs)
                # prevent wrapped_class from becoming unwrapped
                if result == self.collection:
                    return self
                return result
            return hooked
        else:
            return orig_attr

    def get_local_time(self, date_format='datetime'):
        if self.property_helper is None:
            return datetime.now()
        return self.property_helper.get_local_time(date_format)

    def log(self, msg, level='msg'):
        if self.log_helper is not None:
            self.log_helper.log(msg, level)
