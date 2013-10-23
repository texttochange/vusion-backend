

class ModelManager(object):

    def __init__(self, db, collection_name, **kwargs):
        self.property_helper = None
        self.log_helper = None
        if collection_name in db.collection_names():
            self.collection = db[collection_name]
        else:
            self.collection = db.create_collection(collection_name)        
    
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
        return self.property_helper.get_local_time(date_format)

    def log(self, msg, level='msg'):
        return self.set_log_helper.log(level, msg)
