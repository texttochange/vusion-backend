from content_variable import ContentVariable


class ContentVariableManager(object):
    
    
    def __init__(self, db, collection_name):
        self.properties = None
        if collection_name in db.collection_names():
            self.collection = db[collection_name]
        else:
            self.collection = db.create_collection(collection_name)
        self.collection.ensure_index('keys.key', background=True)

    def set_property_helper(self, property_helper):
        self.property_helper = property_helper

    def __getattr__(self, attr):
        orig_attr = self.collection.__getattribute__(attr)
        if callable(orig_attr):
            def hooked(*args, **kwargs):
                result = orig_attr(*args, **kwargs)
                if result == self.collection:
                    return self
                return result
            return hooked
        else:
            return orig_attr

    def get_content_variable_from_match(self, match):
        condition = {'keys':[{'key':match['key1']}]}
        if match['key2'] is not None:
            condition['keys'].append({'key':match['key2']})
        condition = {'$and':[condition]}
        condition['$and'].append({'keys':{'$size': len(condition['$and'][0]['keys'])}})
        content_variable = self.collection.find_one(condition)
        if content_variable is None:
            return None
        return ContentVariable(**content_variable)
        