from vusion.persist.model_manager import ModelManager
from content_variable import ContentVariable


class ContentVariableManager(ModelManager):

    def __init__(self, db, collection_name, **kwargs):
        super(ContentVariableManager, self).__init__(db, collection_name, **kwargs)
        self.collection.ensure_index('keys.key', background=True)

    def get_content_variable_from_match(self, match):
        condition = {'keys.0': {'key': match['key1']}}
        if match['key2'] is not None:
            condition['keys.1'] = {'key': match['key2']}
            if match['key3'] is not None:
                condition['keys.2'] = {'key': match['key3']}
        condition['keys'] = {'$size': len(condition)}
        content_variable = self.collection.find_one(condition)
        if content_variable is None:
            return None
        return ContentVariable(**content_variable)
        