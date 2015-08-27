from bson import ObjectId

from vusion.persist import ModelManager, ContentVariable, ContentVariableTable


class ContentVariableManager(ModelManager):

    def __init__(self, db, collection_name, collection_name_table, **kwargs):
        super(ContentVariableManager, self).__init__(
            db, collection_name, **kwargs)
        self.collection.ensure_index('keys.key', background=True)

        if collection_name_table in db.collection_names():
            self.collection_table = db[collection_name_table]
        else:
            self.collection_table = db.create_collection(
                collection_name_table)

    def drop(self):
        self.collection.drop()
        self.collection_table.drop()

    def get_value(self, match):
        cv = self.get_content_variable_from_match(match)
        if cv is None:
            return None
        return cv['value']

    def get_content_variable_from_match(self, match):
        condition = self._from_match_to_condition(match)
        content_variable = self.collection.find_one(condition)
        if content_variable is None:
            return None
        return ContentVariable(**content_variable)

    def save_content_variable(self, match, value, table_id=None):
        cv = self.get_content_variable_from_match(match)
        if not cv is None:
            self._update_content_variable(match, value)
            self._update_table(match, cv, value)
            return True
        keys = [{'key': match['key1']}]
        if 'key2' in match and match['key2'] is not None:
            keys.append({'key': match['key2']})
            if 'key3' in match and match['key3'] is not None:
                keys.append({'key': match['key3']})
        cv = ContentVariable(**{
            'keys': keys,
            'table': table_id,
            'value': value})
        self.save_object(cv)
        self._update_table(match, cv, value)
        return True

    def _update_table(self, match, cv, value):
        if cv.belong_to_table():
            table_id = cv.get_table_id()
            table = self.get_content_variable_table(table_id)
            table.set_value(match, value)
            self.save_object(table)

    def get_content_variable_table(self, _id):
        table = self.collection_table.find_one({'_id': ObjectId(_id)})
        table = ContentVariableTable(**table)
        return table

    def save_object(self, instance):
        instance.validate_fields()
        if isinstance(instance, ContentVariableTable):
            c = self.collection_table
        elif isinstance(instance, ContentVariable):
            c = self.collection
        return c.save(instance.get_as_dict())

    def _update_content_variable(self, match, value):
        condition = self._from_match_to_condition(match)
        self.collection.update(condition, {'$set': {'value': value}})

    def exits(self, match):
        condition = self._from_match_to_condition(match)
        cursor = self.collection.find(condition)
        if cursor.count() == 0:
            return False
        return True

    def match_has_valid_table(self, match, table_id):
        condition = self._from_match_to_condition(match)
        condition['table'] = table_id
        cursor = self.collection.find(condition)
        if cursor.count() == 0:
            return False
        return True

    def _from_match_to_condition(self, match):
        condition = {'keys.0': {'key': match['key1']}}
        if 'key2' in match and match['key2'] is not None:
            condition['keys.1'] = {'key': match['key2']}
            if 'key3' in match and match['key3'] is not None:
                condition['keys.2'] = {'key': match['key3']}
        condition['keys'] = {'$size': len(condition)}
        return condition
