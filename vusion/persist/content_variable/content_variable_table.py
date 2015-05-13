from vusion.persist import Model

class ContentVariableTable(Model):

    MODEL_TYPE = 'content_variable_table'
    MODEL_VERSION = '1'

    fields= {
         'name': {
              'required': True
              },
         'columns': {
             'required': True
             }
         }

    def validate_fields(self):
        self._validate(self, self.fields)

    def _find_indexes(self, match):
        key1_indexes = self._get_indexes(self['columns'][0], match['key1'])
        if not key1_indexes:
            return None
        if 'key3' in match:
            key2_indexes = self._get_indexes(self['columns'][1], match['key2'])
            if len(key1_indexes & key2_indexes) == 0:
                return None
            row_index = (key1_indexes & key2_indexes).pop()
            col_index = self._get_column_index(match['key3'])
        else:
            row_index = key1_indexes.pop()
            col_index = self._get_column_index(match['key2'])
        if col_index is None or row_index is None:
            return None
        return {'col_index': col_index, 'row_index': row_index}

    def get_value(self, match):
        indexes = self._find_indexes(match)
        if indexes is None:
            return None
        return self._get_index_value(indexes['col_index'],
                                     indexes['row_index'])

    def set_value(self, match, value, upsert=True):
        indexes = self._find_indexes(match)
        if indexes is None:
            if not upsert:
                False
            self._add_match(match)
            indexes = self._find_indexes(match)
        self._set_index_value(indexes['col_index'],
                              indexes['row_index'],
                              value)
        return True

    ## function that will add the necessary col or row for a match
    def _add_match(self, match):
        if 'key3' not in match: 
            if not self._get_indexes(self['columns'][0], match['key1']):
                self._create_row(match['key1'], None)
            if not self._get_column_index(match['key2']):
                self._create_column(match['key2'])
        else:
            key1_indexes = self._get_indexes(self['columns'][0], match['key1'])
            key2_indexes = self._get_indexes(self['columns'][1], match['key2'])
            if len(key1_indexes & key2_indexes) == 0:
                self._create_row(match['key1'], match['key2'])
            if not self._get_column_index(match['key3']):
                self._create_column(match['key3'])

    def _create_column(self, key):
        index = self._count_columns()
        values = []
        for i in range(0, self._count_rows()):
            values.append(None)
        self['columns'].append(
            {'header': key,
             'values': values,
             'validation': None,
             'type': 'contentvariable'})

    def _create_row(self, key1, key2):
        index = 1
        self['columns'][0]['values'].append(key1)
        if not key2 is None:
            self['columns'][1]['values'].append(key2)
            index = 2
        for i in range(index, self._count_columns()):
            self['columns'][i]['values'].append(None)

    def _count_columns(self):
        return len(self['columns'])

    def _count_rows(self):
        return len(self['columns'][0]['values'])

    def _get_index_value(self, col_index, row_index):
        return self['columns'][col_index]['values'][row_index]

    def _set_index_value(self, col_index, row_index, value):
        self['columns'][col_index]['values'][row_index] = value

    def _get_indexes(self, column, key):
        indexes = set()
        for i,x in enumerate(column['values']):
            if x == key:
                indexes.add(i)
        return indexes

    def _get_column_index(self, key):
        for i, col in enumerate(self['columns']):
            if col['header'] == key:
                return i
        return None
