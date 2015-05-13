from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.persist import ContentVariable


class TestContentVariableTable(TestCase, ObjectMaker):


    def test_get_value_two_keys_ok(self):
        cvt = self.mkobj_content_variable_one_key_table()
        match = {'key1': 'Mombasa',
                 'key2': 'temperature'}
        self.assertEqual('23 C', cvt.get_value(match))

    def test_get_value_three_keys_ok(self):
        cvt = self.mkobj_content_variable_two_key_table()
        match = {'key1': 'Nairobi',
                 'key2': '2015/01/01',
                 'key3': 'temperature'}
        self.assertEqual('12 C', cvt.get_value(match))

    def test_get_value_fail(self):
        cvt = self.mkobj_content_variable_two_key_table()
        match = {'key1': 'Nairobi',
                 'key2': '2015/01/04',
                 'key3': 'temperature'}
        self.assertEqual(None, cvt.get_value(match))

    def test_set_value_one_key_exists_ok(self):
        cvt = self.mkobj_content_variable_one_key_table()
        match = {'key1': 'Nairobi',
                 'key2': 'precipitation'}
        self.assertEqual(True, cvt.set_value(match, '2mm'))
        self.assertEqual('2mm', cvt.get_value(match))

    def test_set_value_one_key_new_row_ok(self):
        cvt = self.mkobj_content_variable_one_key_table()
        match = {'key1': 'Kisumu',
                 'key2': 'precipitation'}
        self.assertEqual(True, cvt.set_value(match, '10mm'))
        self.assertEqual('10mm', cvt.get_value(match))

    def test_set_value_new_col_ok(self):
        cvt = self.mkobj_content_variable_one_key_table()
        match = {'key1': 'Nairobi',
                 'key2': 'wind'}
        self.assertEqual(True, cvt.set_value(match, '4km/h'))
        self.assertEqual('4km/h', cvt.get_value(match))

    def test_set_value_exists_ok(self):
        cvt = self.mkobj_content_variable_two_key_table()
        match = {'key1': 'Nairobi',
                 'key2': '2015/01/01',
                 'key3': 'temperature'}
        self.assertEqual(True, cvt.set_value(match, '40 C'))
        self.assertEqual('40 C', cvt.get_value(match))

    def test_set_value_new_row_ok(self):
        cvt = self.mkobj_content_variable_two_key_table()
        match = {'key1': 'Nairobi',
                 'key2': '2015/01/05',
                 'key3': 'temperature'}
        self.assertEqual(True, cvt.set_value(match, '40 C'))
        self.assertEqual('40 C', cvt.get_value(match))

    def test_set_value_new_col_ok(self):
        cvt = self.mkobj_content_variable_two_key_table()
        match = {'key1': 'Nairobi',
                 'key2': '2015/01/05',
                 'key3': 'precipitation'}
        self.assertEqual(True, cvt.set_value(match, '10mm'))
        self.assertEqual('10mm', cvt.get_value(match))

    def test_set_value_new_col_ok(self):
        cvt = self.mkobj_content_variable_two_key_table()
        match = {'key1': 'Nairobi',
                 'key2': '2015/01/01',
                 'key3': 'precipitation'}
        self.assertEqual(True, cvt.set_value(match, '10mm'))
        self.assertEqual('10mm', cvt.get_value(match))

    def test_validation(self):
        pass