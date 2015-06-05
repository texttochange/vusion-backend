"""Test of vusion.persist.content_variable.content_variable_manager"""
from pymongo import MongoClient

from twisted.trial.unittest import TestCase
from tests.utils import ObjectMaker

from vusion.persist import (
    ContentVariableManager, ContentVariable, ContentVariableTable)


class TestContentVariableManager(TestCase, ObjectMaker):

    def setUp(self):
        self.database_name = 'test_program_db'
        c = MongoClient(w=1)
        db = c.test_program_db
        self.manager = ContentVariableManager(
            db, 'content_variables', 'content_variable_tables')
        self.clearData()

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.manager.drop()

    def test_get_content_variable_from_match(self):
        match = {
            'domain': 'contentVariable',
            'key1': 'temperature',
            'key2': 'night'}
        content_variable_night = self.mkobj_content_variables_two_keys(
            key1='temperature', key2='night', value='10 C')
        self.manager.save_object(content_variable_night)

        content_variable_day = self.mkobj_content_variables_two_keys(
            key1='temperature', key2='day', value='20 C')
        self.manager.save_object(content_variable_day)

        content_variable_one = self.mkobj_content_variables_one_key(
            key1='temperature', value='40 C')
        self.manager.save_object(content_variable_one)

        matched_content_variable = self.manager.get_content_variable_from_match(match)

        self.assertEqual(
            '10 C',
            matched_content_variable['value'])

    def test_get_content_variable_from_match(self):
        match = {
            'domain': 'contentVariable',
            'key1': 'night',
            'key2': 'temperature',
            'key3': None}
        content_variable_night = self.mkobj_content_variables_two_keys(
            key1='temperature', key2='night', value='10 C')
        self.manager.save_object(content_variable_night)
        
        not_matched_content_variable = self.manager.get_content_variable_from_match(match)
        self.assertTrue(not_matched_content_variable is None)

    def test_save_content_variable_create_ok(self):
        match = {
            'key1': 'temperature',
            'key2': 'night'}
        self.manager.save_content_variable(
            match, '12 C')
        self.assertEqual(
            self.manager.get_value(match), '12 C')

    def test_save_content_variable_update_ok(self):
        table = self.mkobj_content_variable_two_key_table('Temperature')
        saved_table = self.manager.save_object(table)
        
        temperature_night = self.mkobj_content_variables_three_keys(
            key1='Nairobi', key2='2015/01/01', key3='temperature',
            value='10 C', table_id=str(saved_table))
        self.manager.save_object(temperature_night)

        match = {
            'key1': 'Nairobi',
            'key2': '2015/01/01',
            'key3': 'temperature'}
        self.manager.save_content_variable(
            match, '12 C')

        self.assertEqual(
            self.manager.get_value(match), '12 C')
        self.assertEqual( self.manager.count(), 1)
        table = self.manager.get_content_variable_table(str(saved_table))
        self.assertEqual(table.get_value(match), '12 C')

    def test_save_content_variable_create_row_ok(self):
        table = self.mkobj_content_variable_two_key_table('Temperature')
        saved_table = self.manager.save_object(table)
        
        temperature_night = self.mkobj_content_variables_three_keys(
            key1='Kisumu', key2='2015/01/01', key3='temperature',
            value='5 C', table_id=str(saved_table))
        self.manager.save_object(temperature_night)

        match = {
            'key1': 'Kisumu',
            'key2': '2015/01/01',
            'key3': 'temperature'}
        self.manager.save_content_variable(
            match, '9 C')

        self.assertEqual(
            self.manager.get_value(match), '9 C')
        self.assertEqual( self.manager.count(), 1)
        table = self.manager.get_content_variable_table(str(saved_table))
        self.assertEqual(table.get_value(match), '9 C')

    def test_save_content_variable_create_col_ok(self):
        table = self.mkobj_content_variable_two_key_table('Temperature')
        saved_table = self.manager.save_object(table)

        match = {
            'key1': 'Nairobi',
            'key2': '2015/01/01',
            'key3': 'precipitation'}
        self.manager.save_content_variable(
            match, '10mm', table_id=str(saved_table))

        self.assertEqual(
            self.manager.get_value(match), '10mm')
        self.assertEqual( self.manager.count(), 1)
        table = self.manager.get_content_variable_table(str(saved_table))
        self.assertEqual(table.get_value(match), '10mm')

    def test_exists_ok(self):
        match = {
            'key1': 'temperature',
            'key2': 'night'}
        content_variable_night = self.mkobj_content_variables_two_keys(
            key1='temperature', key2='night', value='10 C')
        self.manager.save_object(content_variable_night)

        self.assertTrue(self.manager.exits(match))

    def test_exists_fail(self):
        match = {
            'key1': 'temperature',
            'key2': 'night',
            'key3': 'something'}
        content_variable_night = self.mkobj_content_variables_two_keys(
            key1='temperature', key2='night', value='10 C')
        self.manager.save_object(content_variable_night)

        self.assertFalse(self.manager.exits(match))
