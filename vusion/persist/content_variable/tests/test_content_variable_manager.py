"""Test of vusion.persist.content_variable.content_variable_manager"""
import pymongo

from twisted.trial.unittest import TestCase
from tests.utils import ObjectMaker

from vusion.persist import ContentVariableManager

class TestContentVariableManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        c.safe = True
        db = c.test_program_db
        self.content_variable_manager = ContentVariableManager(db, 'content_variables')
        self.clearData()
    
    def tearDown(self):
        self.clearData()
        
    def clearData(self):
        self.content_variable_manager.drop()
    
    def test_get_content_variable_from_match(self):
        match = {
            'domain': 'contentVariable',
            'key1': 'temperature',
            'key2': 'night'}
        content_variable_night = self.mkobj_content_variables(
            key1='temperature', key2='night', value='10 C')
        self.content_variable_manager.save(content_variable_night)
        
        content_variable_day = self.mkobj_content_variables(
            key1='temperature', key2='day', value='20 C')
        self.content_variable_manager.save(content_variable_day)
        
        content_variable_one = self.mkobj_content_variables_one_key(
            key1='temperature', value='40 C')
        self.content_variable_manager.save(content_variable_one)
        
        matched_content_variable = self.content_variable_manager.get_content_variable_from_match(match)
        
        self.assertEqual(
            '10 C',
            matched_content_variable['value'])
        