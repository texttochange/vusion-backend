from twisted.trial.unittest import TestCase

from vusion.persist.action import Action, action_generator
from vusion.error import MissingField, InvalidField

from tests.utils import ObjectMaker


class TestAction(TestCase, ObjectMaker):
    
    def test_validation_action(self):
        action = {'type-action': 'optin'}
        a = action_generator(**action)
        self.assertEqual(a.get_type(),'optin')        
        self.assertEqual(Action.MODEL_VERSION, a['model-version'])    

    def test_validation_action_fail(self):
        action = {
            'type-action': 'delayed-enrolling',
            'enroll': '1',
            'offset-days': {'days': '0'}}
        try:
            a = action_generator(**action)
            self.assertTrue(False, "Validation should check at-time field")
        except MissingField as e:
            self.assertTrue(True)

    def test_validation_action_fail_condition(self):
        action = {
            'model-type': 'action',
            'model-version': '2',
            'set-condition': 'condition',
            'type-action': 'tagging',
            'tag': 'mytag',}
        try:
            a = action_generator(**action)
            self.assertTrue(False, "Validation should check a conditions field")
        except MissingField as e:
            self.assertTrue(True)

    def test_validation_action_success(self):
        action = {
            'model-type': 'action',
            'model-version': '2',            
            'set-condition': 'condition',
            'condition-operator': 'all',
            'subconditions': [{
                'subcondition-field': 'labelled',
                'subcondition-operator': 'in',
                'subcondition-parameter': 'type:geek',}],
            'type-action': 'tagging',
            'tag': 'mytag'}
        a = action_generator(**action)
        self.assertTrue(True)

    def test_get_condition_mongodb_nosubcondition(self):
           action = {
               'type-action': 'tagging',
               'tag': 'mytag'}
           a = action_generator(**action)
           self.assertEqual(
               a.get_condition_mongodb(),
               {})

    def test_get_condition_mongodb_onesubcondition_all(self):
        action = {
            'model-type': 'action',
            'model-version': '2',            
            'set-condition': 'condition',
            'condition-operator': 'all',
            'subconditions': [{
                'subcondition-field': 'tagged',
                'subcondition-operator': 'in',
                'subcondition-parameter': 'geek',}],
            'type-action': 'tagging',
            'tag': 'mytag'}
        a = action_generator(**action)
        self.assertEqual(
            a.get_condition_mongodb(),
            {'tags': {'$in': 'geek'}})
    
    def test_get_condition_mongodb_twosubcondition_all(self):
        action = {
            'model-type': 'action',
            'model-version': '2',            
            'set-condition': 'condition',
            'condition-operator': 'all',
            'subconditions': [
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'in',
                 'subcondition-parameter': 'geek'},
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'not-in',
                 'subcondition-parameter': 'french'}],
            'type-action': 'tagging',
            'tag': 'mytag'}
        a = action_generator(**action)
        self.assertEqual(
            a.get_condition_mongodb(),
            {'$and': [{'tags': {'$in': 'geek'}},
                      {'tags': {'$in': {'$ne': 'french'}}}]})

    def test_get_condition_mongodb_twosubcondition_any(self):
        action = {
            'model-type': 'action',
            'model-version': '2',
            'set-condition': 'condition',
            'condition-operator': 'any',
            'subconditions': [
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'in',
                 'subcondition-parameter': 'geek'},
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'not-in',
                 'subcondition-parameter': 'french'}],
            'type-action': 'tagging',
            'tag': 'mytag'}
        a = action_generator(**action)
        self.assertEqual(
            a.get_condition_mongodb(),
            {'$or': [{'tags': {'$in': 'geek'}},
                      {'tags': {'$in': {'$ne': 'french'}}}]})

    def test_get_condition_mongodb_onesubconditionLabel(self):
            action = {
                'model-type': 'action',
                'model-version': '2',            
                'set-condition': 'condition',
                'condition-operator': 'all',
                'subconditions': [{
                    'subcondition-field': 'labelled',
                    'subcondition-operator': 'in',
                    'subcondition-parameter': 'gender:male',}],
                'type-action': 'tagging',
                'tag': 'mytag'}
            a = action_generator(**action)
            self.assertEqual(
                a.get_condition_mongodb(),
                {'profile': {'$elemMatch': {'label': 'gender',
                                            'value': 'male'}}})
