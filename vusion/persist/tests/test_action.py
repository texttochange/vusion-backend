from twisted.trial.unittest import TestCase

from vusion.persist.action import (Action, action_generator,
                                   ProportionalTagging, TaggingAction, 
                                   Participant)
from vusion.error import MissingField, InvalidField

from tests.utils import ObjectMaker


class TestAction(TestCase, ObjectMaker):
    
    def test_upgrade_action_noversion_to_current(self):
        action = {'type-action': 'optin'}
        a = action_generator(**action)
        self.assertEqual(a.get_type(),'optin')
        self.assertEqual(Action.MODEL_VERSION, a['model-version'])


    def test_upgrade_action_version_1_to_current(self):
        action = {
            'content': 'As we are not able to recognize your pregnancy month, you will be register as 3 month pregnant.',
            'object-type': u'action',
            'type-action': 'feedback',
            'model-version': '1'}
        a = action_generator(**action)
        self.assertEqual(a.get_type(),'feedback')
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
            'condition-operator': 'all-subconditions',
            'subconditions': [{
                'subcondition-field': 'labelled',
                'subcondition-operator': 'with',
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
            'condition-operator': 'all-subconditions',
            'subconditions': [{
                'subcondition-field': 'tagged',
                'subcondition-operator': 'with',
                'subcondition-parameter': 'geek',}],
            'type-action': 'tagging',
            'tag': 'mytag'}
        a = action_generator(**action)
        self.assertEqual(
            a.get_condition_mongodb(),
            {'tags': 'geek'})
    
    def test_get_condition_mongodb_twosubcondition_all(self):
        action = {
            'model-type': 'action',
            'model-version': '2',            
            'set-condition': 'condition',
            'condition-operator': 'all-subconditions',
            'subconditions': [
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'with',
                 'subcondition-parameter': 'geek'},
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'not-with',
                 'subcondition-parameter': 'french'}],
            'type-action': 'tagging',
            'tag': 'mytag'}
        a = action_generator(**action)
        self.assertEqual(
            a.get_condition_mongodb(),
            {'$and': [{'tags': 'geek'},
                      {'tags': {'$ne': 'french'}}]})

    def test_get_condition_mongodb_twosubcondition_any(self):
        action = {
            'model-type': 'action',
            'model-version': '2',
            'set-condition': 'condition',
            'condition-operator': 'any-subconditions',
            'subconditions': [
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'with',
                 'subcondition-parameter': 'geek'},
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'not-with',
                 'subcondition-parameter': 'french'}],
            'type-action': 'tagging',
            'tag': 'mytag'}
        a = action_generator(**action)
        self.assertEqual(
            a.get_condition_mongodb(),
            {'$or': [{'tags': 'geek'},
                      {'tags': {'$ne': 'french'}}]})

    def test_get_condition_mongodb_onesubconditionLabel(self):
            action = {
                'model-type': 'action',
                'model-version': '2',            
                'set-condition': 'condition',
                'condition-operator': 'all-subconditions',
                'subconditions': [{
                    'subcondition-field': 'labelled',
                    'subcondition-operator': 'with',
                    'subcondition-parameter': 'gender:male',}],
                'type-action': 'tagging',
                'tag': 'mytag'}
            a = action_generator(**action)
            self.assertEqual(
                a.get_condition_mongodb(),
                {'profile': {'$elemMatch': {'label': 'gender',
                                            'value': 'male'}}})


    def test_get_condition_mongodb_for(self):
        action = {
            'model-type': 'action',
            'model-version': '2',
            'set-condition': 'condition',
            'condition-operator': 'all-subconditions',
            'subconditions': [{
                'subcondition-field': 'tagged',
                'subcondition-operator': 'with',
                'subcondition-parameter': 'geek',}],
            'type-action': 'tagging',
            'tag': 'mytag'}
        a = action_generator(**action)
        self.assertEqual(
            a.get_condition_mongodb_for('08', '1'),
            {'phone': '08',
             'session-id': '1',
             'tags': 'geek'})
        
    def test_get_condition_mongodb_for_all(self):
        action = {
            'model-type': 'action',
            'model-version': '2',
            'set-condition': 'condition',
            'condition-operator': 'all-subconditions',
            'subconditions': [
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'with',
                 'subcondition-parameter': 'geek'},
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'with',
                 'subcondition-parameter': 'cool'}],
            'type-action': 'tagging',
            'tag': 'mytag'}
        a = action_generator(**action)
        self.assertEqual(
            a.get_condition_mongodb_for('08', '1'),
            {'$and': [{'phone': '08','session-id': '1'},
                      {'tags': 'geek'},
                      {'tags': 'cool'}]})

    def test_get_condition_mongodb_for_any(self):
        action = {
            'model-type': 'action',
            'model-version': '2',
            'set-condition': 'condition',
            'condition-operator': 'any-subconditions',
            'subconditions': [
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'with',
                 'subcondition-parameter': 'geek'},
                {'subcondition-field': 'tagged',
                 'subcondition-operator': 'with',
                 'subcondition-parameter': 'cool'}],
            'type-action': 'tagging',
            'tag': 'mytag'}
        a = action_generator(**action)
        self.assertEqual(
            a.get_condition_mongodb_for('08', '1'),
            {'$and': [{'phone': '08','session-id': '1'},
                      {'$or': [{'tags': 'geek'},
                               {'tags': 'cool'}]}]})


class TestProportionalTaggingAction(TestCase):

    def test_set_tag_count(self):
        action = {
            'type-action': 'proportional-tagging',
            'proportional-tags': [
                {'tag': 'group 1',
                 'weight': '2'},
                {'tag': 'group 2',
                 'weight': '1'}]
            }
        a = ProportionalTagging(**action)
        a.set_tag_count('group 1', 300)
        self.assertEqual(
            a['proportional-tags'][0],
            {'tag': 'group 1',
             'weight': '2',
             'count': 300})
        
    def test_get_tagging_action(self):
        action = {
            'type-action': 'proportional-tagging',
            'proportional-tags': [
                {'tag': 'group 1',
                 'weight': '2'},
                {'tag': 'group 2',
                 'weight': '1'}]
        }
        a = ProportionalTagging(**action)
        a.set_tag_count('group 1', 10)
        a.set_tag_count('group 2', 4)
        self.assertEqual(
            a.get_tagging_action(),
            TaggingAction(**{'tag': 'group 2'}))

    def test_get_tagging_action_2(self):
        action = {
            'type-action': 'proportional-tagging',
            'proportional-tags': [
                {'tag': 'group 1',
                 'weight': '2'},
                {'tag': 'group 2',
                 'weight': '1'}]
        }
        a = ProportionalTagging(**action)
        a.set_tag_count('group 1', 10)
        a.set_tag_count('group 2', 5)
        self.assertEqual(
            a.get_tagging_action(),
               TaggingAction(**{'tag': 'group 1'}))


class TestSmsForwardingAction(TestCase, ObjectMaker):
    
    def test_generate_action(self):
        action = {
            'type-action': 'sms-forwarding',
            'forward-content': 'hello',
            'forward-to': 'my tag'
            }
        a = action_generator(**action)
        self.assertTrue(True)

    def test_get_query_selector(self):
        action = {
            'type-action': 'sms-forwarding',
            'forward-content': 'hello',
            'forward-to': 'my tag, name:[participant.name]'
            }
        sms_forwarding_action = action_generator(**action)
        participant = Participant(**self.mkobj_participant(
            participant_phone='06',
            profile=[{
                'label': 'name',
                'value': 'olivier'}]))
        self.assertEqual(
            {'$and': [{'tags': 'my tag'},
                      {'profile': {'$elemMatch': {'label': 'name', 'value': 'olivier'}}}],
             'session-id': {'$ne': None},
             'phone': {'$ne': '06'}},             
            sms_forwarding_action.get_query_selector(participant))
        