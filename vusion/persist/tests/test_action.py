from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vusion.persist.action import (
    Action, action_generator, ProportionalTagging, TaggingAction, 
    Participant, ProportionalLabelling, ProfilingAction, ResetAction,
    EnrollingAction, FeedbackAction, Actions, SmsInviteAction)
from vusion.error import MissingField, InvalidField, MissingData

from vusion.context import Context

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


class TestProportionalLabellingAction(TestCase):

    def test_set_count(self):
        action = {
            'type-action': 'proportional-labelling',
            'label-name': 'group',
            'proportional-labels': [
                {'label-value': 'control',
                 'weight': '2'},
                {'label-value': 'scenario1',
                 'weight': '1'}]
            }
        a = ProportionalLabelling(**action)
        a.set_count('scenario1', 300)
        self.assertEqual(
            a['proportional-labels'][1],
            {'label-value': 'scenario1',
             'weight': '1',
             'count': 300})

    def test_get_labelling_action(self):
        action = {
             'type-action': 'proportional-labelling',
             'label-name': 'group',
             'proportional-labels': [
                 {'label-value': 'control',
                  'weight': '2'},
                 {'label-value': 'scenario1',
                  'weight': '1'}]}
        a = ProportionalLabelling(**action)
        a.set_count('control', 5)
        a.set_count('scenario1', 2)
        self.assertEqual(
            a.get_labelling_action(),
            ProfilingAction(**{
                'label': 'group',
                'value': 'scenario1'}))

        a.set_count('control', 5)
        a.set_count('scenario1', 3)
        self.assertEqual(
            a.get_labelling_action(),
            ProfilingAction(**{
                'label': 'group',
                'value': 'control'}))


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
            {'tags': 'my tag',
             'profile': {'$elemMatch': {'label': 'name', 'value': 'olivier'}},
             'session-id': {'$ne': None},
             'phone': {'$ne': '06'}},             
            sms_forwarding_action.get_query_selector(participant, Context()))

    def test_get_query_selector_missing_data(self):
        action = {
            'type-action': 'sms-forwarding',
            'forward-content': 'hello',
            'forward-to': 'name:[participant.name]'
            }
        sms_forwarding_action = action_generator(**action)
        participant = Participant(**self.mkobj_participant(
            participant_phone='06',
            profile=[]))
        try:
            sms_forwarding_action.get_query_selector(participant, Context()),
            self.fail()
        except MissingData:
            return
        self.fail()
     
     
    def test_get_query_selector_message_condition(self):
        action = {
            'type-action': 'sms-forwarding',
            'forward-content': 'hello',
            'forward-to': 'my tag, name:[participant.name]',
            'set-forward-message-condition': 'forward-message-condition',
            'forward-message-condition-type': 'phone-number',
            'forward-message-no-participant-feedback': 'Some message'            
            }
        context = Context(**{'message': 'Answer 2561111'})
        sms_forwarding_action = action_generator(**action)
        participant = Participant(**self.mkobj_participant(
            participant_phone='06',
            profile=[{
                'label': 'name',
                'value': 'olivier'}]))
        self.assertEqual(
            {'$and': [
                {'phone': '+2561111'},
                {'phone': {'$ne': '06'}}],      
             'tags': 'my tag',
             'profile': {'$elemMatch': {'label': 'name', 'value': 'olivier'}},
             'session-id': {'$ne': None}},             
            sms_forwarding_action.get_query_selector(participant, context))

    def test_get_query_selector_message_condition_no_tag_label(self):
        action = {
            'type-action': 'sms-forwarding',
            'forward-content': 'hello',
            'forward-to': None,
            'set-forward-message-condition': 'forward-message-condition',
            'forward-message-condition-type': 'phone-number',
            'forward-message-no-participant-feedback': 'Some message'            
            }
        context = Context(**{'message': 'Answer 2561111'})
        sms_forwarding_action = action_generator(**action)
        participant = Participant(**self.mkobj_participant(
            participant_phone='06',
            profile=[{
                'label': 'name',
                'value': 'olivier'}]))
        self.assertEqual(
            {'$and': [
                {'phone': '+2561111'},
                {'phone': {'$ne': '06'}}],
             'session-id': {'$ne': None}},             
            sms_forwarding_action.get_query_selector(participant, context))


    def test_get_query_selector_message_condition_abscent(self):
        action = {
            'type-action': 'sms-forwarding',
            'forward-content': 'hello',
            'forward-to': 'my tag',
            'set-forward-message-condition': 'forward-message-condition',
            'forward-message-condition-type': 'phone-number',
            'forward-message-no-participant-feedback': 'Some message'            
            }
        context = Context(**{'message': 'Answer'})
        sms_forwarding_action = action_generator(**action)
        participant = Participant(**self.mkobj_participant(
            participant_phone='06',
            profile=[{
                'label': 'name',
                'value': 'olivier'}]))
        self.assertEqual(
            {'$and': [
                {'phone': ''},
                {'phone': {'$ne': '06'}}],
             'tags': 'my tag',
             'session-id': {'$ne': None}},             
            sms_forwarding_action.get_query_selector(participant, context))


class testActions(TestCase, ObjectMaker):
    
    def test_priority(self):
        actions = Actions()
        optin = action_generator(**{'type-action': 'optin'})
        enroll = action_generator(**{'type-action': 'enrolling', 'enroll': '1'})
        reset = action_generator(**{'type-action': 'reset'})
        feedback = action_generator(**{'type-action': 'feedback', 'content': 'hello'})

        actions.append(feedback)
        actions.append(optin)
        actions.append(reset)
        actions.append(enroll)

        self.assertEqual(actions[0].get_type(), 'optin')
        self.assertEqual(actions[1].get_type(), 'reset')
        self.assertEqual(actions[2].get_type(), 'enrolling')
        self.assertEqual(actions[3].get_type(), 'feedback')


class TestSmsInviteAction(TestCase, ObjectMaker):

    def test_generate_action(self):
        action = {
            'type-action': 'sms-invite',
            'message': 'iam inviting you',
            'invitee-tag': 'invited',
            'feedback-already-optin': 'already in the program'}
        a = action_generator(**action)
        self.assertTrue(isinstance(a, SmsInviteAction))
