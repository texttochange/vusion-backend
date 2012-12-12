from twisted.trial.unittest import TestCase

from vusion.persist import UnattachMessage, Participant

from tests.utils import ObjectMaker

class TestUnattachMessage(TestCase, ObjectMaker):
    
    def test_upgrade(self):
        unattach_raw = {
            'name' : 'test',
            'to': 'all participants',
            'content': 'a message',
            'type-schedule': 'fixed-time',
            'fixed-time': '2012-01-01T12:12:00'}
        
        unattach = UnattachMessage(**unattach_raw)

        self.assertTrue(unattach is not None)
        self.assertEqual(
            UnattachMessage.MODEL_VERSION,
            unattach['model-version'])
        self.assertEqual(
            ['all-participants'],
            unattach['to'])

        unattach_raw = {
            'object-type': 'unattached-message',
            'model-version': '1',
            'name' : 'test',
            'to': 'all participants',
            'content': 'a message',
            'type-schedule': 'fixed-time',
            'fixed-time': '2012-01-01T12:12:00'}
        
        unattach = UnattachMessage(**unattach_raw)

        self.assertTrue(unattach is not None)
        self.assertEqual(
            UnattachMessage.MODEL_VERSION,
            unattach['model-version'])

    def test_get_query(self):
        unattach = UnattachMessage(**self.mkobj_unattach_message_2(
            recipient=['all-participants']))
        self.assertEqual(
            {},
            unattach.get_selector_as_query())
        
        unattach = UnattachMessage(**self.mkobj_unattach_message_2(
            recipient=['a tag']))
        self.assertEqual(
            {'tags': 'a tag'},
            unattach.get_selector_as_query())
        
        unattach = UnattachMessage(**self.mkobj_unattach_message_2(
            recipient=['a tag', 'another tag', 'last tag']))
        self.assertEqual(
            {'$or': [{'tags': 'a tag'}, {'tags': 'another tag'}, {'tags': 'last tag'}]},
            unattach.get_selector_as_query())

        unattach = UnattachMessage(**self.mkobj_unattach_message_2(
            recipient=['city:kampala']))
        self.assertEqual(
            {'profile': {'$elemMatch' : {'label': 'city', 'value': 'kampala'}}},
            unattach.get_selector_as_query())
        
        unattach = UnattachMessage(**self.mkobj_unattach_message_2(
            recipient=['city:kampala', 'born:jinja']))
        self.assertEqual(
            {'$or': [
                {'profile': {'$elemMatch' : {'label': 'city', 'value': 'kampala'}}},
                {'profile': {'$elemMatch' : {'label': 'born', 'value': 'jinja'}}}]},
            unattach.get_selector_as_query())

    def test_is_selectable(self):
        participant = Participant(
            **self.mkobj_participant_v2(
                tags=['geek', 'cool'],
                profile=[{'label': 'city', 'value': 'kampala', 'raw': None}]))
        
        um_tag = UnattachMessage(**self.mkobj_unattach_message_2(
            recipient=['male']))
        self.assertFalse(um_tag.is_selectable(participant))

        um_all = UnattachMessage(**self.mkobj_unattach_message_2(
            recipient=['all-participants']))
        self.assertTrue(um_all.is_selectable(participant))

        um_tag = UnattachMessage(**self.mkobj_unattach_message_2(
            recipient=['geek']))
        self.assertTrue(um_tag.is_selectable(participant))
        
        um_profile = UnattachMessage(**self.mkobj_unattach_message_2(
            recipient=['city:kampala']))
        self.assertTrue(um_profile.is_selectable(participant))        
        