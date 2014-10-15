from twisted.trial.unittest import TestCase

from vusion.persist import UnattachedMessage, Participant

from tests.utils import ObjectMaker

class TestUnattachedMessage(TestCase, ObjectMaker):

    def test_upgrade(self):
        unattach_raw = {
            'name' : 'test',
            'to': 'all participants',
            'content': 'a message',
            'type-schedule': 'fixed-time',
            'fixed-time': '2012-01-01T12:12:00'}
        
        unattach = UnattachedMessage(**unattach_raw)

        self.assertTrue(unattach is not None)
        self.assertEqual(
            UnattachedMessage.MODEL_VERSION,
            unattach['model-version'])
        self.assertEqual(
            'all',
            unattach['send-to-type'])

        unattach_raw = {
            'object-type': 'unattached-message',
            'model-version': '1',
            'name' : 'test',
            'to': 'all participants',
            'content': 'a message',
            'type-schedule': 'fixed-time',
            'fixed-time': '2012-01-01T12:12:00'}
        
        unattach = UnattachedMessage(**unattach_raw)

        self.assertTrue(unattach is not None)
        self.assertEqual(
            UnattachedMessage.MODEL_VERSION,
            unattach['model-version'])
        
        unattach_raw = {
            'object-type': 'unattached-message',
            'model-version': '2',
            'name' : 'test',
            'to': ['a tag'],
            'content': 'a message',
            'type-schedule': 'fixed-time',
            'fixed-time': '2012-01-01T12:12:00'}
        
        unattach = UnattachedMessage(**unattach_raw)

        self.assertTrue(unattach is not None)
        self.assertEqual(
            UnattachedMessage.MODEL_VERSION,
            unattach['model-version'])
        self.assertFalse('to' is unattach)
        self.assertTrue('send-to-type' in unattach)
        self.assertEqual(unattach['send-to-type'], 'match')
        self.assertTrue('send-to-match-operator' in unattach)
        self.assertEqual(unattach['send-to-match-operator'], 'any')
        self.assertTrue('send-to-match-conditions' in unattach)
        self.assertEqual(unattach['send-to-match-conditions'], ['a tag'])
        self.assertTrue('created-by' in unattach)
        

    def test_upgrade_none(self):        
        unattach = UnattachedMessage(**{
            'object-type': None, 
            'name': 'Test 3', 
            'fixed-time': u'2012-12-14T00:00:00', 
            'model-version': None, 
            'content': u'something',
            'to': ['all-participants'], 
            'type-schedule': 'fixed-time'})
        self.assertTrue(unattach is not None)        

    def test_get_query_all(self):
        unattach = UnattachedMessage(
            **self.mkobj_unattach_message(send_to_type='all'))
        self.assertEqual(
            {},
            unattach.get_selector_as_query())
    
    def test_get_query_match_any(self):
        unattach = UnattachedMessage(
            **self.mkobj_unattach_message(send_to_type='match',
                                          send_to_match_operator='any',
                                          send_to_match_conditions=['a tag']))
        self.assertEqual(
            {'tags': 'a tag'},
            unattach.get_selector_as_query())
        
        unattach = UnattachedMessage(
            **self.mkobj_unattach_message(send_to_type='match',
                                          send_to_match_operator='any',
                                          send_to_match_conditions=['a tag', 'another tag', 'last tag']))
        self.assertEqual(
            {'$or': [{'tags': 'a tag'}, {'tags': 'another tag'}, {'tags': 'last tag'}]},
            unattach.get_selector_as_query())
        
        unattach = UnattachedMessage(
            **self.mkobj_unattach_message(send_to_type='match',
                                          send_to_match_operator='any',
                                          send_to_match_conditions=['city:kampala']))
        self.assertEqual(
            {'profile': {'$elemMatch' : {'label': 'city', 'value': 'kampala'}}},
            unattach.get_selector_as_query())
        
        unattach = UnattachedMessage(
            **self.mkobj_unattach_message(send_to_type='match',
                                            send_to_match_operator='any',
                                            send_to_match_conditions=['city:kampala', 'born:jinja']))
        self.assertEqual(
            {'$or': [
                {'profile': {'$elemMatch' : {'label': 'city', 'value': 'kampala'}}},
                {'profile': {'$elemMatch' : {'label': 'born', 'value': 'jinja'}}}]},
            unattach.get_selector_as_query())

        unattach = UnattachedMessage(
            **self.mkobj_unattach_message(
                send_to_type='match',
                send_to_match_operator="any", 
                send_to_match_conditions=["Daniel","name:gerald"]))
        self.assertEqual(
            {'$or': [
                {'tags': 'Daniel'},
                {'profile': {'$elemMatch' : {'label': 'name', 'value': 'gerald'}}}]},
            unattach.get_selector_as_query())
        

    def test_get_query_match_all(self):
        unattach = UnattachedMessage(
            **self.mkobj_unattach_message(send_to_type='match',
                                          send_to_match_operator='all',
                                          send_to_match_conditions=['city:kampala', 'born:jinja']))
        self.assertEqual(
            {'$and': [
                {'profile': {'$elemMatch' : {'label': 'city', 'value': 'kampala'}}},
                {'profile': {'$elemMatch' : {'label': 'born', 'value': 'jinja'}}}]},
            unattach.get_selector_as_query())       

    def test_get_query_phone(self):
            unattach = UnattachedMessage(
                **self.mkobj_unattach_message(send_to_type='phone',
                                              send_to_phone=['+256788601462']))
            self.assertEqual(
                {'phone': {'$in': ['+256788601462']}},
                unattach.get_selector_as_query())

    def test_is_selectable_phone(self):
        selectable_participant = Participant(**self.mkobj_participant_v2(
            participant_phone='+06'))
        not_selectable_participant = Participant(**self.mkobj_participant_v2(
            participant_phone='+07'))
        
        unattach = UnattachedMessage(
            **self.mkobj_unattach_message(send_to_type='phone',
                                          send_to_phone=['+06']))

        self.assertTrue(unattach.is_selectable(selectable_participant))        
        self.assertFalse(unattach.is_selectable(not_selectable_participant))

    def test_is_selectable_all(self):
        participant = Participant(
            **self.mkobj_participant_v2(
                tags=['geek', 'cool'],
                profile=[{'label': 'city', 'value': 'kampala', 'raw': None}]))

        um_all = UnattachedMessage(
            **self.mkobj_unattach_message())
        self.assertTrue(um_all.is_selectable(participant))        

    def test_is_selectable_match_all(self):
        participant = Participant(
            **self.mkobj_participant_v2(
                tags=['geek', 'cool'],
                profile=[{'label': 'city', 'value': 'kampala', 'raw': None}]))

        um_tag = UnattachedMessage(
            **self.mkobj_unattach_message(
                send_to_type='match',
                send_to_match_operator='all',
                send_to_match_conditions=['male']))
        self.assertFalse(um_tag.is_selectable(participant))

        um_tag = UnattachedMessage(
            **self.mkobj_unattach_message(
                send_to_type='match',
                send_to_match_operator='all',
                send_to_match_conditions=['geek', 'cool']))
        self.assertTrue(um_tag.is_selectable(participant))

        um_profile = UnattachedMessage(
            **self.mkobj_unattach_message(
                send_to_type='match',
                send_to_match_operator='all',
                send_to_match_conditions=['geek', 'city:kampala']))
        self.assertTrue(um_profile.is_selectable(participant))

    def test_is_selectable_match_any(self):
        participant = Participant(
            **self.mkobj_participant_v2(
                tags=['geek', 'cool'],
                profile=[{'label': 'city', 'value': 'kampala', 'raw': None}]))

        um_tag = UnattachedMessage(
            **self.mkobj_unattach_message(
                send_to_type='match',
                send_to_match_operator='any',
                send_to_match_conditions=['male']))
        self.assertFalse(um_tag.is_selectable(participant))

        um_tag = UnattachedMessage(
            **self.mkobj_unattach_message(
                send_to_type='match',
                send_to_match_operator='any',
                send_to_match_conditions=['male', 'geek']))
        self.assertTrue(um_tag.is_selectable(participant))

        um_profile = UnattachedMessage(
            **self.mkobj_unattach_message(
                send_to_type='match',
                send_to_match_operator='any',
                send_to_match_conditions=['male', 'city:kampala']))
        self.assertTrue(um_profile.is_selectable(participant))
