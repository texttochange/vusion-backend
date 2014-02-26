"""Test for vusion.persist.ParticipantManager"""
import pymongo
from bson import ObjectId

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper
from vusion.persist import ParticipantManager, Participant


class TestParticipantManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        c.safe = True
        db = c.test_program_db
        self.manager = ParticipantManager(db, 'participants')
        self.clearData()

        #parameters:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)
        self.property_helper['timezone'] = 'Africa/Kampala'
        
        self.manager.set_property_helper(self.property_helper)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.manager.drop()

    def test_opting_in(self):
        participant_id = self.manager.opting_in('1')
        self.assertTrue(isinstance(participant_id, ObjectId))
        participant = self.manager.get_participant('1')
        self.assertEqual(
            participant['model-version'],
            Participant.MODEL_VERSION)

    def test_opting_in_again(self):
        participant = self.mkobj_participant(
            '1', 
            session_id=None,
            tags=['geek'],
            profile=[{'label': 'name',
                      'value': 'Olivier'}],
            enrolled=[{'dialogue-id': '1', 'date-time': '2014-02-21T00:00:00'}])
        self.manager.save(participant)
        self.manager.opting_in_again('1')
        participant = self.manager.get_participant('1')
        self.assertTrue(participant['session-id'] is not None)
        self.assertEqual(participant['tags'], [])
        self.assertEqual(participant['profile'], [])
        self.assertEqual(participant['enrolled'], [])

    def test_opting_out(self):
        self.manager.opting_in('1')
        self.manager.opting_out('1')
        participant = self.manager.get_participant('1')
        self.assertTrue(participant['session-id'] is None)

    def test_tagging(self):
        participant = self.mkobj_participant(
            '1', tags=['geek'])
        self.manager.save(participant)
        
        self.manager.tagging('1', 'geek')
        self.manager.tagging('1', 'french')
        participant = self.manager.get_participant('1')
        self.assertEqual(['geek', 'french'], participant['tags'])

    def test_enrolling(self):
        participant = self.mkobj_participant(
            '1',
            enrolled=[{'dialogue-id': '1', 'date-time': '2014-02-21T00:00:00'}])
        self.manager.save(participant)

        #already enrolled, not modification
        self.manager.enrolling('1', '1')
        participant = self.manager.get_participant('1')
        self.assertEqual(
            [{'dialogue-id': '1', 'date-time': '2014-02-21T00:00:00'}],
            participant['enrolled'])
        
        #not enrolled
        self.manager.enrolling('1', '2')
        participant = self.manager.get_participant('1')
        self.assertTrue(participant.is_enrolled('2'))

    #TODO: check default behavior, shouldn't be like tagging?
    def test_labelling(self):
        participant = self.mkobj_participant(
            '1', 
            profile=[{'label': 'name',
                      'value': 'Olivier'}])
        self.manager.save(participant)
        
        self.manager.labelling('1', 'name', 'Olivier', 'name Olivier')
        participant = self.manager.get_participant('1')
        self.assertEqual(
            [{'label': 'name','value': 'Olivier', 'raw': None},
             {'label': 'name','value': 'Olivier', 'raw': 'name Olivier'}],
            participant['profile'])

    def test_save_transport_medadata(self):
        participant = self.mkobj_participant('1')
        self.manager.save(participant)
        self.manager.save_transport_metadata('1', {'token': '11'})
        participant = self.manager.get_participant('1')
        self.assertEqual(
            {'token': '11'},
            participant['transport_metadata'])
        

    def test_get_participant(self):
        self.manager.save(self.mkobj_participant('1'))
        self.assertTrue(isinstance(self.manager.get_participant('1'), Participant))
        self.assertTrue(self.manager.get_participant('2') is None)

    def test_get_participants(self):
        self.manager.save(self.mkobj_participant('1', tags=['geek']))
        self.manager.save(self.mkobj_participant('2', tags=[]))
        participants = self.manager.get_participants({'tags': 'geek'})
        self.assertEqual(len(participants), 1)
        self.assertEqual(participants[0]['phone'], '1')
        self.assertTrue(isinstance(participants[0], Participant))

    def test_is_participant_tagged(self):
        participant = self.mkobj_participant(
            '06',
            tags=['geek', 'male'])
        self.manager.save(participant)

        self.assertTrue(self.manager.is_tagged('06', ['geek']))
        self.assertTrue(self.manager.is_tagged('06', ['geek', 'sometag']))
        self.assertFalse(self.manager.is_tagged('06', ['sometag']))

    def test_is_participant_optin(self):
        self.manager.save(self.mkobj_participant('1'))
        self.manager.save(self.mkobj_participant('2', session_id=None))
        self.assertEqual(self.manager.is_optin('1'), True)
        self.assertEqual(self.manager.is_optin('2'), False)
        self.assertEqual(self.manager.is_optin('3'), False)

    def test_is_matching(self):
        participant = self.mkobj_participant(
            '1',
            tags=['geek'])
        self.manager.save(participant)

        self.assertTrue(self.manager.is_matching({'phone': '1', 'tags': 'geek'}))
        self.assertFalse(self.manager.is_matching({'phone': '1', 'tags': 'male'}))

    def test_count_tag(self):
        participant = self.mkobj_participant(
            '1',
            tags=['geek', 'male'])
        self.manager.save(participant)
        participant = self.mkobj_participant(
                    '2',
                    tags=['geek'])
        self.manager.save(participant)

        self.assertEqual(2, self.manager.count_tag('geek'))
        self.assertEqual(1, self.manager.count_tag('male'))
        self.assertEqual(0, self.manager.count_tag('somethingelse'))        
