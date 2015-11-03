"""Test for vusion.persist.ParticipantManager"""
from pymongo import MongoClient
from bson import ObjectId
from datetime import timedelta

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, maybeDeferred

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper, PrintLogger
from vusion.persist import ParticipantManager, Participant
from vusion.utils import time_to_vusion_format


class TestParticipantManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_program_db'
        c = MongoClient(w=1)
        self.db = c.test_program_db
        self.manager = ParticipantManager(self.db, 'participants')
        self.clearData()

        #parameters:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)
        self.property_helper['timezone'] = 'Africa/Kampala'
        
        self.manager.set_property_helper(self.property_helper)
        
    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.manager.drop()

    def test_opting_in_ok(self):
        self.assertTrue(self.manager.opting_in('1'))
        participant = self.manager.get_participant('1')
        self.assertEqual(
            participant['model-version'],
            Participant.MODEL_VERSION)

    def test_opting_in_ok_again(self):
        participant = self.mkobj_participant(
            '1', 
            session_id=None,
            tags=['geek'],
            profile=[{'label': 'name',
                      'value': 'Olivier'}],
            enrolled=[{'dialogue-id': '1', 'date-time': '2014-02-21T00:00:00'}])
        self.manager.save(participant)
        self.manager.opting_in('1')
        participant = self.manager.get_participant('1')
        self.assertTrue(participant['session-id'] is not None)
        self.assertEqual(participant['tags'], [])
        self.assertEqual(participant['profile'], [])
        self.assertEqual(participant['enrolled'], [])

    def test_opting_in_fail(self):
        participant = self.mkobj_participant(
            '1', 
            session_id='1')
        self.manager.save(participant)
        self.assertFalse(self.manager.opting_in('1'))

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
        self.assertTrue(participant.is_enrolled('1'))
        self.assertTrue(participant.is_enrolled('2'))

    def test_enrolling_participants(self):
        participant = self.mkobj_participant('1')
        self.manager.save(participant)
        participant = self.mkobj_participant('2')
        self.manager.save(participant)
        participant = self.mkobj_participant('3', session_id=None)
        self.manager.save(participant)

        self.manager.enrolling_participants({}, 'dialogueID')
        
        participant = self.manager.get_participant('1')
        self.assertTrue(participant.is_enrolled('dialogueID'))
        participant = self.manager.get_participant('2')
        self.assertTrue(participant.is_enrolled('dialogueID'))
        participant = self.manager.get_participant('3')
        self.assertFalse(participant.is_enrolled('dialogueID'))        

    def test_enrolling_participant_not_reversible_with_auto_enroll(self):
        participant = self.mkobj_participant(
            '1',
            tags=['geek'],
            enrolled=[{'dialogue-id': '1', 'date-time': '2014-01-01T10:10:00'}])
        self.manager.save(participant)

        self.manager.enrolling_participants({'tags': {'$ne': 'geek'}}, '1')
        
        participant = self.manager.get_participant('1')
        self.assertTrue(participant.is_enrolled('1'))
        
    def test_labelling(self):
        participant = self.mkobj_participant(
            '1', 
            profile=[{'label': 'name',
                      'value': 'Olivier'}])
        self.manager.save(participant)
        
        self.manager.labelling('1', 'name', 'Olivier', 'name Olivier')
        participant = self.manager.get_participant('1')
        self.assertEqual(
            [{'label': 'name','value': 'Olivier', 'raw': 'name Olivier'}],
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
        self.assertEqual(participants.count(), 1)
        participant = participants.next()
        self.assertEqual(participant['phone'], '1')
        self.assertTrue(isinstance(participant, Participant))

    def test_get_participants_fail(self):
        self.manager.save({'object-type': 'participant', 'phone': '06'})
        participants = self.manager.get_participants()
        self.assertEqual(participants.count(), 1)
        participant = participants.next()
        self.assertEqual(participant, None)

    def test_is_participant_tagged(self):
        participant = self.mkobj_participant(
            '06',
            tags=['geek', 'male'])
        self.manager.save(participant)

        self.assertTrue(self.manager.is_tagged('06', ['geek']))
        self.assertTrue(self.manager.is_tagged('06', ['geek', 'sometag']))
        self.assertFalse(self.manager.is_tagged('06', ['sometag']))

    def test_is_participant_labelled(self):
        participant = self.mkobj_participant(
            '06',
            profile=[{'label': 'name',
                     'value': 'Olivier'}])
        self.manager.save(participant)

        self.assertTrue(self.manager.is_labelled('06', 'name'))
        self.assertFalse(self.manager.is_labelled('06', 'age'))

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

    @inlineCallbacks
    def test_count_tag_async(self):
        participant = self.mkobj_participant(
            '1',
            tags=['geek', 'male'])
        self.manager.save(participant)
        participant = self.mkobj_participant(
                    '2',
                    tags=['geek'])
        self.manager.save(participant)

        count = yield self.manager.count_tag_async('geek')
        self.assertEqual(2, count)
        count = yield self.manager.count_tag_async('male')
        self.assertEqual(1, count)
        count = yield self.manager.count_tag_async('somethingelse')
        self.assertEqual(0, count)

    @inlineCallbacks
    def test_count_label_async(self):
        participant = self.mkobj_participant(
            '1',
            profile=[{'label': 'name', 'value': 'olivier'}])
        self.manager.save(participant)
        participant = self.mkobj_participant(
                    '2',
                    profile=[{'label': 'name', 'value': 'mark'},
                             {'label': 'age', 'value': '32'}])
        self.manager.save(participant)

        count = yield self.manager.count_label_async({'label': 'name', 'value': 'olivier'})
        self.assertEqual(count, 1)
        count = yield self.manager.count_label_async({'label': 'age', 'value': '31'})
        self.assertEqual(count, 0)

    @inlineCallbacks
    def test_get_labels(self):
        participant = self.mkobj_participant(
            '1',
            profile=[{'label': 'name', 'value': 'olivier'}])
        self.manager.save(participant)
        participant = self.mkobj_participant(
                    '2',
                    profile=[{'label': 'name', 'value': 'mark'},
                             {'label': 'age', 'value': '32'}])
        self.manager.save(participant)

        results = []
        labels = yield self.manager.get_labels()
        for result in labels:
            results.append(result)
        self.assertEqual(results, ['age', 'name'])

    def test_aggregate_count_per_day(self):
        now = self.property_helper.get_local_time()
        past_1_day = now - timedelta(days=1)
        past_2_day = now - timedelta(days=2)
        past_3_day = now - timedelta(days=3)

        participant = self.mkobj_participant(
            '1',
            last_optin_date=time_to_vusion_format(past_3_day),
            last_optout_date=time_to_vusion_format(past_2_day))
        self.manager.save(participant)

        participant = self.mkobj_participant(
            '2',
            last_optin_date=time_to_vusion_format(past_3_day))
        self.manager.save(participant)

        participant = self.mkobj_participant(
            '3',
            last_optin_date=time_to_vusion_format(now))
        self.manager.save(participant)

        self.manager.aggregate_count_per_day()
        results = []
        for result in self.db["participants_stats"].find():
            results.append(result)

        self.assertEqual( 
            [{'_id': past_3_day.strftime("%Y-%m-%d"),
              'value': {
                  'opt-in': 2.0,
                  'opt-out': 0.0}},
             {'_id': past_2_day.strftime("%Y-%m-%d"),
              'value': {
                  'opt-in': 2.0,
                  'opt-out': 0.0}},
             {'_id': past_1_day.strftime("%Y-%m-%d"),
              'value': {
                  'opt-in': 1.0,
                  'opt-out': 1.0}},
             {'_id': now.strftime("%Y-%m-%d"),
              'value': {
                  'opt-in': 2.0,
                  'opt-out': 1.0}}
             ],
            results)

        ##Once the stats collection is created only compute current day
        self.manager.remove({'phone': '1'})
        participant = self.mkobj_participant('4')
        self.manager.save(participant)
        self.manager.aggregate_count_per_day()

        results = []
        for result in self.db["participants_stats"].find():
            results.append(result)
        self.assertEqual( 
            [{'_id': past_3_day.strftime("%Y-%m-%d"),
              'value': {
                  'opt-in': 2.0,
                  'opt-out': 0.0}},
             {'_id': past_2_day.strftime("%Y-%m-%d"),
              'value': {
                  'opt-in': 2.0,
                  'opt-out': 0.0}},
             {'_id': past_1_day.strftime("%Y-%m-%d"),
              'value': {
                  'opt-in': 1.0,
                  'opt-out': 1.0}},
             {'_id': now.strftime("%Y-%m-%d"),
              'value': {
                  'opt-in': 3.0,
                  'opt-out': 0}}
             ],
            results)
