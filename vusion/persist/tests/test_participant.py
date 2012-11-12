"""Tests for vusion.persist.participant."""

from twisted.trial.unittest import TestCase

from vusion.persist import Participant

from tests.utils import ObjectMaker


class TestParticipant(TestCase, ObjectMaker):
    
    def test_validation_fail(self):
        participant= Participant(**self.mkobj_participant(profile=[{'label': 'gender',
                                                       'value': 'Female'}]))        
        self.assertIsInstance(participant, Participant)
    
    def test_is_enrolled(self):
        pass
    
    def test_get_label(self):
        participant = Participant(**self.mkobj_participant())
        self.assertEqual(participant.get_participant_label_value('gender'), None)
        
        participant = Participant(
            **self.mkobj_participant(profile=[{'label': 'gender',
                                               'value': 'Female'}]))
        self.assertEqual(participant.get_participant_label_value('gender'), 'Female')

        participant = Participant(
            **self.mkobj_participant(profile=[{'label': 'Some thing label',
                                               'value': 'some value'}]))
        self.assertEqual(
            participant.get_participant_label_value('Some thing label'),
            'some value')

    def test_get_label_raw(self):
        participant = Participant(
            **self.mkobj_participant(profile=[{'label': 'Some thing label',
                                               'value': 'some value'}]))
        self.assertEqual(
            participant.get_participant_label_value('Some thing label_raw'),
            None)                
        
        participant = Participant(
            **self.mkobj_participant_v2(profile=[{'label': 'Some thing label',
                                               'value': 'some value',
                                               'raw': 'keyword 1 other content'}]))
        self.assertEqual(
            participant.get_participant_label_value('Some thing label_raw'),
            'keyword 1 other content')
        
        participant = Participant(
            **self.mkobj_participant_v2(profile=[{'label': 'Month of Pregnancy',
                                                  'value': 'some value',
                                                  'raw': 'keyword 1 other content'}]))
        self.assertEqual(
            participant.get_participant_label_value('Month of Pregnancy_raw'),
            'keyword 1 other content')        
