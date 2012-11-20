"""Tests for vusion.persist.participant."""

from twisted.trial.unittest import TestCase

from vusion.persist import Participant

from tests.utils import ObjectMaker


class TestParticipant(TestCase, ObjectMaker):

    def test_upgrade(self):
        participant1 = { 
             "enrolled":[{"date-time" : "2012-11-07T11:53:46",
                          "dialogue-id" : "506c0fb487776" },
                         {"date-time" : "2012-11-07T11:53:47",
                          "dialogue-id" : "506e826a3fd44"}, 
                         { "date-time" : "2012-11-07T11:59:17",
                           "dialogue-id" : "506bece239b45" },
                         { "date-time" : "2012-11-07T12:04:02",
                           "dialogue-id" : "506e76b8bba86" },
                         { "date-time" : "2012-11-09T09:00:14",
                           "dialogue-id" : "506e88aeea51f" },
                         { "date-time" : "2012-11-14T01:00:01",
                           "dialogue-id" : "506e8280aa5f0" } ],
             "last-optin-date" : "2012-11-07T11:53:46",
             "model-version" : 1, 
             "phone" : "+255713840370", 
             "profile" : [ { "value" : "A", "label" : "type" } ], 
             "session-id" : "022fba874b664b7f87265be805cfaa50", 
             "tags" : [ "Mother updated week 5" ] }
        p = Participant(**participant1)
        self.assertEqual('2', p['model-version'])

    def test_upgrade_cake(self):
        participant_cake = {
            "model-version" : "2", 
            "object-type" : "participant", 
            "phone" : "+255654033486", 
            "session-id" : "ee29e5a2321f426cb52f19e1371cb32e", 
            "last-optin-date" : "2012-11-20T13:30:56",
            "enrolled" : [ ],
            "tags" : [ ],
            "profile" : [ ]}
        p = Participant(**participant_cake)
        self.assertEqual('2', p['model-version']) 

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
