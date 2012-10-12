"""Tests for vusion.persist.interaction."""

from twisted.trial.unittest import TestCase

from vusion.error import FailingModelUpgrade
from vusion.persist import Interaction

from tests.utils import ObjectMaker

class TestInteraction(TestCase, ObjectMaker):
    
    def test_validation_announcement(self):
        dialogue = self.mkobj_dialogue_annoucement()
        interaction = Interaction(**dialogue['interactions'][0])
        self.assertTrue(interaction is not None)

    def test_validation_open_question(self):
        dialogue = self.mkobj_dialogue_open_question()
        interaction = Interaction(**dialogue['interactions'][0])
        self.assertTrue(interaction is not None)
        
    def test_validation_closed_question(self):
        dialogue = self.dialogue_question
        interaction = Interaction(**dialogue['interactions'][0])
        self.assertTrue(interaction is not None)

    def test_validation_multi_keyword_question(self):
        dialogue = self.mkobj_dialogue_question_multi_keyword()
        interaction = Interaction(**dialogue['interactions'][0])
        self.assertTrue(interaction is not None)

    def test_upgrade_question_1_to_2(self):
        interaction = {
            'interaction-id': '01-01',
            'type-interaction': 'question-answer',
            'content': 'What is your name?',
            'keyword': 'name',
            'set-use-template': 'use-template',
            'type-question': 'open-question',
            'answer-label': 'name',
            'type-schedule': 'offset-days',
            'days': '1',
            'at-time': '22:30'}
        
        interaction = Interaction(**interaction)
        
        self.assertTrue(interaction is not None)
