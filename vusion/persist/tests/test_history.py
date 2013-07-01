"""Tests for vusion.persist.history."""

from twisted.trial.unittest import TestCase

from vusion.persist import DialogueHistory, history_generator, UnattachHistory

from tests.utils import ObjectMaker


class TestDialogueHistory(TestCase, ObjectMaker):

    def test_validation_dialogue(self):
        history = self.mkobj_history_dialogue(
            '1',
            '1',
            '2012-10-10T15:25:12')
        dialogue_history = history_generator(**history)
        self.assertTrue(dialogue_history is not None)
        self.assertEqual(dialogue_history['object-type'], 'dialogue-history')
        self.assertEqual(dialogue_history['model-version'], DialogueHistory.MODEL_VERSION)

    def test_validation_unattach(self):
        history = self.mkobj_history_unattach(
            '1',
            '2012-10-10T15:15:12')
        unattach_history = history_generator(**history)
        self.assertTrue(unattach_history is not None)
        self.assertEqual(unattach_history['object-type'], 'unattach-history')
        self.assertEqual(unattach_history['model-version'], UnattachHistory.MODEL_VERSION)
