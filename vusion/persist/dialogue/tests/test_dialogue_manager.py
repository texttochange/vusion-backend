import pymongo
from bson.objectid import ObjectId
from bson.timestamp import Timestamp

from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper
from vusion.persist import DialogueManager
from vusion.context import Context
from vusion.persist.action import Actions


class TestDialogueManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        c.safe = True
        db = c.test_program_db
        self.dialogue_manager = DialogueManager(db, 'dialogues')        
        self.clearData()

        #parameters:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)
        self.property_helper['timezone'] = 'Africa/Kampala'
        
        self.dialogue_manager.set_property_helper(self.property_helper)
    
    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.dialogue_manager.drop()

    def test_get_active_dialogues(self):
        dNow = self.property_helper.get_local_time()
        dPast1 = datetime.now() - timedelta(minutes=30)
        dPast2 = datetime.now() - timedelta(minutes=60)
        dPast3 = datetime.now() - timedelta(minutes=70)

        dialogue = self.mkobj_dialogue_question_answer()

        dialogue['dialogue-id'] = '1'
        dialogue['activated'] = 1
        dialogue['modified'] = dPast1
        id_active_dialogue_one = self.dialogue_manager.save(dialogue)

        dialogue.pop('_id')
        dialogue['dialogue-id'] = '1'
        dialogue['activated'] = 2
        dialogue['modified'] = dPast2
        self.dialogue_manager.save(dialogue)

        dialogue.pop('_id')
        dialogue['dialogue-id'] = '1'
        dialogue['activated'] = 0
        dialogue['modified'] = dPast3
        self.dialogue_manager.save(dialogue)

        dialogue.pop('_id')
        dialogue['dialogue-id'] = '2'
        dialogue['activated'] = 1
        dialogue['modified'] = dPast1
        id_active_dialogue_two = self.dialogue_manager.save(dialogue)

        dialogue.pop('_id')
        dialogue['dialogue-id'] = '2'
        dialogue['activated'] = 2
        dialogue['modified'] = dPast2
        self.dialogue_manager.save(dialogue)

        dialogue.pop('_id')
        dialogue['dialogue-id'] = '2'
        dialogue['activated'] = 0
        dialogue['modified'] = dPast2
        self.dialogue_manager.save(dialogue)

        dialogues = self.dialogue_manager._get_active_dialogues()
        self.assertEqual(len(dialogues), 2)
        self.assertEqual(dialogues[0]['_id'],
                         id_active_dialogue_one)
        self.assertEqual(dialogues[1]['_id'],
                         id_active_dialogue_two)    
    
    def test_get_matching_dialogue_actions(self):
        self.dialogue_manager.save(self.dialogue_question)
        actions = Actions()
        context = Context()
        self.dialogue_manager.get_matching_dialogue_actions('fel ok', actions, context)
        self.assertEqual(3, len(actions))
        self.assertTrue(context.is_matching())

    def test_get_dialogue_interaction(self):
        self.dialogue_manager.save(self.mkobj_dialogue_announcement_2())
        interaction = self.dialogue_manager.get_dialogue_interaction("2", "2")
        self.assertEqual("Today is the special day", interaction['content'])

    #TODO add 2 more dialogue one without, one with keywords
    def test_get_keywords(self):
        self.dialogue_manager.save(self.dialogue_question)
        self.assertEqual(['feel', 'fel'], self.dialogue_manager.get_all_keywords())
    
    def test_get_current_dialogue(self):
        dialogue = self.mkobj_dialogue_annoucement()
        dialogue['modified'] = Timestamp(datetime.now() - timedelta(minutes=1), 0)
        dialogue['activated'] = 2
        self.dialogue_manager.save(dialogue)
        other_dialogue = self.mkobj_dialogue_annoucement()
        other_dialogue['interactions'] = []
        self.dialogue_manager.save(other_dialogue)
        active_dialogue = self.dialogue_manager.get_current_dialogue("0")
        self.assertTrue(active_dialogue)
        self.assertEqual([], active_dialogue['interactions'])

    def test_load_dialogues(self):
        self.assertFalse(True)

    def test_load_dialogue(self):
        self.assertFalse(True)
