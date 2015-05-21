import pymongo
from bson.objectid import ObjectId
from bson.timestamp import Timestamp

from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper, PrintLogger
from vusion.persist import DialogueManager, Dialogue
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

        dialogues = self.dialogue_manager.get_active_dialogues()
        self.assertEqual(len(dialogues), 2)
        self.assertEqual(dialogues[0]['_id'], id_active_dialogue_one)
        self.assertEqual(dialogues[1]['_id'], id_active_dialogue_two)
        self.assertTrue('1' in self.dialogue_manager.loaded_dialogues)
        self.assertTrue('2' in self.dialogue_manager.loaded_dialogues)

        ## Tests that removing one will make it desapear from the loaded dialogues
        self.dialogue_manager.remove(id_active_dialogue_one)
        dialogues = self.dialogue_manager.get_active_dialogues()
        self.assertEqual(len(dialogues), 1)
        self.assertEqual(dialogues[0]['_id'], id_active_dialogue_two)
        self.assertEqual(len(self.dialogue_manager.loaded_dialogues), 1)
        self.assertTrue('2' in self.dialogue_manager.loaded_dialogues)
    
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
        #Save a first dialogue that is no more activated
        dialogue_not_active = self.mkobj_dialogue_announcement()
        dialogue_not_active['dialogue-id'] = '0'
        dialogue_not_active['activated'] = 2
        self.dialogue_manager.save(dialogue_not_active)

        #Save a second dialogue that is activated
        dialogue_active = self.mkobj_dialogue_announcement()
        dialogue_active['dialogue-id'] = '0'
        dialogue_active['activated'] = 1
        dialogue_active_id = self.dialogue_manager.save(dialogue_active)

        current_dialogue = self.dialogue_manager.get_current_dialogue("0")
        self.assertTrue(current_dialogue)
        self.assertEqual(dialogue_active_id, current_dialogue['_id'])
        #Assert the dialogue is now loaded
        self.assertTrue('0' in self.dialogue_manager.loaded_dialogues)
        self.assertTrue(isinstance(self.dialogue_manager.loaded_dialogues['0'], Dialogue));

    def test_get_actions(self):
        dialogue = self.mkobj_dialogue_question_answer()
        self.dialogue_manager.save(dialogue)

        actions = self.dialogue_manager.get_actions(
            dialogue['dialogue-id'],
            dialogue['interactions'][1]['interaction-id'],
            'Olivier')

        self.assertTrue(actions.contains('profiling'))

    def test_get_labels_order_from_dialogues(self):
        dialogue = self.mkobj_dialogue_question_answer()
        self.dialogue_manager.save(dialogue)

        labels = self.dialogue_manager.get_labels_order_from_dialogues(['name', 'status'])

        self.assertEqual(labels, ['status','name'])

