from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper
from vusion.persist import DialogueManager

class TestDialogueManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        c.safe = True
        db = c.test_program_db
        self.dialogue_manager = DialogueManager
        
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
        pass
    
    def test_get_matching_dialogue_actions(self):
        pass

    def test_get_dialogue_interaction(self):
        pass

    def test_get_keywords(self):
        pass