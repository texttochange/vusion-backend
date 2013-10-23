

class DialogueManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(DialogueManager, self).__init__(db, collection_name, **kwargs)

    def get_active_dialogues(self):
        pass

    def get_matching_dialogue_actions(self):
        pass
    
    def get_dialogue_interaction(self):
        pass
    
    def test_get_keywords(self):
        pass
