

class DialogueManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(DialogueManager, self).__init__(db, collection_name, **kwargs)