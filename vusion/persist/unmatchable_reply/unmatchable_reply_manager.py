from vusion.persist import ModelManager


class UnmatchableReplyManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(UnmatchableReplyManager, self).__init__(db, collection_name)
        self.collection.ensure_index('timestamp')