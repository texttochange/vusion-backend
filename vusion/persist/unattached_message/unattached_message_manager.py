
from vusion.persist import ModelManager, UnattachedMessage

class UnattachedMessageManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(UnattachedMessageManager, self).__init__(db, collection_name, **kwargs)