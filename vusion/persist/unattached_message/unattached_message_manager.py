from bson.objectid import ObjectId

from vusion.persist import ModelManager, UnattachedMessage
from vusion.persist.cursor_instanciator import CursorInstanciator

class UnattachedMessageManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(UnattachedMessageManager, self).__init__(db, collection_name, **kwargs)

    def get_unattached_message(self, unattach_id):
        try:
            return UnattachedMessage(**self.collection.find_one({
                '_id': ObjectId(unattach_id)}))
        except TypeError:
            self.log("Error unattach message %s cannot be found" % unattach_id)
            return None

    #only return the one in the future
    def get_unattached_messages(self, query={}):
        future_query = {'fixed-time': {'$gt': self.get_local_time('vusion')}}
        query = dict(query.items()+ future_query.items())

        def log(exception, item=None):
            self.log("Exception %r while instanciating an unattached message %r" % (exception, item))

        return CursorInstanciator(self.collection.find(query), UnattachedMessage, [log])

    def get_unattached_messages_selector_tag(self, tag):
        query = {'send-to-match-conditions': tag}
        return self.get_unattached_messages(query)
