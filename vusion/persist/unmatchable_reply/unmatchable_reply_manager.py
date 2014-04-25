from datetime import timedelta
from vusion.utils import time_to_vusion_format, date_from_vusion_format
from vusion.persist import ModelManager, UnmatchableReply


class UnmatchableReplyManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(UnmatchableReplyManager, self).__init__(db, collection_name)
        self.collection.ensure_index('timestamp')

    def get_older_date(self, date=None):
        if date is None:
            date = self.get_local_time() + timedelta(days=1)
        date = date.replace(hour=0, minute=0, second=0)
        cursor = self.find(
            {'timestamp': {'$lte': time_to_vusion_format(date)}}).sort('timestamp', -1).limit(1)
        if cursor.count() == 0:
            return None
        um = UnmatchableReply(**cursor.next())
        return date_from_vusion_format(um['timestamp'])
