from datetime import timedelta
from bson import Code
from vusion.utils import (time_to_vusion_format, date_from_vusion_format,
                          time_to_vusion_format_date)
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

    def count_day_credits(self, date):
        reducer = Code("function(obj, prev) {"
                       "    credits = 1;"
                       "    switch (obj['direction']) {"
                       "    case 'incoming':"
                       "        prev['incoming'] = prev['incoming'] + credits;"
                       "        break;"
                       "    case 'outgoing':"
                       "        prev['outgoing'] = prev['outgoing'] + credits;"
                       "        break;"
                       "     }"
                       " }")
        conditions = {
            "timestamp": {
                "$gte": time_to_vusion_format_date(date),
                "$lt": time_to_vusion_format_date(date + timedelta(days=1))},
            }
        counters =  {"incoming": 0,
                     "outgoing": 0}
        result = self.group(None, conditions, counters, reducer)
        if len(result) == 0:
            return counters
        counters = result[0]
        return {k : int(float(counters[k])) for k in counters.iterkeys()}