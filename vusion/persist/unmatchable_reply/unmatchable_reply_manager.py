from datetime import timedelta
from bson import Code
from vusion.utils import (time_to_vusion_format, date_from_vusion_format,
                          time_to_vusion_format_date, get_shortcode_value,
                          get_shortcode_international_prefix)
from vusion.persist import ModelManager, UnmatchableReply
from vusion.persist.cursor_instanciator import CursorInstanciator

class UnmatchableReplyManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(UnmatchableReplyManager, self).__init__(db, collection_name, **kwargs)
        self.collection.ensure_index('timestamp')

    def get_older_date(self, date=None, code=None):
        if date is None:
            date = self.get_local_time() + timedelta(days=1)
        date = date.replace(hour=0, minute=0, second=0)
        conditions = {'timestamp': {'$lt': time_to_vusion_format(date)}}
        if code is not None:
            conditions['$or'] = [
                {'to': code},
                {'participant-phone': code},
                {'to': get_shortcode_value(code), 
                 'participant-phone': {'$regex': "^\+%s" % get_shortcode_international_prefix(code)}}
            ]
        cursor = self.find(conditions).sort('timestamp', -1).limit(1)
        if cursor.count() == 0:
            return None
        try:
            um = UnmatchableReply(**cursor.next())
            return date_from_vusion_format(um['timestamp'])
        except Exception as e:
            self.log_helper.log(e.message)
            return None

    def count_day_credits(self, date, code):
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
            'timestamp': {
                '$gte': time_to_vusion_format_date(date),
                '$lt': time_to_vusion_format_date(date + timedelta(days=1))},
            '$or': [
                {'to': code}, {"participant-phone": code}, 
                {'to': get_shortcode_value(code), 
                 'participant-phone': {'$regex': "^\+%s" % get_shortcode_international_prefix(code)}},
            ]}
        counters =  {"incoming": 0,
                     "outgoing": 0}
        result = self.group(None, conditions, counters, reducer)
        if len(result) == 0:
            return counters
        counters = result[0]
        return {k : int(float(counters[k])) for k in counters.iterkeys()}

    def get_unmatchable_replys(self, query=None):
        def log(exception, item=None):
            self.log("Exception %r while instanciating an unmatchable reply %r" % (exception, item))
        return CursorInstanciator(self.collection.find(query), UnmatchableReply, [log])
