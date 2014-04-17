from bson.code import Code

from vusion.persist.model_manager import ModelManager
from vusion.persist import CreditLog
from vusion.utils import time_to_vusion_format, time_to_vusion_format_date


class CreditLogManager(ModelManager):
    
    def __init__(self, db, collection_name, program_database,**kwargs):
        super(CreditLogManager, self).__init__(db, collection_name, **kwargs)
        self.program_database = program_database
        ##As default filter index
        self.collection.ensure_index(
            'date',
            background=True)
        ##For max speed on increment operations
        self.collection.ensure_index(
            [('date', 1), ('program-database', 1)],
            background=True)

    def _has_today_credit_log(self, date, program_database, code):
        return self.collection.find({'date': date, 
                                     'program-database': program_database,
                                     'code': code}).count()

    def increment_incoming(self, credit_number):
        self._increment_counter('incoming', credit_number)

    def increment_outgoing(self, credit_number):
        self._increment_counter('outgoing', credit_number)

    def increment_acked(self, credit_number):
        self._increment_counter('outgoing-acked', credit_number)
    
    def increment_delivered(self, credit_number):
        self._increment_counter('outgoing-delivered', credit_number)

    def increment_failed(self, credit_number):
        self._increment_counter('outgoing-failed', credit_number)

    def _increment_counter(self, credit_type, credit_number):
        #get date, code from the programSettingHelper
        today = self.property_helper.get_local_time("iso_date")
        code = self.property_helper['shortcode']
        if self._has_today_credit_log(today, self.program_database, code) == 0:
            credit_log = CreditLog(**{
                'date': today,
                'program-database': self.program_database,
                'code': code,
                'incoming': 0,
                'outgoing': 0})
            self.save_document(credit_log, safe=True)
        self.collection.update(
            {'date': today,
             'program-database': self.program_database,
             'code': code},
            {'$inc': {credit_type: credit_number}})

    def get_count(self, from_date, count_type='outgoing-incoming', to_date=None):
        if count_type == 'outgoing-incoming':
            reducer = Code(
                "function(obj, prev) {"
                "    prev.count = prev.count + obj['incoming'] + obj['outgoing'];"
                " }")
        else:
            reducer = Code(
                "function(obj, prev) {"
                "    prev.count = prev.count + obj['outgoing'];"
                " }")
        if to_date is None:
            to_date = self.property_helper.get_local_time()
        condition = {
            "date": {
                "$gte": time_to_vusion_format_date(from_date),
                "$lte": time_to_vusion_format_date(to_date)},
            "program-database": self.program_database}
        result = self.collection.group(None, condition, {"count":0}, reducer)
        if len(result) != 0:
            return int(float(result[0]['count']))
        return 0
