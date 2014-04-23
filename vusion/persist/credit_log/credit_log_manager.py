from bson.code import Code
from abc import ABCMeta, abstractmethod

from datetime import datetime

from vusion.persist.model_manager import ModelManager
from vusion.persist import CreditLog
from vusion.utils import time_to_vusion_format, time_to_vusion_format_date


class CreditLogManager(ModelManager):
    
    __metaclass__ = ABCMeta
    
    def __init__(self, db, collection_name, logger, **kwargs):
        super(CreditLogManager, self).__init__(db, collection_name, **kwargs)
        self.logger = logger
        #self.program_database = program_database
        ##As default filter index
        self.collection.ensure_index(
            'date',
            background=True)

    @abstractmethod
    def _has_today_credit_log(self):
        pass
    
    @abstractmethod
    def _create_today_credit_log(self):
        pass
    
    @abstractmethod
    def _update_credit_log_counter(self):
        pass

    def _increment_counter(self, credit_type, credit_number, **kwargs):
        if self._has_today_credit_log(**kwargs):
            credit_log = self._create_today_credit_log(**kwargs)
            self.save_document(credit_log, safe=True)
        self._update_credit_log_counter(credit_type, credit_number, **kwargs)

    def increment_incoming(self, credit_number, **kwargs):
        self._increment_counter('incoming', credit_number, **kwargs)

    def increment_outgoing(self, credit_number, **kwargs):
        self._increment_counter('outgoing', credit_number, **kwargs)

    def increment_acked(self, credit_number, **kwargs):
        self._increment_counter('outgoing-acked', credit_number, **kwargs)
    
    def increment_delivered(self, credit_number, **kwargs):
        self._increment_counter('outgoing-delivered', credit_number, **kwargs)

    def increment_failed(self, credit_number, **kwargs):
        self._increment_counter('outgoing-failed', credit_number, **kwargs)

    @abstractmethod
    def _get_count_conditions(self):
        pass

    def get_count(self, from_date, to_date=None, count_type='outgoing-incoming', **kwargs):
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
        conditions = self._get_count_conditions(from_date, to_date, **kwargs)
        result = self.collection.group(None, conditions, {"count":0}, reducer)
        if len(result) != 0:
            return int(float(result[0]['count']))
        return 0


class ProgramCreditLogManager(CreditLogManager):
    
    def __init__(self, db, collection_name, program_database, **kwargs):
        super(ProgramCreditLogManager, self).__init__(db, collection_name, 'program', **kwargs)
        self.program_database = program_database
        self.collection.ensure_index(
            [('date', 1), ('program-database', 1)],
            background=True,
            spare=True)

    def _has_today_credit_log(self):
        return 0 == self.collection.find(
            {'date': self.property_helper.get_local_time("iso_date"),
             'program-database': self.program_database,
             'code': self.property_helper['shortcode']
             }).count()
    
    def _create_today_credit_log(self):
        return CreditLog(**{
            'date': self.property_helper.get_local_time("iso_date"),
            'logger': 'program',
            'program-database': self.program_database,
            'code': self.property_helper['shortcode'],
            'incoming': 0,
            'outgoing': 0})
    
    def _update_credit_log_counter(self, credit_type, credit_number):
        self.collection.update(
            {'date': self.property_helper.get_local_time("iso_date"),
             'program-database': self.program_database,
             'code': self.property_helper['shortcode']},
            {'$inc': {credit_type: credit_number}})

    def _get_count_conditions(self, from_date, to_date):
        if to_date is None:
            to_date = self.property_helper.get_local_time()
        return {
            "date": {
                "$gte": time_to_vusion_format_date(from_date),
                "$lte": time_to_vusion_format_date(to_date)},
            "program-database": self.program_database}


class GarbageCreditLogManager(CreditLogManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(GarbageCreditLogManager, self).__init__(db, collection_name, 'garbage', **kwargs)

    def _has_today_credit_log(self, code):
        return 0 == self.collection.find({
            'date': time_to_vusion_format_date(datetime.now()),
            'logger': 'garbage',
            'code': code}).count()
    
    def _create_today_credit_log(self, code):
        return CreditLog(**{
            'date': time_to_vusion_format_date(datetime.now()),
            'logger': 'garbage',
            'code': code,
            'incoming': 0,
            'outgoing': 0})
    
    def _update_credit_log_counter(self, credit_type, credit_number, code):
        self.collection.update(
            {'date': time_to_vusion_format_date(datetime.now()),
             'logger': 'garbage',
             'code': code},
            {'$inc': {credit_type: credit_number}})

    def _get_count_conditions(self, from_date, to_date, code):
        if to_date is None:
            to_date = datetime.now()
        return {
            'date': {
                '$gte': time_to_vusion_format_date(from_date),
                '$lte': time_to_vusion_format_date(to_date)
                },
            'code': code,
            'logger': 'garbage'
        }