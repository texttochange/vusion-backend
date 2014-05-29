from bson.code import Code
from abc import ABCMeta, abstractmethod

from datetime import datetime

from vusion.persist.model_manager import ModelManager
from vusion.persist import (CreditLog, GarbageCreditLog,
                            ProgramCreditLog, DeletedProgramCreditLog)
from vusion.utils import time_to_vusion_format, time_to_vusion_format_date


class CreditLogManager(ModelManager):

    __metaclass__ = ABCMeta

    def __init__(self, db, collection_name, **kwargs):
        super(CreditLogManager, self).__init__(db, collection_name, **kwargs)
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
    def _increment_credit_log_counter(self):
        pass
    
    @abstractmethod
    def _set_credit_log_counter(self):
        pass

    def _create_no_exist_day_credit_log(self, **kwargs):
        if self._has_today_credit_log(**kwargs):
            return
        credit_log = self._create_today_credit_log(**kwargs)
        self.save_document(credit_log, safe=True)        

    def _set_counters(self, counters, **kwargs):
        self._create_no_exist_day_credit_log(**kwargs)
        self._set_credit_log_counter(counters, **kwargs)

    def _validate_counters(self, counters):
        return dict((k, v) for (k, v) in counters.iteritems() if k in [
            "outgoing", 
            "incoming",
            "outgoing-pending",
            "outgoing-ack",
            "outgoing-nack",
            "outgoing-failed",
            "outgoing-delivered"])

    def set_counters(self, counters, **kwargs):
        #check that counters is valid
        counters = self._validate_counters(counters)
        self._set_counters(counters, **kwargs)

    def _increment_counter(self, credit_type, credit_number, **kwargs):
        self._create_no_exist_day_credit_log(**kwargs)
        self._increment_credit_log_counter(credit_type, credit_number, **kwargs)

    def increment_incoming(self, credit_number, **kwargs):
        self._increment_counter('incoming', credit_number, **kwargs)

    def increment_outgoing(self, credit_number, **kwargs):
        self._increment_counter('outgoing', credit_number, **kwargs)
        self._increment_counter('outgoing-pending', credit_number, **kwargs)

    def increment_acked(self, credit_number, **kwargs):
        self._increment_counter('outgoing-acked', credit_number, **kwargs)
        self._increment_counter('outgoing-pending', -1 * credit_number, **kwargs)
    
    def increment_nacked(self, credit_number, **kwargs):
        self._increment_counter('outgoing-nacked', credit_number, **kwargs)
        self._increment_counter('outgoing-pending', -1 * credit_number, **kwargs)
    
    def increment_delivered(self, credit_number, **kwargs):
        self._increment_counter('outgoing-delivered', credit_number, **kwargs)

    def increment_failed(self, credit_number, **kwargs):
        self._increment_counter('outgoing-failed', credit_number, **kwargs)

    def increment_event_counter(self, prev_status, current_status, credit_number, **kwargs):
        if current_status == 'ack':
            self.increment_acked(credit_number, **kwargs)
        elif current_status == 'nack':
            self.increment_nacked(credit_number, **kwargs)
        elif current_status == 'delivered':
            if prev_status == 'pending':
                self._increment_counter('outgoing-pending', -1 * credit_number, **kwargs)
            self.increment_delivered(credit_number, **kwargs)
        elif current_status == 'failed':
            if prev_status == 'pending':
                self._increment_counter('outgoing-pending', -1 * credit_number, **kwargs)
            self.increment_failed(credit_number, **kwargs)
        else:
            self.log("Credit Log event counter is not supporing %s" % current_status)

    @abstractmethod
    def _get_count_conditions(self):
        pass

    def get_count(self, from_date, to_date=None, counters=['outgoing','incoming'], **kwargs):
        counter_line = 'prev.count'
        for counter in counters:
            counter_line = ("%s + ('%s' in obj? obj['%s']: 0)" % (counter_line, counter, counter))
        reducer = Code(
            "function(obj, prev) { prev.count = %s;}" % counter_line)
        conditions = self._get_count_conditions(from_date, to_date, **kwargs)
        result = self.collection.group(None, conditions, {"count":0}, reducer)
        if len(result) != 0:
            return int(float(result[0]['count']))
        return 0


class ProgramCreditLogManager(CreditLogManager):

    def __init__(self, db, collection_name, program_database, **kwargs):
        super(ProgramCreditLogManager, self).__init__(db, collection_name, **kwargs)
        self.program_database = program_database
        self.collection.ensure_index(
            [('date', 1), ('program-database', 1)],
            background=True,
            spare=True)

    def _has_today_credit_log(self, date=None):
        if date is None:
            date = self.property_helper.get_local_time('datetime')
        return 0 != self.collection.find(
            {'date': time_to_vusion_format_date(date),
             'program-database': self.program_database,
             'code': self.property_helper['shortcode']
             }).count()
    
    def _create_today_credit_log(self, date=None):
        if date is None:
            date = self.property_helper.get_local_time('datetime')
        return ProgramCreditLog(**{
            'date': time_to_vusion_format_date(date),
            'program-database': self.program_database,
            'code': self.property_helper['shortcode'],
            'incoming': 0,
            'outgoing': 0})
    
    def _increment_credit_log_counter(self, credit_type, credit_number):
        self.collection.update(
            {'date': self.property_helper.get_local_time("iso_date"),
             'program-database': self.program_database,
             'code': self.property_helper['shortcode']},
            {'$inc': {credit_type: credit_number}})

    def _set_credit_log_counter(self, counters, date):
        if date is None:
            date = self.property_helper.get_local_time()
        self.collection.update(
            {'date': time_to_vusion_format_date(date),
             'program-database': self.program_database,
             'code': self.property_helper['shortcode']},
            {'$set': counters})

    def _get_count_conditions(self, from_date, to_date):
        if to_date is None:
            to_date = self.property_helper.get_local_time()
        return {
            "date": {
                "$gte": time_to_vusion_format_date(from_date),
                "$lte": time_to_vusion_format_date(to_date)},
            "program-database": self.program_database}

    def deleting_program(self, program_name):
        self.collection.update(
            {'program-database': self.program_database},
            {'$set': {
                'object-type': DeletedProgramCreditLog.MODEL_TYPE,
                'program-name': program_name},
             '$unset': {'program-database': ''}},
            multi=True)


class GarbageCreditLogManager(CreditLogManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(GarbageCreditLogManager, self).__init__(db, collection_name, **kwargs)

    def _has_today_credit_log(self, code, date=None):
        if date is None:
            date = datetime.now()
        return 0 != self.collection.find({
            'object-type': 'garbage-credit-log',
            'date': time_to_vusion_format_date(date),
            'code': code}).count()
    
    def _create_today_credit_log(self, code, date=None):
        if date is None:
            date = datetime.now()        
        return GarbageCreditLog(**{
            'date': time_to_vusion_format_date(date),
            'code': code,
            'incoming': 0,
            'outgoing': 0})
    
    def _increment_credit_log_counter(self, credit_type, credit_number, code):
        self.collection.update(
            {'object-type': 'garbage-credit-log',
             'date': time_to_vusion_format_date(datetime.now()),
             'code': code},
            {'$inc': {credit_type: credit_number}})

    def _set_credit_log_counter(self, counters, date, code):
        self.collection.update(
            {'object-type': 'garbage-credit-log',
             'date': time_to_vusion_format_date(date),
             'code': code},
            {'$set': counters})

    def _get_count_conditions(self, from_date, to_date, code):
        if to_date is None:
            to_date = datetime.now()
        return {
            'date': {
                '$gte': time_to_vusion_format_date(from_date),
                '$lte': time_to_vusion_format_date(to_date)
                },
            'code': code,
            'object-type': 'garbage-credit-log'}