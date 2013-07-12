# -*- test-case-name: vusion.component.tests.test_sms_limit_manager -*-
from bson.code import Code

from twisted.internet.task import LoopingCall


class SmsLimitManager(object):
    
    SMSLIMIT_KEY = 'smslimit'
    COUNT_KEY = 'count'
    
    def __init__(self, prefix_key, redis, history, schedule,
                limit_type='none',
                limit_number=None, from_date=None, to_data=None):
        self.prefix_key = prefix_key
        self.redis = redis
        self.history_collection = history
        self.schedule_collection = schedule
        self.limit_type = limit_type
        self.limit_number = limit_number
        self.limit_from_date = from_date
        self.limit_to_date = to_data
        self.delete_redis_counter()

    def __del__(self):
        self.delete_redis_counter()

    ## in case the program settings are changing
    def set_limit(self, limit_type, limit_number=None, from_date=None, to_date=None):
        need_resync = False
        if (self.limit_type != limit_type 
            or self.limit_from_date != from_date
            or self.limit_to_date != to_date):
            need_resync = True
        self.limit_type = limit_type
        self.limit_number = limit_number
        self.limit_from_date = from_date
        self.limit_to_date = to_date
        if need_resync:
            self.delete_redis_counter()

    def sms_limit_key(self):
        return ':'.join([self.prefix_key, self.SMSLIMIT_KEY])

    def used_credit_counter_key(self):
        return ':'.join([self.sms_limit_key(), self.COUNT_KEY])

    def whitecard_key(self, source_type, source_id):
        return ':'.join([self.sms_limit_key(), unicode(source_type), unicode(source_id)])

    ## To keep some counting correct even in between sync
    def received_message(self, message_credits):
        if self.limit_type == 'outgoing-incoming':
            self.redis.incr(self.used_credit_counter_key(), message_credits)

    ## Should go quite fast
    def is_allowed(self, message_credits, schedule=None):
        if self.limit_type == 'none':
            return True
        used_credit_counter = self.get_used_credit_counter()
        if self.can_be_sent(used_credit_counter, message_credits, schedule):
            self.redis.incr(self.used_credit_counter_key(), message_credits)
            self.set_whitecard(schedule)
            return True
        self.set_blackcard(schedule)
        return False
        
    def can_be_sent(self, used_credit_count, message_credits, schedule=None):
        if schedule is not None and schedule.get_type() == 'unattach-schedule':
            if self.has_whitecard(schedule):
                return True
            else:
                estimation = self.estimate_unattached_required_credit(message_credits, schedule)
                return used_credit_count + estimation <= self.limit_number
        return int(used_credit_count) + message_credits <= self.limit_number
        
    ## This is just a rought estimation based on the message send to the first participant
    def estimate_unattached_required_credit(self, message_credits, schedule):
        conditions = {
            'object-type': 'unattach-schedule', 
            'unattach-id': schedule['unattach-id'],
            'date-time': schedule['date-time']}
        scheduled_count = self.schedule_collection.find(conditions).count()
        return message_credits * scheduled_count + message_credits ##the first one have already been deleted

    ## Cache a card for a given schedule for 30min
    def cache_card(self, schedule, card):
        if schedule is None or schedule.get_type() != 'unattach-schedule':
            return False
        whitecard_key = self.whitecard_key('unattach-schedule', schedule['unattach-id'])
        self.redis.setex(whitecard_key, card, 1800000) ## 30 minutes
        return True

    def set_whitecard(self, schedule):
        self.cache_card(schedule, 'white')

    def set_blackcard(self, schedule):
        self.cache_card(schedule, 'black')

    ## On it implemented the "all or none of participant" for unattach-schedule
    ## the "all or none of sequence for dialogue and request" is not yet prioritized
    def has_whitecard(self, schedule):
        if schedule.get_type() != 'unattach-schedule':
            return False
        card_key = self.whitecard_key('unattach-schedule', schedule['unattach-id'])
        is_whitecarded = self.redis.get(card_key)
        if is_whitecarded is not None and is_whitecarded == 'white':
            return True
        return False

    def get_used_credit_counter(self):
        used_credit_counter = self.redis.get(self.used_credit_counter_key())
        if used_credit_counter is not None:
            return int(used_credit_counter)
        used_credit_counter = self.get_used_credit_counter_mongo()
        self.redis.set(self.used_credit_counter_key(), used_credit_counter)
        return used_credit_counter

    def delete_redis_counter(self):
        self.redis.delete(self.used_credit_counter_key())

    def get_used_credit_counter_mongo(self):
        reducer = Code("function(obj, prev) {"
                       "    if ('message-credits' in obj) {"
                       "        prev.count = prev.count + obj['message-credits'];"
                       "    } else {"
                       "        prev.count = prev.count + 1;"
                       "    }"
                       " }")
        condition = {"timestamp": {"$gt": self.limit_from_date},
                     "timestamp": {"$lt": self.limit_to_date},
                     "object-type": {"$in": ["dialogue-history", "unattach-history", "request-history"]}}
        if self.limit_type == 'outgoing-only':
            condition.update({'message-direction': 'outgoing'})
        result = self.history_collection.group(None, condition, {"count":0}, reducer)
        if len(result) != 0:
            return int(float(result[0]['count']))
        return 0
