# -*- test-case-name: vusion.component.tests.test_sms_limit_manager -*-
from bson.code import Code

from twisted.internet.task import LoopingCall


class SmsLimitManager(object):
    
    SMSLIMIT_KEY = 'smslimit'
    COUNT_KEY = 'count'
    
    def __init__(self, prefix_key, redis, history,
                limit_type, limit_number, from_date, to_data):
        self.prefix_key = prefix_key
        self.history = history
        self.redis = redis
        self.limit_type = limit_type
        self.limit_number = limit_number
        self.limit_from_date = from_date
        self.limit_to_date = to_data
        self.sync_data()

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
            self.sync_data()

    def sms_limit_key(self):
        return ':'.join([self.prefix_key, self.SMSLIMIT_KEY])

    def count_key(self):
        return ':'.join([self.sms_limit_key(), self.COUNT_KEY])

    def wild_key(self, source_type, source_id):
        return ':'.join([self.sms_limit_key(), unicode(source_type), unicode(source_id)])

    ## To keep some counting correct even in between sync
    def received_message(self, message_credits):
        if self.limit_type == 'outgoing-incoming':
            self.redis.incr(self.count_key(), message_credits)

    ## Should go quite fast
    def is_allowed(self, message_credits, source_type=None, source_id=None, recipient_count=1):
        if self.limit_type == 'none':
            return True
        count = self.redis.get(self.count_key())
        if count is None:
            count = self.sync_data()
        expected = int(float(count)) + message_credits * recipient_count
        if  expected <= self.limit_number: 
            self.redis.incr(self.count_key(), message_credits * recipient_count)
            return True
        if source_type is not None and source_id is not None:
            if self.redis.exists(self.wild_key(source_type, source_id)):
                return True
        return False

    ## Should go with wildcard the key should be store with expiring 1h later
    def is_allowed_set_wildcard(self, message_credits, recipient_count, source_type, source_id):
        is_allowed = self.is_allowed(message_credits, source_type, source_id, recipient_count)
        if is_allowed:
            wild_key = self.wild_key(source_type, source_id)
            self.redis.set(wild_key, 1)
            self.redis.expire(wild_key, 3600000) # remove after 1h
        return is_allowed

    def sync_data(self):
        if self.limit_type == 'none':
            self.redis.delete(self.count_key())
            return None
        reducer = Code("function(obj, prev) {"
                       "    prev.count = prev.count + obj['message-credits'];"
                       " }")
        condition = {"timestamp": {"$gt": self.limit_from_date},
                     "timestamp": {"$lt": self.limit_to_date},
                     "object-type": {"$in": ["dialogue-history", "unattach-history", "request-history"]}}
        if self.limit_type == 'outgoing-only':
            condition.update({'message-direction': 'outgoing'})
        result = self.history.group(None, condition, {"count":0}, reducer)
        if len(result) != 0:
            count = int(float(result[0]['count']))
        else:
            count = 0
        self.redis.set(self.count_key(), count)
        return count
