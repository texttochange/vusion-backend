# -*- test-case-name: vusion.component.tests.test_sms_limit_manager -*-
import re
from datetime import timedelta

from bson.code import Code

from twisted.internet.task import LoopingCall

from vumi import log

from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.persist import VusionModel
from vusion.error import WrongModelInstanciation


class CreditManager(object):
    
    CREDITMANAGER_KEY = 'creditmanager'
    COUNT_KEY = 'count'
    STATUS_KEY = 'status'
    CARD_KEY = 'card'
    NOTIFICATION_KEY = 'notifications'
    
    limit_type = 'none'
    limit_number = None
    limit_from_date = None
    limit_to_date = None
    
    last_status = None
    
    def __init__(self, prefix_key, redis, history, schedule,
                 property_helper):
        self.prefix_key = prefix_key
        self.redis = redis
        self.history_collection = history
        self.schedule_collection = schedule
        self.last_status = self.redis.get(self.status_key())
        self.property_helper = property_helper        
        self.set_limit()

    ## in case the program settings are changing
    def set_limit(self):
        property_helper = self.property_helper
        need_resync = False
        new_limit_type = property_helper['sms-limit-type']
        new_limit_number = int(property_helper['sms-limit-number']) if property_helper['sms-limit-number'] is not None else None
        new_limit_from_date = property_helper['sms-limit-from-date']
        if property_helper['sms-limit-to-date'] is not None:
            ## the timeframe include the last day
            new_limit_to_date = time_to_vusion_format(time_from_vusion_format(property_helper['sms-limit-to-date']) + timedelta(days=1))
        else: 
            new_limit_to_date = None
        if (self.limit_type != new_limit_type 
            or self.limit_number != new_limit_number
            or self.limit_from_date != new_limit_from_date
            or self.limit_to_date != new_limit_to_date):
            need_resync = True
        self.limit_type = new_limit_type
        self.limit_number = new_limit_number
        self.limit_from_date = new_limit_from_date
        self.limit_to_date = new_limit_to_date
        if need_resync:
            self.reinitialize_counter()
            if self.property_helper.is_ready():
                self.check_status()
        
    def credit_manager_key(self):
        return ':'.join([self.prefix_key, self.CREDITMANAGER_KEY])

    def used_credit_counter_key(self):
        return ':'.join([self.credit_manager_key(), self.COUNT_KEY])

    def status_key(self):
        return ':'.join([self.credit_manager_key(), self.STATUS_KEY])

    def card_key(self, source_type, source_id):
        return ':'.join([self.credit_manager_key(), self.CARD_KEY, unicode(source_type), unicode(source_id)])

    def notification_key(self):
        return ':'.join([self.credit_manager_key(), self.NOTIFICATION_KEY])

    ## To keep some counting correct even in between sync
    def received_message(self, message_credits):
        if self.limit_type == 'outgoing-incoming':
            self.redis.incr(self.used_credit_counter_key(), message_credits)

    ## Should go quite fast
    def is_allowed(self, message_credits, schedule=None):
        if not self.has_limit():
            return True
        if not self.is_timeframed():
            self.set_blackcard(schedule)
            return False
        if self.can_be_sent(message_credits, schedule):
            self.increase_used_credit_counter(message_credits)
            self.set_whitecard(schedule)
            return True
        self.check_status()
        self.set_blackcard(schedule)
        return False

    def has_limit(self):
        if self.limit_type == 'none':
            return False
        return True

    def increase_used_credit_counter(self, message_credits):
        self.redis.incr(self.used_credit_counter_key(), message_credits)        

    def is_timeframed(self):
        local_time = self.property_helper.get_local_time('vusion')
        log.msg("[credit manager] is timeframed %s < %s < %s" % (self.limit_from_date, local_time, self.limit_to_date))
        return (self.limit_from_date <= local_time
                and local_time <= self.limit_to_date)

    def check_status(self):
        local_time = self.property_helper.get_local_time('vusion')
        if not self.has_limit():
            status = CreditStatus(**{
                'status': 'none',
                'since': local_time})
        elif not self.is_timeframed():
            status = CreditStatus(**{
                'status': 'no-credit-timeframe',
                'since': local_time,
            })
        elif not self.can_be_sent(1):
            status = CreditStatus(**{
                'status': 'no-credit',
                'since': local_time,
            })
        else:
            status = CreditStatus(**{
                'status': 'ok',
                'since': local_time,
            })
        log.msg('[credit manager] old status %r' % self.last_status)
        if self.last_status is not None and self.last_status == status:
            log.msg('[credit manager] not updating the status')
            return self.last_status
        self.last_status = status
        log.msg('[credit manager] save new status %r' % status)
        self.redis.set(self.status_key(), self.last_status.get_as_json())
        return self.last_status

    def can_be_sent(self, message_credits, schedule=None):
        used_credit_counter = self.get_used_credit_counter()
        if schedule is not None and schedule.get_type() == 'unattach-schedule':
            if self.has_whitecard(schedule):
                return True
            else:
                estimation = self.estimate_unattached_required_credit(message_credits, schedule)
                return used_credit_counter + estimation <= self.limit_number
        return int(used_credit_counter) + message_credits <= self.limit_number
        
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
        whitecard_key = self.card_key('unattach-schedule', schedule['unattach-id'])
        self.redis.setex(whitecard_key, card, 1800000) ## 30 minutes
        return True
    
    def push_notification(self, notification, local_time):
        notification_key = self.notification_key()
        self.redis.zadd(
            notification_key,
            notification.get_as_json(),
            get_local_time_as_timestamp(local_time))

    def delete_old_notification(self):
        remove_older_than = self.property_helper.get_local_time() - timedelta(days=5)
        notification_key = self.notification_key()
        self.redis.zremrangebyscore(
            notification_key,
            1,
            get_local_time_as_timestamp(remove_older_than))


    def set_whitecard(self, schedule):
        self.cache_card(schedule, 'white')

    def set_blackcard(self, schedule):
        self.cache_card(schedule, 'black')

    ## On it implemented the "all or none of participant" for unattach-schedule
    ## the "all or none of sequence for dialogue and request" is not yet prioritized
    def has_whitecard(self, schedule):
        if schedule.get_type() != 'unattach-schedule':
            return False
        card_key = self.card_key('unattach-schedule', schedule['unattach-id'])
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

    def reinitialize_counter(self):
        self.redis.delete(self.used_credit_counter_key())
        self.get_used_credit_counter()

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


## CreditNotification aims highligth a decision from the creditManage on to the frontend
class CreditNotification(VusionModel):
    
    MODEL_TYPE='credit-notification'
    MODEL_VERSION='1'
    
    fields = {
        'timestamp': {
            'required': True,
            'valid_value': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)T0?(\d+):0?(\d+)(:0?(\d+))$'), v['timestamp'])
            },
        'notification-type': {
            'required': True,
            'valid_value': lambda v: v['notification-type'] in [
                'no-credit', 
                'no-credit-timeframe'],
            'required_subfield': lambda v: getattr(v, 'required_subfields')(
                v['notification-type'],
                {'no-credit':['source-type'],
                 'no-credit-timeframe': []}),
            },
        'source-type': {
            'required': False,
            'valid_value': lambda v: v['source-type'] in [
                'unattach-schedule',
                'dialogue-schedule',
                'request-schedule'],
            'required_subfield': lambda v: getattr(v, 'required_subfields')(
                            v['source-type'],
                            {'unattach-schedule':['unattach-id'],
                             'dialogue-schedule': ['dialogue-id', 'interaction-id'],
                             'request-schedule': ['request-id']}), 
            },
        'unattach-id': {
            'required': False,
            'valid_type': lambda v: isinstance(v['unattach-id'], str),
            'valid_value': lambda v: v['unattach-id'] is not None,
            },
        'dialogue-id': {
            'required': False,
            'valid_type': lambda v: isinstance(v['dialogue-id'], str),
            'valid_value': lambda v: v['unattach-id'] is not None,            
            },
        'interaction-id': {
            'required': False,
            'valid_type': lambda v: isinstance(v['interaction-id'], str),
            'valid_value': lambda v: v['unattach-id'] is not None,            
            },
        'request-id': {
            'required': False,
            'valid_type': lambda v: isinstance(v['request-id'], str),
            'valid_value': lambda v: v['unattach-id'] is not None,            
            },
        }

    def __init__(self, **kwargs):
        if 'object-type' in kwargs:
            if kwargs['object-type'] != self.MODEL_TYPE:
                message = 'Object-type %s cannot be instanciate as %s' % (kwargs['object-type'], self.MODEL_TYPE)
                raise WrongModelInstanciation(message)
        else:
            kwargs.update({
                'object-type': self.MODEL_TYPE})
        super(CreditNotification, self).__init__(**kwargs)    

    def validate_fields(self):
        self._validate(self, self.fields)


class CreditStatus(VusionModel):

    MODEL_TYPE='credit-status'
    MODEL_VERSION='1'

    fields = {
        'since': {
            'required': True,
            'valid_value': lambda v: re.match(re.compile('^(\d{4})-0?(\d+)-0?(\d+)T0?(\d+):0?(\d+)(:0?(\d+))$'), v['since'])
            },
        'status': {
            'required': True,
            'valid_value': lambda v: v['status'] in [
                'none', 
                'ok', 
                'no-credit', 
                'no-credit-timeframe'],
            },
    }
    
    def __init__(self, **kwargs):
        if 'object-type' in kwargs:
            if kwargs['object-type'] != self.MODEL_TYPE:
                message = 'Object-type %s cannot be instanciate as %s' % (kwargs['object-type'], self.MODEL_TYPE)
                raise WrongModelInstanciation(message)
        else:
            kwargs.update({
                'object-type': self.MODEL_TYPE})
        super(CreditStatus, self).__init__(**kwargs)    

    def __eq__(self, other):
            if isinstance(other, CreditStatus):
                return self['status'] == other['status']
            return False 

    def validate_fields(self):
        self._validate(self, self.fields)    