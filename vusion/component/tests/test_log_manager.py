import re
from datetime import datetime, timedelta

from redis import Redis

from twisted.trial.unittest import TestCase

from vusion.utils import get_local_time_as_timestamp
from vusion.component import DialogueWorkerPropertyHelper, LogManager

class LogManagerTestCase(TestCase):

    def setUp(self):
        # setUp redis
        self.redis = Redis()
        self.prefix_redis_key = 'unittest:testprogram'
        self.program_key =  'testprogram'
        self.clearData()

        self.lm = LogManager(self.program_key, self.prefix_redis_key, self.redis)

        #parameters:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)

        self.lm.startup(self.property_helper)

    def tearDown(self):
        self.lm.stop()
        self.clearData()

    def clearData(self):
        keys = self.redis.keys("%s:*" % self.prefix_redis_key)
        for key in keys:
            self.redis.delete(key)

    def test_log(self):
        self.lm.log("this is my first log")
        logs = self.redis.zrange('unittest:testprogram:logs', -5, -1, True)
        self.assertEqual(1, len(logs))
        
        self.lm.log("this is my second log")
        logs = self.redis.zrange('unittest:testprogram:logs', -5, -1, True)
        self.assertEqual(2, len(logs))
        self.assertRegexpMatches(
            logs[0],
            re.compile('^\[.*\] this is my second log$'))
        self.assertRegexpMatches(
            logs[1],
            re.compile('^\[.*\] this is my first log$'))        

    def test_clear_logs(self):
        self.lm.log("this is my first log")
        logs = self.redis.zrange('unittest:testprogram:logs', -5, -1, True)
        self.assertEqual(1, len(logs))
        
        self.lm.clear_logs()
        
        logs = self.redis.zrange('unittest:testprogram:logs', -5, -1, True)
        self.assertEqual(0, len(logs))
    
    def test_log_gc(self):
        still_valid_time = self.property_helper.get_local_time() - timedelta(minutes=119)
        self.redis.zadd(
            'unittest:testprogram:logs', 
            'still valid log message',
            get_local_time_as_timestamp(still_valid_time))
        
        not_valid_time = self.property_helper.get_local_time() - timedelta(minutes=121)
        self.redis.zadd(
            'unittest:testprogram:logs', 
            'no more valid log message', 
            get_local_time_as_timestamp(not_valid_time))
         
        self.lm.gc_logs()
         
        logs = self.redis.zrange('unittest:testprogram:logs', -5, -1, True)
        self.assertEqual(1, len(logs))
        self.assertEqual(
            'still valid log message',
            logs[0])
