from datetime import timedelta

from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from vumi import log


class RedisLogger(object):
    
    LOGS_KEY = 'logs'

    def __init__(self, program_key, prefix_key, redis, gc_interval=120,
                 keep_log=120, property_helper=None):
        self.program_key = program_key
        self.prefix_key = prefix_key
        self.redis = redis
        self.property_helper = None
        self.gc_interval = gc_interval
        self.keep_log = keep_log
        if property_helper is not None:
            self.startup(property_helper)

    def stop(self):
        if self.gc.running:
            self.gc.stop()

    def startup(self, property_helper):
        self.property_helper = property_helper
        self.clock = self.get_clock()
        self.gc = LoopingCall(self.gc_logs)
        self.gc.clock = self.clock
        self.gc.start(self.gc_interval)

    def get_clock(self):
        return reactor

    def logs_key(self):
        return ':'.join([self.prefix_key, self.LOGS_KEY])

    def gc_logs(self):
        log.msg('remove old log')
        self.redis.zremrangebyscore(
            self.logs_key(),
            1,
            self.property_helper.get_local_time(
                date_format='timestamp', 
                time_delta=timedelta(minutes=-1 * self.keep_log)))

    def log(self, msg, level='msg'):
        #log in redis
        try:
            to_log = msg
            if level != 'msg':
                to_log = "-%s- %s" % (level, msg)
            to_log = "[%s] %s" % (self.property_helper.get_local_time('vusion'), to_log)
            self.redis.zadd(
                self.logs_key(), to_log, self.property_helper.get_local_time('timestamp'))
        except:
            pass
            #log.error('[%s] %s' % (self.program_key, "Couldn't log in redis"))
        
        #log in file
        if (level == 'msg'):
            log.msg('[%s] %s' % (self.program_key, msg))
        else:
            log.error('[%s] %s' % (self.program_key, msg))

    def clear_logs(self):
        self.redis.delete(self.logs_key())


class BasicLogger(object):
    
    def log(self, msg, level='msg'):
        if level == 'err':
            log.err(msg)
        elif level == 'debug':
            log.debug(msg)
        else:
            log.msg(msg)


class PrintLogger(object):
    
    def log(self, msg, level='msg'):
        if level == 'err':
            print("ERROR: %s" % msg)
        elif level == 'debug':
            print("DEBUG: %s" % msg)
        else:
            print("%s" % msg)
