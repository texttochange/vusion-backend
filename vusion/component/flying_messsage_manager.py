import re
from bson import ObjectId

class FlyingMessageManager(object):
    
    FLYING_MESSAGE_KEY = 'fm'
    KEY_EXPIRING_TIME = 18000   #5h in seconds

    def __init__(self, prefix_key, redis):
        self.prefix_key = prefix_key
        self.redis = redis

    def data_key(self, message_user_id):
        return ':'.join([self.prefix_key, self.FLYING_MESSAGE_KEY, message_user_id])

    def append_message_data(self, message_user_id, history_id, credits, status):
        key = self.data_key(message_user_id)
        data = ':'.join([str(history_id), str(credits), status])
        self.redis.setex(key, data, self.KEY_EXPIRING_TIME)

    def get_message_data(self, message_user_id):
        key = self.data_key(message_user_id)
        data = self.redis.get(key)
        if data is None:
            return None, 0
        [history_id, credits, status] = re.split(':', data)
        return ObjectId(history_id), int(credits), status