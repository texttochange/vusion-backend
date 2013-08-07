from datetime import timedelta

from bson import ObjectId

from history import history_generator
from vusion.utils import time_to_vusion_format


class HistoryManager(object):

    def __init__(self, db, collection_name):
        self.properties = None
        if collection_name in db.collection_names():
            self.collection = db[collection_name]
        else:
            self.collection = db.create_collection(collection_name)
        self.collection.ensure_index('timestamp',
                                     background=True)
        self.collection.ensure_index([('interaction-id', 1),('participant-session-id',1)],
                                     sparce = True,
                                     backgournd=True)

    # TODO move in a manager super class
    def set_property_helper(self, property_helper):
        self.property_helper = property_helper

    # TODO move in a manager super class
    def __getattr__(self,attr):
        orig_attr = self.collection.__getattribute__(attr)
        if callable(orig_attr):
            def hooked(*args, **kwargs):
                result = orig_attr(*args, **kwargs)
                # prevent wrapped_class from becoming unwrapped
                if result == self.collection:
                    return self
                return result
            return hooked
        else:
            return orig_attr

    def get_local_time(self, date_format='datetime'):
        return self.property_helper.get_local_time(date_format)
    
    def save_history(self, **kwargs):
        if 'timestamp' in kwargs:
            kwargs['timestamp'] = time_to_vusion_format(kwargs['timestamp'])
        else:
            kwargs['timestamp'] = self.get_local_time('vusion')
        if 'interaction' in kwargs:
            kwargs.pop('interaction')
        history = history_generator(**kwargs)
        self.collection.save(history.get_as_dict())

    def update_status(self, message_id, status):
        message_status = None
        failure_reason = None        
        if isinstance(status, dict):
            message_status = status['status']
            failure_reason = status['reason']
        else:
            message_status = status
        limit_timesearch = self.get_local_time() - timedelta(hours=6)        
        selector_query = {'message-id': message_id,
                          'timestamp': {'$gt' : time_to_vusion_format(limit_timesearch)}}
        update_query = {'$set': {'message-status': message_status}}
        if failure_reason is not None:
            update_query['$set'].update({'failure-reason': failure_reason})
        self.collection.update(selector_query, update_query)              

    def update_forwarded_status(self, message_id, status):
        message_status = None
        failure_reason = None
        if isinstance(status, dict):
            message_status = status['status']
            failure_reason = status['reason']
        else:
            message_status = status
        limit_timesearch = self.get_local_time() - timedelta(hours=3)
        selector_query = {'forwards.message-id': message_id,
                    'timestamp': {'$gt' : time_to_vusion_format(limit_timesearch)}}
        update_query = {'$set': {'forwards.$.status': message_status}}
        if failure_reason is not None:
            update_query['$set'].update({'forwards.$.failure-reason': failure_reason})
        self.collection.update(selector_query, update_query)

    def update_forwarding(self, history_id, message_id, to_addr):
        selector_query = {'_id': ObjectId(str(history_id))}
        update_query = {
            '$set': {'message-status': 'forwarded'},
            '$push': {'forwards': {'status': 'pending', 
                                   'timestamp': self.get_local_time('vusion'),
                                   'message-id': message_id,
                                   'to-addr': to_addr}}}
        self.collection.update(selector_query, update_query)
