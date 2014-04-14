from datetime import timedelta

from bson import ObjectId

from vusion.persist.model_manager import ModelManager
from vusion.utils import time_to_vusion_format
from history import history_generator


class HistoryManager(ModelManager):

    TIMESTAMP_LIMIT_ACK = 6          #in hours
    TIMESTAMP_LIMIT_ACK_FORWARD = 3  #in hours

    def __init__(self, db, collection_name, **kwargs):
        super(HistoryManager, self).__init__(db, collection_name, **kwargs)
        self.collection.ensure_index('timestamp',
                                     background=True)
        self.collection.ensure_index('participant-phone')
        self.collection.ensure_index([('interaction-id', 1),('participant-session-id',1)],
                                     sparce = True,
                                     background=True)
    
    def get_history(self, history_id):
        return self.collection.find_one({'_id': ObjectId(history_id)})
    
    def save_history(self, **kwargs):
        if 'timestamp' in kwargs:
            kwargs['timestamp'] = time_to_vusion_format(kwargs['timestamp'])
        else:
            kwargs['timestamp'] = self.get_local_time('vusion')
        if 'interaction' in kwargs:
            kwargs.pop('interaction')
        history = history_generator(**kwargs)
        return self.save_document(history)

    def update_status(self, message_id, status):
        message_status = None
        failure_reason = None        
        if isinstance(status, dict):
            message_status = status['status']
            failure_reason = status['reason']
        else:
            message_status = status
        limit_timesearch = self.get_local_time() - timedelta(hours=self.TIMESTAMP_LIMIT_ACK)
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
        limit_timesearch = self.get_local_time() - timedelta(hours=self.TIMESTAMP_LIMIT_ACK_FORWARD)
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
