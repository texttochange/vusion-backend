from datetime import timedelta

from bson import ObjectId, Code
from pymongo import ASCENDING

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread

from vusion.persist.model_manager import ModelManager
from vusion.utils import (time_to_vusion_format, time_to_vusion_format_date, 
                          date_from_vusion_format)
from vusion.component.flying_messsage_manager import FlyingMessageManager
from history import history_generator


class HistoryManager(ModelManager):

    #Deprecated
    TIMESTAMP_LIMIT_ACK = 6          #in hours
    TIMESTAMP_LIMIT_ACK_FORWARD = 3  #in hours

    def __init__(self, db, collection_name, prefix_key, redis, **kwargs):
        super(HistoryManager, self).__init__(db, collection_name, **kwargs)
        self.collection.ensure_index('timestamp',
                                     background=True)
        self.collection.ensure_index('participant-phone',
                                     background=True)
        self.collection.ensure_index([('interaction-id', 1),('participant-session-id',1)],
                                     sparce = True,
                                     background=True)
        self.collection.ensure_index('unattach-id',
                                     sparce = True,
                                     background=True)
        self.prefix_key = prefix_key
        self.flying_manager = FlyingMessageManager(prefix_key, redis)

    def get_history(self, history_id):
        result = self.collection.find_one({'_id': ObjectId(history_id)})
        if result is None:
            return None
        return history_generator(**result)

    def save_history(self, **kwargs):
        if 'timestamp' in kwargs and not isinstance(kwargs['timestamp'], str):
            kwargs['timestamp'] = time_to_vusion_format(kwargs['timestamp'])
        else:
            kwargs['timestamp'] = self.get_local_time('vusion')
        if 'interaction' in kwargs:
            kwargs.pop('interaction')
        history = history_generator(**kwargs)
        history_id = self.save_document(history)
        if history.is_message() and history.is_outgoing():
            self.flying_manager.append_message_data(
                history['message-id'],
                history_id,
                history['message-credits'],
                history['message-status'])
        return history_id

    def update_status_from_event(self, event):
        history_id, credits, old_status = self.flying_manager.get_message_data(event['user_message_id'])
        if history_id is None:
            self.log("Cannot find flying message %s, cannot proceed updating the history" % event['user_message_id'])
            return None
        if (event['event_type'] == 'ack'):
            status = 'ack'
            new_status = status
        elif (event['event_type'] == 'delivery_report'):
            status = event['delivery_status']
            new_status = status
            if (event['delivery_status'] == 'failed'):
                status = {
                  'status': event['delivery_status'],
                  'reason': ("Level:%s Code:%s Message:%s" % (
                      event.get('failure_level', 'unknown'),
                      event.get('failure_code', 'unknown'),
                      event.get('failure_reason', 'unknown')))}
                credit_status = event['delivery_status']
        if ('transport_type' in event['transport_metadata']
           and event['transport_metadata']['transport_type'] == 'http_forward'):
            self.update_forwarded_status(history_id, event['user_message_id'], status)
        else:
            self.update_status(history_id, status)
        self.flying_manager.append_message_data(
            event['user_message_id'],
            history_id,
            credits,
            new_status)
        return new_status, old_status, credits


    def update_status(self, history_id, status):
        message_status = None
        failure_reason = None        
        if isinstance(status, dict):
            message_status = status['status']
            failure_reason = status['reason']
        else:
            message_status = status
        selector_query = {'_id': history_id}
        update_query = {'$set': {'message-status': message_status}}
        if failure_reason is not None:
            update_query['$set'].update({'failure-reason': failure_reason})
        self.collection.update(selector_query, update_query)              

    def update_forwarded_status(self, history_id, message_id, status):
        message_status = None
        failure_reason = None
        if isinstance(status, dict):
            message_status = status['status']
            failure_reason = status['reason']
        else:
            message_status = status
        limit_timesearch = self.get_local_time() - timedelta(hours=self.TIMESTAMP_LIMIT_ACK_FORWARD)
        selector_query = {
            '_id': history_id,
            'forwards.message-id': message_id}
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
        self.flying_manager.append_message_data(message_id, history_id, 0, 'pending')

    def count_day_credits(self, date):
        reducer = Code("function(obj, prev) {"
                       "    credits = 0;"
                       "    if ('message-credits' in obj) {"
                       "        credits = obj['message-credits'];"
                       "    } else {"
                       "        credits = 1;"
                       "    }"
                       "    switch (obj['message-direction']) {"
                       "    case 'incoming':"
                       "        prev['incoming'] = prev['incoming'] + credits;"
                       "        break;"
                       "    case 'outgoing':"
                       "        prev['outgoing'] = prev['outgoing'] + credits;"
                       "        switch (obj['message-status']) {"
                       "        case 'ack':"
                       "              prev['outgoing-acked'] = prev['outgoing-acked'] + credits;"
                       "              break;"
                       "        case 'delivered':"
                       "              prev['outgoing-delivered'] = prev['outgoing-delivered'] + credits;"
                       "              break;"
                       "        case 'failed':"
                       "              prev['outgoing-failed'] = prev['outgoing-failed'] + credits;"
                       "              break;"
                       "        case 'nack':"
                       "              prev['outgoing-nacked'] = prev['outgoing-nacked'] + credits;"
                       "              break;"
                       "        case 'pending':"
                       "              prev['outgoing-pending'] = prev['outgoing-pending'] + credits;"
                       "              break;"
                       "        }"
                       "        break;"
                       "     }"
                       " }")
        conditions = {
            "timestamp": {
                "$gte": time_to_vusion_format_date(date),
                "$lt": time_to_vusion_format_date(date + timedelta(days=1))},
            "object-type": {"$in": ["dialogue-history", "unattach-history", "request-history"]}}
        counters =  {"incoming": 0,
                     "outgoing": 0,
                     "outgoing-pending": 0,
                     "outgoing-acked": 0,
                     "outgoing-nacked": 0,
                     "outgoing-failed": 0,
                     "outgoing-delivered": 0}
        result = self.group(None, conditions, counters, reducer)
        if len(result) == 0:
            return counters
        counters = result[0]
        return {k : int(float(counters[k])) for k in counters.iterkeys()}

    def get_older_date(self, date=None):
        if date is None:
            date = self.get_local_time() + timedelta(days=1)
        date = date.replace(hour=0, minute=0, second=0)
        cursor = self.find(
            {'timestamp': {'$lt': time_to_vusion_format(date)}}).sort('timestamp', -1).limit(1)
        if cursor.count() == 0:
            return None
        try:
            history = history_generator(**cursor.next())
            return date_from_vusion_format(history['timestamp'])
        except Exception as e:
            self.log_helper.log(e.message)
            return None

    def get_status_and_credits(self, user_message_id):
        limit_timesearch = self.get_local_time() - timedelta(hours=self.TIMESTAMP_LIMIT_ACK)
        return self.collection.find_one(
            {'message-id': user_message_id,
             'timestamp': {'$gt' : time_to_vusion_format(limit_timesearch)}},
            ['message-status', 'message-credits'])

    @inlineCallbacks
    def was_unattach_sent(self, participant_phone, unattach_id):
        d = deferToThread(
            self._was_unattach_sent, participant_phone, unattach_id)
        yield d

    def _was_unattach_sent(self, participant_phone, unattach_id):
        unattach_history = self.collection.find_one({
            'participant-phone': participant_phone,
            'unattach-id': str(unattach_id)})
        if unattach_history is None:
            returnValue(False)
        returnValue(True)

    def get_history_of_interaction(self, participant, dialogue_id, interaction_id):
        result = self.collection.find_one(
            {'participant-phone': participant['phone'],
             'participant-session-id': participant['session-id'],
             'dialogue-id': dialogue_id,
             'interaction-id': interaction_id,
             '$or': [{'message-direction': 'outgoing'},
                     {'message-direction': 'incoming',
                      'matching-answer': {'$ne':None}}]},
            sort=[('timestamp', ASCENDING)])
        if result is None:
            return None
        return history_generator(**result)


    def get_history_of_offset_condition_answer(self, participant, dialogue_id,
                                               interaction_id):
        result = self.collection.find_one(
            {"participant-phone": participant['phone'],
             "participant-session-id": participant['session-id'],
             "message-direction": 'incoming',
             "dialogue-id": dialogue_id,
             "interaction-id": interaction_id,
             "$or": [{'matching-answer': {'$exists': False}},
                     {'matching-answer': {'$ne': None}}]})
        if result is None:
            return None
        return history_generator(**result)

    def add_oneway_marker(self, participant_phone, participant_session_id,
                          context):
        if self.has_oneway_marker(participant_phone, participant_session_id, 
                                  context['dialogue-id'], context['interaction-id']):
            return
        history = {
            'object-type': 'oneway-marker-history',
            'participant-phone': participant_phone,
            'participant-session-id':participant_session_id,
            'dialogue-id': context['dialogue-id'],
            'interaction-id': context['interaction-id']}
        self.save_history(**history)

    #def has_oneway_marker(self, participant, dialogue_id, interaction_id):
        #return self.has_oneway_marker(
            #participant['phone'], participant['session-id'],
            #dialogue_id, interaction_id)

    def has_oneway_marker(self, participant_phone, participant_session_id,
                          dialogue_id, interaction_id):
        return self.collection.find_one({
            'object-type': 'oneway-marker-history',
            'participant-phone': participant_phone,
            'participant-session-id':participant_session_id,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id}) is not None

    def participant_has_max_unmatching_answers(self, participant, dialogue_id, interaction):
        if (not interaction.has_max_unmatching_answers()):
            return False
        query = {'participant-phone': participant['phone'],
                 'participant-session-id':participant['session-id'],
                 'message-direction': 'incoming',
                 'dialogue-id': dialogue_id,
                 'interaction-id': interaction['interaction-id'],
                 'matching-answer': None}
        history = self.collection.find(query)
        if history.count() == int(interaction['max-unmatching-answer-number']):
            return True
        return False
    
    #TODO: move to History Manager
    def has_already_valid_answer(self, participant, dialogue_id, interaction_id, number=1):
        query = {'participant-phone': participant['phone'],
                 'participant-session-id':participant['session-id'],
                 'message-direction': 'incoming',
                 'matching-answer': {'$ne': None},
                 'dialogue-id': dialogue_id,
                 'interaction-id': interaction_id}
        history = self.collection.find(query)
        if history is None or history.count() <= number:
            return False
        return True

    def add_outgoing(self, message, message_credits, context, schedule):
        self.log("Message has been sent to %s '%s'" % (message['to_addr'], message['content']))
        history = {
            'message-content': message['content'],
            'participant-phone': message['to_addr'],
            'message-direction': 'outgoing',
            'message-status': 'pending',
            'message-id': message['message_id'],
            'message-credits': message_credits}
        history.update(context.get_dict_for_history(schedule))
        return self.save_history(**history)

    def add_nocredit(self, message_content, context, schedule):
        self.log("NO CREDIT, message '%s' hasn't been sent to %s" % (
            message_content, schedule['participant-phone']))        
        history = {
             'message-content': message_content,
             'participant-phone': schedule['participant-phone'],
             'message-direction': 'outgoing',
             'message-status': 'no-credit',
             'message-id': None,
             'message-credits': 0}
        history.update(context.get_dict_for_history(schedule))
        return self.save_history(**history)

    def add_nocredittimeframe(self, message_content, context, schedule):
        self.log("OUT OF CREDIT TIMEFRAME, message '%s' hasn't been sent to %s" % (
             message_content, schedule['participant-phone']))        
        history = {
             'message-content': message_content,
             'participant-phone': schedule['participant-phone'],
             'message-direction': 'outgoing',
             'message-status': 'no-credit-timeframe',
             'message-id': None,
             'message-credits': 0}
        history.update(context.get_dict_for_history(schedule))
        return self.save_history(**history)

    def add_missingdata(self, message_content, error_message, context, schedule):
        self.log("MISSING DATA(%s): message '%s' hasn't been send to %s" % (
            error_message, message_content, schedule['participant-phone']))
        history = {
            'message-content': message_content,
            'participant-phone': schedule['participant-phone'],
            'message-direction': 'outgoing',
            'message-status': 'missing-data',
            'missing-data': [error_message],
            'message-id': None,
            'message-credits': 0}
        history.update(context.get_dict_for_history(schedule))
        self.save_history(**history)

    def add_datepassed_action_marker(self, action, schedule):
        self.log("ADD DATEPASSED ACTION: action '%s' has been added for %s" % (
                   action.get_type(), schedule['participant-phone']))
        history = {
            'object-type': 'datepassed-action-marker-history',
            'participant-phone': schedule['participant-phone'],
            'participant-session-id': schedule['participant-session-id'],
            'action-type': action.get_type(),
            'scheduled-date-time': schedule['date-time']}
        self.save_history(**history)

    def add_datepassed_marker_for_interaction(self, participant, dialogue_id, interaction_id):
        self.log("ADD DATEPASSED: the message dialogue %s and interaction %s hasn't been send to %s" % (
                   dialogue_id, interaction_id, participant['phone']))
        history = {
            'object-type': 'datepassed-marker-history',
            'participant-phone': participant['phone'],
            'participant-session-id': participant['session-id'],
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id}
        return self.save_history(**history)

    def add_datepassed_marker(self, schedule, context):
        self.log("ADD DATEPASSED: the schedule %r from context %r hasn't been send" % (
            schedule, context))        
        history = {
            'object-type': 'datepassed-marker-history',
            'participant-phone': schedule['participant-phone'],
            'participant-session-id': schedule['participant-session-id'],
            'scheduled-date-time': schedule['date-time']}
        history.update(context.get_dict_for_history())
        return self.save_history(**history)

    def count_reminders(self, participant, dialogue_id, interaction_id):
        count = self.collection.find({
            'participant-phone': participant['phone'],
            'participant-session-id': participant['session-id'],
            'message-direction': 'outgoing',
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id}).count()
        return count - 1 if count > 0 else 0
        