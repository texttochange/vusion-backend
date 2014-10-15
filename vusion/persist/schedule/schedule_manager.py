import sys, traceback
from bson import ObjectId

from twisted.internet.threads import deferToThread
from twisted.internet.defer import returnValue, inlineCallbacks

from vusion.persist.cursor_instanciator import CursorInstanciator
from vusion.persist import ModelManager, schedule_generator
from vusion.persist.schedule.schedule import Schedule, UnattachSchedule


class ScheduleManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(ScheduleManager, self).__init__(db, collection_name, **kwargs)
        self.collection.ensure_index('date-time', backgound=True)
        self.collection.ensure_index([
            ('participant-phone',1), ('interaction-id', 1)], backgroun=True)

    def save_schedule(self, schedule):
        if not isinstance(schedule, Schedule):
            schedule = schedule_generator(**schedule)
        self.save_document(schedule)

    def remove_schedule(self, schedule):
        self.collection.remove(schedule['_id'])

    #This need to be logged in the history...
    def _generate_schedule(self, raw_schedule, remove_failure=True):
        try:
            if raw_schedule is None:
                return None
            return schedule_generator(**raw_schedule)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Exception while instanciating schedule %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
            if remove_failure:
                self._remove_failure(raw_schedule)
        return None

    def _remove_failure(self, exception, item):
        self.log("Deleting schedule from collection")
        self.collection.remove(item['_id'])

    def _wrap_cursor_schedules(self, cursor):
        def log(exception, item):
            self.log("Exception %s while intanciating a schedule %r" % (exception, item))        
        return CursorInstanciator(cursor, schedule_generator, log)        

    def get_participant_reminder_tail(self, participant_phone, dialogue_id, interaction_id):
        cursor = self.collection.find({
            "participant-phone": participant_phone,
            "$or":[{"object-type":'reminder-schedule'},
                   {"object-type": 'deadline-schedule'}],
            "dialogue-id": dialogue_id,
            "interaction-id": interaction_id})
        return self._wrap_cursor_schedules(cursor)

    def get_participant_unattach(self, participant_phone, unattach_id):
        return self._generate_schedule(self.collection.find_one({
            'participant-phone': participant_phone,
            'unattach-id': str(unattach_id)}))

    def get_participant_interaction(self, participant_phone, dialogue_id, interaction_id):
        return self._generate_schedule(self.collection.find_one({
            "participant-phone": participant_phone,
            "object-type": 'dialogue-schedule',
            "dialogue-id": dialogue_id,
            "interaction-id": interaction_id}))

    def get_next_schedule_time(self):
        schedules = self._wrap_cursor_schedules(
            self.collection.find(
                sort=[('date-time', 1)],
                limit=100))
        schedules.add_failure_callback(self._remove_failure)
        for schedule in schedules:
            if schedule is not None:
                return schedule.get_schedule_time()
        return None

    def get_due_schedules(self, limit=100):
        cursor = self.collection.find(
            spec={'date-time': {'$lt': self.get_local_time('vusion')}},
            sort=[('date-time', 1)], limit=limit)
        return self._wrap_cursor_schedules(cursor)
    
    def remove_participant_reminders(self, participant_phone, dialogue_id, interaction_id):
        self.collection.remove({
            'participant-phone': participant_phone,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id,
            'object-type': 'reminder-schedule'})

    def remove_participant_deadline(self, participant_phone, dialogue_id, interaction_id):
        self.remove({
            'participant-phone': participant_phone,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id,
            'object-type': 'deadline-schedule'})        
    
    def remove_participant_interaction(self, participant_phone, dialogue_id, interaction_id):
        self.collection.remove({
            'participant-phone': participant_phone,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id,
            'object-type': 'dialogue-schedule'})

    def remove_participant_schedules(self, participant_phone):
        self.collection.remove({
            'participant-phone': participant_phone,
            'object-type': {'$ne': 'feedback-schedule'}})    

    def remove_dialogue(self, dialogue_id):
        self.collection.remove({'dialogue-id': dialogue_id})

    def remove_unattach(self, unattach_id):
        self.collection.remove({'unattach-id': str(unattach_id)})

    @inlineCallbacks
    def remove_unattach_schedule(self, participant, unattach):
        d = deferToThread(self._remove_unattach_schedule, participant, unattach)
        yield d

    def _remove_unattach_schedule(self, participant, unattah):
        self.collection.remove({
            'participant-phone': participant['phone'],
            'unattach-id': str(unattah['_id'])})

    @inlineCallbacks
    def save_unattach_schedule(self, participant, unattach):
        d = deferToThread(self._save_unattach_schedule, participant, unattach)
        yield d

    def _save_unattach_schedule(self, participant, unattach):
        schedule = self.get_participant_unattach(
            participant['phone'], unattach['_id'])
        if schedule is None:
            schedule = UnattachSchedule(**{
                    'participant-phone': participant['phone'],
                    'participant-session-id': participant['session-id'],
                    'unattach-id': str(unattach['_id']),
                    'date-time': unattach['fixed-time']})
        else:
            schedule.set_time(unattach['fixed-time'])
        self.save_schedule(schedule)

    @inlineCallbacks
    def unattach_schedule(self, participant, unattach):
        if unattach.is_selectable(participant):
            yield self.save_unattach_schedule(participant, unattach)
        else:
            yield self.remove_unattach_schedule(participant, unattach)
