import sys, traceback

from vusion.persist import ModelManager, schedule_generator
from vusion.persist.schedule.schedule import Schedule


class ScheduleManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(ScheduleManager, self).__init__(db, collection_name, **kwargs)
        self.collection.ensure_index('date-time', backgound=True)
        self.collection.ensure_index([
            ('participant-phone',1), ('interaction-id', 1)], backgroun=True)

    def save_schedule(self, schedule):
        if isinstance(schedule, Schedule):
            schedule = schedule.get_as_dict()
        self.collection.save(schedule)

    def remove_schedule(self, schedule):
        self.collection.remove(schedule['_id'])

    #This need to be logged in the history...
    def _generate_schedule(self, raw_schedule, remove_failure=True):
        try:
            return schedule_generator(**raw_schedule)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error while retriving schedule %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
            if remove_failure:
                self.collection.remove(raw_schedule['_id'])
        return None

    def _generate_schedules(self, raw_schedules):
        schedules = []
        for raw_schedule in raw_schedules:
            schedule = self._generate_schedule(raw_schedule)
            if schedule is not None:
                schedules.append(schedule)
        return schedules

    def get_reminder_tail(self, participant_phone, dialogue_id, interaction_id):
        return self._generate_schedules(self.collection.find({
            "participant-phone": participant_phone,
            "$or":[{"object-type":'reminder-schedule'},
                   {"object-type": 'deadline-schedule'}],
            "dialogue-id": dialogue_id,
            "interaction-id": interaction_id}))

    def get_interaction(self, participant_phone, dialogue_id, interaction_id):
        pass

    def get_next_schedule_time(self):
        while(True):
            schedules = self.collection.find(
                sort=[('date-time', 1)],
                limit=1)
            if schedules.count() == 0:
                return None
            schedule = self._generate_schedule(schedules[0])
            if schedule is not None:
                return schedule.get_schedule_time()
    
    #TODO shall we also remove them at the same time
    def get_due_schedules(self, limit=100):
        return self._generate_schedules(
            self.collection.find(
                spec={'date-time': {'$lt': self.get_local_time('vusion')}},
                sort=[('date-time', 1)], limit=limit))
    
    def remove_reminders(self, participant_phone, dialogue_id, interaction_id):
        self.collection.remove({
            'participant-phone': participant_phone,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id,
            'object-type': 'reminder-schedule'})
    
    def remove_deadline(self, participant_phone, dialogue_id, interaction_id):
        self.remove({
            'participant-phone': participant_phone,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id,
            'object-type': 'deadline-schedule'})        
    
    def remove_interaction(self, participant_phone, dialogue_id, interaction_id):
        self.collection.remove({
            'participant-phone': participant_phone,
            'dialogue-id': dialogue_id,
            'interaction-id': interaction_id,
            'object-type': 'dialogue-schedule'})
    
    def remove_dialogue(self, participant_phone, dialogue_id):
        pass

    def remove_unattach(self):
        pass
    
    def remove_schedules(self, participant_phone):
        self.collection.remove({
            'participant-phone': participant_phone,
            'object-type': {'$ne': 'feedback-schedule'}})
