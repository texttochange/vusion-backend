import sys, traceback
from uuid import uuid4

from vusion.utils import time_to_vusion_format
from vusion.persist import Participant, ModelManager


class ParticipantManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(ParticipantManager, self).__init__(db, collection_name, **kwargs)
        self.collection.ensure_index('phone', background=True)

    def opting_in(self, participant_phone, safe=True):
        participant = Participant(**{
            'phone': participant_phone,
            'session-id': uuid4().get_hex(), 
            'last-optin-date': time_to_vusion_format(self.get_local_time()),
            'last-optout-date': None,
            'tags': [],
            'enrolled':[],
            'profile':[]})
        return self.save_participant(participant, safe)

    def opting_in_again(self, participant_phone, safe=True):
        self.collection.update(
            {'phone': participant_phone},
            {'$set': {'session-id': uuid4().get_hex(), 
                      'last-optin-date': time_to_vusion_format(self.get_local_time()),
                      'last-optout-date': None,
                      'tags': [],
                      'enrolled': [],
                      'profile': [] }},
            safe=safe)

    def opting_out(self, participant_phone, safe=True):
        self.collection.update(
            {'phone': participant_phone},
            {'$set': {'session-id': None,
                      'last-optout-date': time_to_vusion_format(self.get_local_time())}},
            safe=safe)

    def tagging(self, participant_phone, tag, safe=True):
        self.collection.update(
            {'phone': participant_phone,
             'session-id': {'$ne': None},
             'tags': {'$ne': tag}},
            {'$push': {'tags': tag}},
            safe=safe)

    def enrolling(self, participant_phone, dialogue_id, safe=True):
        self.collection.update(
            {'phone': participant_phone,
             'enrolled.dialogue-id': {'$ne': dialogue_id}},
            {'$push': {'enrolled': {
                'dialogue-id': dialogue_id,
                'date-time': time_to_vusion_format(self.get_local_time())}}},
            safe=True)
    
    def labelling(self, participant_phone, label, value, raw, safe=True):
        self.collection.update(
            {'phone': participant_phone,
             'session-id': {'$ne': None}},
            {'$push': {'profile': {'label': label,
                                   'value': value,
                                   'raw': raw}}},
            safe=True)

    def save_participant(self, participant, safe=False):
        if isinstance(participant, Participant):
            participant = participant.get_as_dict()
        return self.collection.save(participant, safe)

    def get_participant(self, participant_phone, only_optin=False):
        try:
            query = {'phone':participant_phone}
            if only_optin:
                query.update({'session-id':{'$ne': None}})
            return Participant(**self.collection.find_one(query))
        except TypeError:
            self.log("Participant %s is either not optin or not in collection." % participant_phone)
            return None
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error while retriving participant %s  %r" %
                (participant_phone,
                 traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return None

    def get_participants(self, query):
        participants = []
        for participant in self.collection.find(query):
            try:
                participants.append(Participant(**participant))
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log(
                    "Error while retriving participant %r" %
                     traceback.format_exception(exc_type, exc_value, exc_traceback))
        return participants
    
    def is_tagged(self, participant_phone, tags):
        query = {'phone':participant_phone,
                 'tags': {'$in': tags}}
        result = self.collection.find(query).limit(1).count()
        return 0 < self.collection.find(query).limit(1).count()

    def is_optin(self, participant_phone):
        query = {'phone':participant_phone,
                 'session-id': {'$ne': None}}
        return 0 != self.collection.find(query).limit(1).count()

