import sys
import traceback
import os

from bson.son import SON
from bson.code import Code
from pymongo import DESCENDING

from uuid import uuid4
from twisted.internet.threads import deferToThread
from twisted.internet.defer import returnValue, inlineCallbacks

from vusion.utils import time_to_vusion_format
from vusion.persist import Participant, ModelManager
from vusion.persist.cursor_instanciator import CursorInstanciator


class ParticipantManager(ModelManager):

    def __init__(self, db, collection_name, **kwargs):
        super(ParticipantManager, self).__init__(db, collection_name, True, **kwargs)
        self.collection.ensure_index('phone', background=True)

    ## return False if the participant is already optin
    def opting_in(self, participant_phone, simulated=False):
        participant = self.get_participant(participant_phone)
        if not participant:
            ## The participant is opting in for the first time
            self.opting_in_first(participant_phone, simulated)
            return True
        elif participant['session-id'] is None:
            ## The participant is optout and opting in again
            self.opting_in_again(participant_phone)
            return True
        return False

    def opting_in_first(self, participant_phone, simulated=False):
        participant = Participant(**{
            'phone': participant_phone,
            'session-id': uuid4().get_hex(), 
            'last-optin-date': self.get_local_time(),
            'last-optout-date': None,
            'tags': [],
            'enrolled':[],
            'profile':[],
            'simulate': simulated})
        return self.save_participant(participant)

    def opting_in_again(self, participant_phone):
        self.collection.update(
            {'phone': participant_phone},
            {'$set': {'session-id': uuid4().get_hex(), 
                      'last-optin-date': time_to_vusion_format(self.get_local_time()),
                      'last-optout-date': None,
                      'tags': [],
                      'enrolled': [],
                      'profile': []}})

    def opting_out(self, participant_phone):
        self.collection.update(
            {'phone': participant_phone},
            {'$set': {'session-id': None,
                      'last-optout-date': time_to_vusion_format(self.get_local_time())}})

    def tagging(self, participant_phone, tag):
        self.collection.update(
            {'phone': participant_phone,
             'session-id': {'$ne': None},
             'tags': {'$ne': tag}},
            {'$push': {'tags': tag}})

    def enrolling(self, participant_phone, dialogue_id):
        self.enrolling_participants(
            {'phone': participant_phone}, dialogue_id)

    def enrolling_participants(self, query, dialogue_id, multi=True):
        ##unenroll participants that are no more auto
        query.update({'enrolled.dialogue-id': {'$ne': dialogue_id},
                      'session-id':{'$ne': None}})
        self.collection.update(
            query,
            {'$push': {'enrolled': {
                'dialogue-id': dialogue_id,
                'date-time': self.get_local_time("vusion")}}},
            multi=multi)
    
    def labelling(self, participant_phone, label, value, raw):
        self.collection.update(
                    {'phone': participant_phone,
                     'session-id': {'$ne': None}},
                    {'$pull': {'profile': {'label': label}}})
        self.collection.update(
            {'phone': participant_phone,
             'session-id': {'$ne': None}},
            {'$push': {'profile': {'label': label,
                                   'value': value,
                                   'raw': raw}}})


    def save_transport_metadata(self, participant_phone, transport_metadata):
        self.collection.update(
            {'phone': participant_phone},
            {'$set': {'transport_metadata': transport_metadata}})

    def save_participant(self, participant):
        if not isinstance(participant, Participant):
            participant = Participant(**participant)
        return self.save_document(participant)

    def get_participant(self, participant_phone, only_optin=False):
        try:
            query = {'phone':participant_phone}
            if only_optin:
                query.update({'session-id':{'$ne': None}})
            return Participant(**self.collection.find_one(query))
        except TypeError:
            self.log("Participant phone %s is either not optin or not in collection." % participant_phone)
            return None
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error while retriving participant %s  %r" %
                (participant_phone,
                 traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return None

    def get_participants(self, query=None, sort=None):
        def log(exception, item=None):
            self.log("Exception %r while instanciating a participant %r" % (exception, item))
        return CursorInstanciator(
            self.collection.find(query, sort=sort), Participant, [log])

    @inlineCallbacks
    def get_labels(self, query=None):
        d = deferToThread(self._get_labels_async, query)
        yield d

    def _get_labels_async(self, query):
        pipeline = []
        if query != None and query != []:
            pipeline.append({'$match': query})
        pipeline.append({'$project': {'_id': 0, 'profile': 1}})
        pipeline.append({'$unwind': '$profile'})
        pipeline.append({'$group': {'_id': '$profile.label'}})
        cursor = self.aggregate(pipeline, cursor={})
        results = []
        for label in cursor:
            results.append(label['_id'])
        returnValue(results)

    def is_tagged(self, participant_phone, tags):
        query = {'phone': participant_phone,
                 'tags': {'$in': tags}}
        return 0 < self.collection.find(query).limit(1).count()

    def is_labelled(self, participant_phone, label_name):
        query = {'phone': participant_phone,
                 'profile': {'$elemMatch': {'label': label_name}}}
        return 0 < self.collection.find(query).limit(1).count()

    def is_optin(self, participant_phone):
        query = {'phone':participant_phone,
                 'session-id': {'$ne': None}}
        return 0 != self.collection.find(query).limit(1).count()

    def is_matching(self, query):
        return 1 == self.collection.find(query).limit(1).count()

    ## The call is async because the count on program with many participants will take a long time
    @inlineCallbacks
    def count_tag_async(self, tag):
        d = deferToThread(self._count_tag_async, tag)
        yield d

    def _count_tag_async(self, tag):
        returnValue(self.collection.find({'tags': tag}).count())

    ## The call is async because the count on program with many participants will take a long time
    @inlineCallbacks
    def count_label_async(self, label):
        d = deferToThread(self._count_label_async, label)
        yield d

    def _count_label_async(self, label):
        returnValue(self.collection.find({
            'profile': {
                '$elemMatch': {
                    'label': label['label'],
                    'value': label['value']}}}).count())

    def aggregate_count_per_day(self, local_time):
        last_day = self.stats_collection.find_one(sort=[('_id', DESCENDING)])
        file_dir = os.path.dirname(os.path.realpath(__file__))
        map_fct = open("%s/aggregate_count_per_day_map.js" % file_dir).read()
        local_date = local_time.strftime("%Y-%m-%d")
        if last_day is None:
            map_fct = map_fct % ('this["last-optin-date"].substring(0,10)', local_date)
        else:
            map_fct = map_fct % ('"%s"' % last_day['_id'], local_date)
        map = Code(map_fct)
        reduce = Code(open("%s/aggregate_count_per_day_reduce.js" % file_dir).read())
        self.collection.map_reduce(
            map,
            reduce, 
            out=SON([
                ("merge", self.stats_collection_name)]))
        return
