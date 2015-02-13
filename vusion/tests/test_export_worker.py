import os.path
from pymongo import MongoClient
from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase, MessageHelper, WorkerHelper

from tests.utils import MessageMaker, ObjectMaker

from vusion.persist import (
    ParticipantManager, HistoryManager, history_generator, ScheduleManager,
    UnmatchableReplyManager)
from vusion.export_worker import ExportWorker


class ExportWorkerTestCase(VumiTestCase, MessageMaker, ObjectMaker):
    timeout = 100
    
    base_config = {
        'application_name': 'export',
        'mongodb_host': 'localhost',
        'mongodb_port': 27017,
        'redis_manager': {'host': 'localhost', 'port': 6379}}
    
    @inlineCallbacks
    def setUp(self):
        self.application_name = self.base_config['application_name']
        self.worker_helper = self.add_helper(WorkerHelper())
        self.message_helper = self.add_helper(MessageHelper())
        self.worker = yield self.worker_helper.get_worker(
            ExportWorker, self.base_config)
        self.mongo = MongoClient(w=1)

    @inlineCallbacks
    def tearDown(self):
        yield self.worker_helper.cleanup()
        self.cleanData()

    def cleanData(self):
        self.mongo.drop_database('test')
        if os.path.isfile('testing_export.csv'):
            os.remove('testing_export.csv')

    def dispatch_control(self, control):
        return self.worker_helper.dispatch_raw('.'.join([self.application_name, 'control']), control)

    @inlineCallbacks
    def test_export_participants_with_conditions(self):
        manager = ParticipantManager(self.mongo['test'], 'participants')
        manager.save_participant(self.mkobj_participant(
            tags=['geek', 'mombasa'],
            profile=[{'label': 'name',
                      'value': 'olivier'}]))
        manager.save_participant(self.mkobj_participant(
            tags=['nogeek'],
            profile=[{'label': 'name',
                      'value': 'steve'}]))
        
        control = self.mkmsg_exportworker_control(
            message_type='export_participants',
            file_full_name='testing_export.csv',
            database='test',
            conditions={'tags': 'mombasa'},
            redis_key='unittest:myprogramUrl:participants')
        yield self.dispatch_control(control)

        self.assertTrue(os.path.isfile('testing_export.csv'))
        with open('testing_export.csv', 'rb') as csvfile:
            self.assertEqual('phone,tags,name\n', csvfile.readline())
            self.assertEqual('"06","geek,mombasa","olivier"\n', csvfile.readline())
            self.assertEqual('', csvfile.readline())

    @inlineCallbacks
    def test_export_participants_no_conditions(self):
        manager = ParticipantManager(self.mongo['test'], 'participants')
        manager.save_participant(self.mkobj_participant(
            tags=['geek', 'mombasa'],
            profile=[{'label': 'name',
                      'value': 'olivier'}]))
        manager.save_participant(self.mkobj_participant(
            tags=['nogeek'],
            profile=[{'label': 'name',
                      'value': 'steve'},
                     {'label': 'age',
                      'value': '20'}]))
        
        control = self.mkmsg_exportworker_control(
            message_type='export_participants',
            file_full_name='testing_export.csv',
            database='test',
            conditions=[],
            redis_key='unittest:myprogramUrl:participants')
        yield self.dispatch_control(control)

        self.assertTrue(os.path.isfile('testing_export.csv'))
        with open('testing_export.csv', 'rb') as csvfile:
            self.assertEqual(
                'phone,tags,age,name\n', csvfile.readline())
            self.assertEqual(
                '"06","geek,mombasa",,"olivier"\n', csvfile.readline())
            self.assertEqual(
                '"06","nogeek","20","steve"\n', csvfile.readline())
            self.assertEqual('', csvfile.readline())

    @inlineCallbacks
    def test_export_participant_with_fakejoin(self):
        manager = ParticipantManager(self.mongo['test'], 'participants')
        participant = self.mkobj_participant(
            participant_phone='06',
            session_id='1',
            tags=['geek', 'mombasa'],
            profile=[{'label': 'name',
                      'value': 'olivier'}])
        manager.save_participant(participant)
        manager.save_participant(self.mkobj_participant(
            participant_phone='01',
            tags=['nogeek'],
            profile=[{'label': 'name',
                      'value': 'steve'}]))

        schedule_mgr = ScheduleManager(self.mongo['test'], 'schedules')
        schedule_mgr.add_reminder(
            participant,
            '2100-01-01T10:10:10',
            '01',
            '01')

        conditions = {
            '$and': [{
                'phone': {
                    '$join': {
                        'function': 'getUniqueParticipantPhone',
                        'field': 'phone',
                        'model': 'Schedule',
                        'parameters': {
                            'cursor': True}}}}
                ,{
                'phone': '06'}]}

        control = self.mkmsg_exportworker_control(
                    message_type='export_participants',
                    file_full_name='testing_export.csv',
                    database='test',
                    conditions=conditions,
                    redis_key='unittest:myprogramUrl:participants')

        yield self.dispatch_control(control)

        self.assertTrue(os.path.isfile('testing_export.csv'))
        with open('testing_export.csv', 'rb') as csvfile:
            self.assertEqual('phone,tags,name\n', csvfile.readline())
            self.assertEqual('"06","geek,mombasa","olivier"\n', csvfile.readline())
            self.assertEqual('', csvfile.readline())

    @inlineCallbacks
    def test_export_history(self):
        manager = HistoryManager(self.mongo['test'], 'history', None, None)
        h1 = self.mkobj_history_dialogue(
            '1', '1', '2015-01-20T10:10:10', message_content='hello')
        manager.save_document(history_generator(**h1))

        control = self.mkmsg_exportworker_control(
            message_type='export_history', 
            file_full_name='testing_export.csv', 
            conditions=None, 
            collection='history', 
            database='test',
            redis_key='unittest:myprogramUrl:history')

        yield self.dispatch_control(control)

        self.assertTrue(os.path.isfile('testing_export.csv'))
        with open('testing_export.csv', 'rb') as csvfile:
            self.assertEqual(
                'participant-phone,message-direction,message-status,message-content,timestamp\n', csvfile.readline())
            self.assertEqual(
                '"06","outgoing","delivered","hello","2015-01-20T10:10:10"\n', csvfile.readline())
            self.assertEqual('', csvfile.readline())

    @inlineCallbacks
    def test_export_unmatchable_reply(self):
        manager = UnmatchableReplyManager(self.mongo['test'], 'unmatchable-reply')
        unmatchable = self.mkobj_unmatchable_reply()
        manager.save_document(unmatchable)

        control = self.mkmsg_exportworker_control(
            message_type='export_unmatchable_reply',
            file_full_name='testing_export.csv',
            conditions=None,
            collection='unmatchable-reply',
            database='test',
            redis_key='unittest:myprogramUrl:unmatchable-reply')

        yield self.dispatch_control(control)

        self.assertTrue(os.path.isfile('testing_export.csv'))
        with open('testing_export.csv', 'rb') as csvfile:
            self.assertEqual(
                'from,to,message-content,timestamp\n', csvfile.readline())
            self.assertEqual(
                '"+25611111","256-8181","Hello","2014-01-01T10:10:00"\n', csvfile.readline())
            self.assertEqual('', csvfile.readline())