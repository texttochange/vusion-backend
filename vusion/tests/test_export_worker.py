import os.path
from pymongo import MongoClient
from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase, MessageHelper, WorkerHelper

from tests.utils import MessageMaker, ObjectMaker

from vusion.persist import ParticipantManager, HistoryManager, history_generator
from vusion.export_worker import ExportWorker


class ExportWorkerTestCase(VumiTestCase, MessageMaker, ObjectMaker):
    
    base_config = {
        'application_name': 'export',
        'mongodb_host': 'localhost',
        'mongodb_port': 27017}
    
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
            conditions={'tags': 'mombasa'})
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
            conditions=[])
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
            database='test')  

        yield self.dispatch_control(control)

        self.assertTrue(os.path.isfile('testing_export.csv'))
        with open('testing_export.csv', 'rb') as csvfile:
            self.assertEqual(
                'participant-phone,message-direction,message-status,message-content,timestamp\n', csvfile.readline())
            self.assertEqual(
                '"06","outgoing","delivered","hello","2015-01-20T10:10:10"\n', csvfile.readline())
            self.assertEqual('', csvfile.readline())
