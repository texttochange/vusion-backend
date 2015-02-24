# encoding:utf-8
import os.path
from pymongo import MongoClient
from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase, MessageHelper, WorkerHelper

from tests.utils import MessageMaker, ObjectMaker

from vusion.persist import (
    ParticipantManager, HistoryManager, history_generator, ScheduleManager,
    UnmatchableReplyManager, ExportManager, Export)
from vusion.export_worker import ExportWorker


class ExportWorkerTestCase(VumiTestCase, MessageMaker, ObjectMaker):

    base_config = {
        'application_name': 'export',
        'database': 'test_vusion',
        'mongodb_host': 'localhost',
        'mongodb_port': 27017,
        'max_total_export_megabytes': 1.0
    }

    @inlineCallbacks
    def setUp(self):
        self.application_name = self.base_config['application_name']
        self.worker_helper = self.add_helper(WorkerHelper())
        self.message_helper = self.add_helper(MessageHelper())
        self.worker = yield self.worker_helper.get_worker(
            ExportWorker, self.base_config)
        self.mongo = MongoClient(w=1)
        self.exports = ExportManager(
            self.mongo[self.base_config['database']], 'exports')

    @inlineCallbacks
    def tearDown(self):
        yield self.worker_helper.cleanup()
        self.cleanData()

    def cleanData(self):
        self.mongo.drop_database('test_vusion')
        self.mongo.drop_database('test_program')
        if os.path.isfile('testing_export.csv'):
            os.remove('testing_export.csv')

    def dispatch_control(self, control):
        return self.worker_helper.dispatch_raw('.'.join([self.application_name, 'control']), control)

    @inlineCallbacks
    def test_export_participants_with_conditions(self):
        manager = ParticipantManager(
            self.mongo['test_program'],
            'participants')
        manager.save_participant(self.mkobj_participant(
            tags=['geek', 'mombasa'],
            profile=[{'label': 'name',
                      'value': 'olivier'}]))
        manager.save_participant(self.mkobj_participant(
            tags=['nogeek'],
            profile=[{'label': 'name',
                      'value': 'steve'}]))

        export = Export(**self.mkdoc_export(
            database='test_program',
            collection='participants',
            file_full_name='testing_export.csv',
            conditions={'tags': 'mombasa'}))
        export_id = self.exports.save_object(export)

        control = self.mkmsg_exportworker_control(export_id=str(export_id))
        yield self.dispatch_control(control)

        self.assertTrue(os.path.isfile('testing_export.csv'))
        with open('testing_export.csv', 'rb') as csvfile:
            self.assertEqual('phone,tags,name\n', csvfile.readline())
            self.assertEqual('"06","geek,mombasa","olivier"\n', csvfile.readline())
            self.assertEqual('', csvfile.readline())

        export = self.exports.get_export(export_id)
        self.assertEqual(export['status'], 'success')

    @inlineCallbacks
    def test_export_participants_no_conditions(self):
        manager = ParticipantManager(
            self.mongo['test_program'],
            'participants')
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

        export = Export(**self.mkdoc_export(
                    database='test_program',
                    collection='participants',
                    file_full_name='testing_export.csv',
                    conditions={}))
        export_id = self.exports.save_object(export)

        control = self.mkmsg_exportworker_control(export_id=str(export_id))
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

        export = self.exports.get_export(export_id)
        self.assertEqual(export['status'], 'success')

    @inlineCallbacks
    def test_export_participant_with_fakejoin(self):
        manager = ParticipantManager(
            self.mongo['test_program'],
            'participants')
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

        schedule_mgr = ScheduleManager(
            self.mongo['test_program'],
            'schedules')
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

        export = Export(**self.mkdoc_export(
                            database='test_program',
                            collection='participants',
                            file_full_name='testing_export.csv',
                            conditions=conditions))
        export_id = self.exports.save_object(export)

        control = self.mkmsg_exportworker_control(export_id=str(export_id))
        yield self.dispatch_control(control)

        self.assertTrue(os.path.isfile('testing_export.csv'))
        with open('testing_export.csv', 'rb') as csvfile:
            self.assertEqual('phone,tags,name\n', csvfile.readline())
            self.assertEqual('"06","geek,mombasa","olivier"\n', csvfile.readline())
            self.assertEqual('', csvfile.readline())

        export = self.exports.get_export(export_id)
        self.assertEqual(export['status'], 'success')

    @inlineCallbacks
    def test_export_history(self):
        manager = HistoryManager(
            self.mongo['test_program'], 'history', None, None)
        h1 = self.mkobj_history_dialogue(
            '1', '1', '2015-01-20T10:10:10', message_content='hello ±')
        manager.save_document(history_generator(**h1))

        export = Export(**self.mkdoc_export(
                            database='test_program',
                            collection='history',
                            file_full_name='testing_export.csv',
                            conditions={}))
        export_id = self.exports.save_object(export)

        control = self.mkmsg_exportworker_control(export_id=str(export_id))
        yield self.dispatch_control(control)

        self.assertTrue(os.path.isfile('testing_export.csv'))
        with open('testing_export.csv', 'rb') as csvfile:
            self.assertEqual(
                'participant-phone,message-direction,message-status,message-content,timestamp\n', csvfile.readline())
            self.assertEqual(
                '"06","outgoing","delivered","hello ±","2015-01-20T10:10:10"\n', csvfile.readline())
            self.assertEqual('', csvfile.readline())

        export = self.exports.get_export(export_id)
        self.assertEqual(export['status'], 'success')

    @inlineCallbacks
    def test_export_unmatchable_reply(self):
        manager = UnmatchableReplyManager(
            self.mongo['test_vusion'], 'unmatchable-reply')
        unmatchable = self.mkobj_unmatchable_reply()
        manager.save_document(unmatchable)

        export = Export(**self.mkdoc_export(
                            database='test_vusion',
                            collection='unmatchable-reply',
                            file_full_name='testing_export.csv',
                            conditions={}))
        export_id = self.exports.save_object(export)

        control = self.mkmsg_exportworker_control(export_id=str(export_id))
        yield self.dispatch_control(control)

        export = self.exports.get_export(export_id)
        self.assertEqual(export['status'], 'success')

        self.assertTrue(os.path.isfile('testing_export.csv'))
        with open('testing_export.csv', 'rb') as csvfile:
            self.assertEqual(
                'from,to,message-content,timestamp\n', csvfile.readline())
            self.assertEqual(
                '"+25611111","256-8181","Hello","2014-01-01T10:10:00"\n', csvfile.readline())
            self.assertEqual('', csvfile.readline())

    @inlineCallbacks
    def test_export_failed_max_total_export_size(self):
        export_old = Export(**self.mkdoc_export(
            database='test_program',
            collection='history',
            file_full_name='testing_export_old.csv',
            conditions={},
            status='success',
            size=1048577L))  #1Mo + 1byte
        self.exports.save_object(export_old)

        export = Export(**self.mkdoc_export(
            database='test_program',
            collection='history',
            file_full_name='testing_export.csv',
            conditions={}))
        export_id = self.exports.save_object(export)

        control = self.mkmsg_exportworker_control(export_id=str(export_id))
        yield self.dispatch_control(control)

        export = self.exports.get_export(export_id)
        self.assertEqual(export['status'], 'no-space')
        self.assertFalse(os.path.isfile('testing_export.csv'))
