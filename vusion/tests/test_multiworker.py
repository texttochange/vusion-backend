from twisted.trial.unittest import TestCase
from twisted.internet.defer import (Deferred, DeferredList,
                                    inlineCallbacks, returnValue)

import pymongo

from vumi.multiworker import MultiWorker
from vumi.tests.utils import StubbedWorkerCreator, get_stubbed_worker
from vumi.tests.test_multiworker import ToyWorker

from vusion import VusionMultiWorker, DialogueWorker
from tests.utils import MessageMaker, DataLayerUtils


class StubbedVusionMultiWorker(VusionMultiWorker):

    def WORKER_CREATOR(self, options):
        worker_creator = StubbedWorkerCreator(options)
        worker_creator.broker = self._amqp_client.broker
        return worker_creator

    def wait_for_workers(self):
        return DeferredList([w._d for w in self.workers.values()])


class VusionMultiWorkerTestCase(TestCase, MessageMaker, DataLayerUtils):
    timeout = 3

    base_config = {
        'application_name': 'vusion',
        'vusion_database_name': 'test3',
        'mongodb_host': 'localhost',
        'mongodb_port': 27017,
        'dispatcher_name': 'dispatcher',
        'workers': {
            'worker1': 'vusion.DialogueWorker'},
        'worker1': {
            'control_name': 'test',
            'transport_name': 'test',
            'database_name': 'test'},
        'defaults': {
            'mongodb_host': 'localhost',
            'mongodb_port': 27017,
            'dispatcher_name': 'dispatcher',
            'vusion_database_name': 'test3'}
    }

    new_worker_config = {
        'control_name': 'test2',
        'transport_name': 'test2',
        'database_name': 'test2',
    }

    def setUp(self):
        DataLayerUtils.__init__(self)
        self.conn = pymongo.Connection()
        self.db = self.conn[self.base_config['vusion_database_name']]
        self.setup_collection('workers')
        self.collections['workers'].drop()

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.wait_for_workers()
        yield self.worker.stopService()
        self.cleanData()

    def cleanData(self):
        self.conn.drop_database('test')
        self.conn.drop_database('test2')
        self.conn.drop_database('test3')
        self.conn.drop_database(self.base_config['vusion_database_name'])

    def send_control(self, rkey, message, exchange='vumi'):
        self.broker.publish_message(exchange,
                                    ('%s.control' % (rkey)),
                                    message)
        return self.broker.kick_delivery()

    @inlineCallbacks
    def get_multiwoker(self, config):
        self.worker = get_stubbed_worker(StubbedVusionMultiWorker, config)
        self.worker.startService()
        self.worker.startWorker()
        self.broker = self.worker._amqp_client.broker
        yield self.worker.wait_for_workers()
        returnValue(self.worker)

    @inlineCallbacks
    def test_add_remove_workers(self):

        yield self.get_multiwoker(self.base_config)

        self.assertEqual(self.collections['workers'].count(), 1)

        yield self.send_control(
            'vusion',
            self.mkmsg_multiworker_control(
                message_type='add_worker',
                worker_name='worker2',
                worker_class='vusion.DialogueWorker',
                config=self.new_worker_config))

        yield self.worker.wait_for_workers()

        self.assertEqual(self.collections['workers'].count(), 2)
        self.assertTrue('worker2' in self.worker.workers)

        yield self.send_control(
            'vusion',
            self.mkmsg_multiworker_control(
                message_type='add_worker',
                worker_name='worker2',
                worker_class='vusion.DialogueWorker',
                config=self.new_worker_config))

        yield self.worker.wait_for_workers()

        self.assertEqual(self.collections['workers'].count(), 2)
        self.assertTrue('worker2' in self.worker.workers)

        yield self.send_control(
            'vusion',
            self.mkmsg_multiworker_control(
                message_type='remove_worker',
                worker_name='worker2'))

        yield self.worker.wait_for_workers()

        self.assertEqual(self.collections['workers'].count(), 1)
        self.assertFalse('worker2' in self.worker.workers)

    @inlineCallbacks
    def test_startup(self):
        #The worker1 class and config store in the database are overwrite
        #by the config file
        self.collections['workers'].save({
            'name': 'worker1',
            'class': 'vumi.tests.test_multiworker.ToyWorker',
            'config': self.new_worker_config})

        self.collections['workers'].save({
            'name': 'worker2',
            'class': 'vumi.tests.test_multiworker.ToyWorker',
            'config': self.new_worker_config})

        yield self.get_multiwoker(self.base_config)
        yield self.worker.wait_for_workers()

        self.assertEqual(self.collections['workers'].count(), 2)
        worker_configs = self.collections['workers'].find()
        self.assertEqual(worker_configs[0]['class'], 'vusion.DialogueWorker')
        self.assertEqual(worker_configs[0]['model-version'], '2')
        self.assertEqual(
            worker_configs[1]['class'],
            'vumi.tests.test_multiworker.ToyWorker')
        self.assertEqual(worker_configs[1]['model-version'], '2')

        self.assertTrue('worker1' in self.worker.workers)
        self.assertTrue(isinstance(self.worker.workers['worker1'],
                                   DialogueWorker))
        self.assertTrue('worker2' in self.worker.workers)
        self.assertTrue(isinstance(self.worker.workers['worker2'],
                                   ToyWorker))
