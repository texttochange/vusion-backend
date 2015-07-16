from twisted.trial.unittest import TestCase
from twisted.internet.defer import (Deferred, DeferredList,
                                    inlineCallbacks, returnValue)

from pymongo import MongoClient

from vumi.multiworker import MultiWorker
from vumi.message import TransportUserMessage
from vumi.tests.utils import StubbedWorkerCreator, get_stubbed_worker
#from vumi.tests.test_multiworker import ToyWorker
from vumi.tests.helpers import VumiTestCase, WorkerHelper, MessageHelper
from vumi.service import Worker

from vusion import VusionMultiWorker, DialogueWorker
from tests.utils import MessageMaker, DataLayerUtils


class ToyWorker(Worker):
    events = []

    def __init__(self, *args):
        self._d = Deferred()
        return super(ToyWorker, self).__init__(*args)

    @inlineCallbacks
    def startWorker(self):
        self.events.append("START: %s" % self.name)
        self.pub = yield self.publish_to("%s.outbound" % self.name)
        yield self.consume("%s.inbound" % self.name, self.process_message,
                           message_class=TransportUserMessage)
        self._d.callback(None)

    def stopWorker(self):
        self.events.append("STOP: %s" % self.name)

    def process_message(self, message):
        return self.pub.publish_message(
            message.reply(''.join(reversed(message['content']))))


class ToyDialogueWorker(DialogueWorker):
    
    def __init__(self, *args):
        self._d = Deferred()
        return super(ToyDialogueWorker, self).__init__(*args)
    
    def setup_application(self):
        super(ToyDialogueWorker, self).setup_application()
        self._d.callback(None)

    def teardown_application(self):
        super(ToyDialogueWorker, self).teardown_application()

    def before_teardown_application(self):
        super(ToyDialogueWorker, self).before_teardown_application()


class StubbedVusionMultiWorker(VusionMultiWorker):

    def WORKER_CREATOR(self, options):
        worker_creator = StubbedWorkerCreator(options)
        worker_creator.broker = self._amqp_client.broker
        return worker_creator

    def wait_for_workers(self):
        return DeferredList([w._d for w in self.workers.values()])


class VusionMultiWorkerTestCase(VumiTestCase, MessageMaker):
    timeout = 10

    base_config = {
        'application_name': 'vusion',
        'vusion_database_name': 'test3',
        'mongodb_host': 'localhost',
        'mongodb_port': 27017,
        'dispatcher_name': 'dispatcher',
        'workers': {
            'worker1': '%s.ToyDialogueWorker' % (__name__,) },
        'worker1': {
            'control_name': 'test1',
            'transport_name': 'test1',
            'database_name': 'test1'},
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
        self.worker_helper = self.add_helper(WorkerHelper())
        self.message_helper = self.add_helper(MessageHelper())
        
        self.application_name = self.base_config['application_name']
        
        self.mongo_client = MongoClient(w=1)
        self.cleanData()
        db = self.mongo_client[self.base_config['vusion_database_name']]
        self.collections = {} 
        self.collections['workers'] = db['workers']
        
    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopService()
        yield super(VusionMultiWorkerTestCase, self).tearDown()   
        self.cleanData()

    def cleanData(self):
        self.mongo_client.drop_database('test1')
        self.mongo_client.drop_database('test2')
        self.mongo_client.drop_database('test3')
        self.mongo_client.drop_database(self.base_config['vusion_database_name'])

    def dispatch_control(self, control):
        return self.worker_helper.dispatch_raw('.'.join([self.application_name, 'control']), control)

    @inlineCallbacks
    def get_multiwoker(self, config):
        self.worker = yield self.worker_helper.get_worker(
            StubbedVusionMultiWorker, config, start=True)
        #yield self.worker.startWorker()
        #yield self.worker.startService()
        #yield self.worker.setup_worker()
        yield self.worker.wait_for_workers()
        returnValue(self.worker)

    @inlineCallbacks
    def test_add_remove_workers(self):

        yield self.get_multiwoker(self.base_config)

        self.assertEqual(self.collections['workers'].count(), 1)

        control = self.mkmsg_multiworker_control(
            message_type='add_worker',
            worker_name='worker2',
            worker_class= '%s.ToyDialogueWorker' % (__name__,),
            config=self.new_worker_config)
        yield self.dispatch_control(control)

        yield self.worker.wait_for_workers()

        self.assertEqual(self.collections['workers'].count(), 2)
        self.assertTrue('worker2' in self.worker.workers)

        control = self.mkmsg_multiworker_control(
            message_type='add_worker',
            worker_name='worker2',
            worker_class= '%s.ToyDialogueWorker' % (__name__,),
            config=self.new_worker_config)
        yield self.dispatch_control(control)

        #yield self.worker.wait_for_workers()

        self.assertEqual(self.collections['workers'].count(), 2)
        self.assertTrue('worker2' in self.worker.workers)

        control = self.mkmsg_multiworker_control(
            message_type='remove_worker',
            worker_name='worker2')
        yield self.dispatch_control(control)

        #yield self.worker.wait_for_workers()

        self.assertEqual(self.collections['workers'].count(), 1)
        self.assertFalse('worker2' in self.worker.workers)
        
        #yield self.worker.stopService()

    @inlineCallbacks
    def test_startup(self):
        #The worker1 class and config store in the database are overwrite
        #by the config file
        self.collections['workers'].save({
            'name': 'worker1',
            'class': '%s.ToyWorker' % (__name__),
            'config': self.new_worker_config})

        self.collections['workers'].save({
            'name': 'worker2',
            'class': '%s.ToyWorker' % (__name__),
            'config': self.new_worker_config})

        yield self.get_multiwoker(self.base_config)
        yield self.worker.wait_for_workers()

        self.assertEqual(self.collections['workers'].count(), 2)
        worker_configs = self.collections['workers'].find()
        self.assertEqual(worker_configs[0]['class'], '%s.ToyDialogueWorker' % (__name__))
        self.assertEqual(worker_configs[0]['model-version'], '2')
        self.assertEqual(
            worker_configs[1]['class'],
            '%s.ToyWorker' % (__name__))
        self.assertEqual(worker_configs[1]['model-version'], '2')

        self.assertTrue('worker1' in self.worker.workers)
        self.assertTrue(isinstance(self.worker.workers['worker1'],
                                   DialogueWorker))
        self.assertTrue('worker2' in self.worker.workers)
        self.assertTrue(isinstance(self.worker.workers['worker2'],
                                   ToyWorker))
