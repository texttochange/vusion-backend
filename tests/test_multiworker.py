from twisted.trial.unittest import TestCase
from twisted.internet.defer import (Deferred, DeferredList,
                                    inlineCallbacks, returnValue)

from vumi.multiworker import MultiWorker
from vumi.tests.utils import StubbedWorkerCreator, get_stubbed_worker
from vumi.tests.test_multiworker import ToyWorker

from vusion import VusionMultiWorker, TtcGenericWorker
from tests.utils import MessageMaker


class StubbedVusionMultiWorker(VusionMultiWorker):

    def WORKER_CREATOR(self, options):
        worker_creator = StubbedWorkerCreator(options)
        worker_creator.broker = self._amqp_client.broker
        return worker_creator

    def wait_for_workers(self):
        return DeferredList([w._d for w in self.workers.values()])


class VusionMultiWorkerTestCase(TestCase, MessageMaker):
    timeout = 3

    base_config = {
        'application_name': 'vusion',
        'workers': {
            'worker1': 'vusion.TtcGenericWorker'
            },
        'worker1': {
            'control_name': 'test',
            'transport_name': 'test',
            'dispatcher_name': 'dispatcher',
            'database_name': 'test',
            'vusion_database_name': 'test2'
            }
        }

    def setUp(self):
        pass

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopService()

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
    def test_start_stop_workers(self):
        new_worker_config = {
            'control_name': 'test2',
            'transport_name': 'test2',
            'dispatcher_name': 'dispatcher2',
            'database_name': 'test2',
            'vusion_database_name': 'test3'
        }

        yield self.get_multiwoker(self.base_config)

        yield self.send_control(
            'vusion',
            self.mkmsg_multiworker_control(
                message_type='add_worker',
                worker_name='worker2',
                worker_class='vusion.TtcGenericWorker',
                config=new_worker_config))
        
        yield self.worker.wait_for_workers()

        self.assertTrue('worker2' in self.worker.workers)

        yield self.send_control(
            'vusion',
            self.mkmsg_multiworker_control(
                message_type='remove_worker',
                worker_name='worker2'))

        yield self.worker.wait_for_workers()

        self.assertFalse('worker2' in self.worker.workers)

        yield self.worker.stopService()
