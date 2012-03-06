from twisted.trial.unittest import TestCase
from twisted.internet.defer import (Deferred, DeferredList, 
                                    inlineCallbacks, returnValue)

from vumi.multiworker import MultiWorker
from vumi.tests.utils import StubbedWorkerCreator, get_stubbed_worker
from vusion import VusionMultiWorker, TtcGenericWorker


class StubbedVusionMultiWorker(VusionMultiWorker):
   def WORKER_CREATOR(self, options):
      worker_creator = StubbedWorkerCreator(options)
      worker_creator.broker = self._amqp_client.broker
      return worker_creator

   def wait_for_workers(self):
      return DeferredList([w._d for w in self.workers]) 

class VusionMultiWorkerTestCase(TestCase):
    timeout = 3
    
    base_config = {
        'workers': {
            'worker1' : 'vusion.TtcGenericWorker'
            },
        'worker1':{
            'control_name' : 'test',
            'transport_name' : 'test',
            }
        }
    
    def setUp(self):
        #TtcGenericWorker.events[:] = []
        pass
        
    
    #@inlineCallbacks
    def tearDown(self):
        yield self.worker.stopService()
        #TtcGenericWorker.events[:] = []
        
    @inlineCallbacks
    def get_multiwoker(self, config):
        self.worker = get_stubbed_worker(StubbedVusionMultiWorker, config)
        self.worker.startService()
        self.broker = self.worker._amqp_client.broker
        yield self.worker.wait_for_workers()
        returnValue(self.worker)
    
    @inlineCallbacks
    def test_start_stop_worker(self):
        worker = yield self.get_multiwoker(self.base_config)
        self.assertTrue(True)
        