from twisted.internet.defer import (Deferred, DeferredList)

from vumi.multiworker import MultiWorker
from vumi.tests.utils import StubbedWorkerCreator

class VusionMultiWorker(MultiWorker):
    def WORKER_CREATOR(self, options):
        worker_creator = StubbedWorkerCreator(options)
        worker_creator.broker = self._amqp_client.broker
        return worker_creator

    def wait_for_workers(self):
        return DeferredList([w._d for w in self.workers])  