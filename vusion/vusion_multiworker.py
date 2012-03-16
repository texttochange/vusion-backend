from twisted.internet.defer import (Deferred, DeferredList)

from vumi.multiworker import MultiWorker
from vumi.tests.utils import StubbedWorkerCreator


class VusionMultiWorker(MultiWorker):
    OLIVE = True
