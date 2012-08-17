# -*- test-case-name: tests.test_multiworker

from twisted.internet.defer import Deferred, DeferredList, inlineCallbacks

import pymongo

from vumi.multiworker import MultiWorker
from vumi.message import Message
from vumi import log

from vusion.utils import DataLayerUtils

class VusionMultiWorker(MultiWorker, DataLayerUtils):

    def startService(self):
        log.debug('Starting Multiworker %s' % (self.config,))
        super(MultiWorker, self).startService()
        self.workers = {}
        self.worker_creator = self.WORKER_CREATOR(self.options)

    @inlineCallbacks
    def startWorker(self):
        #connection = pymongo.Connection('localhost', 27017)
        #self.db = connection[self.config['database_name']]
        #self.setup_collection('workers')
        for wname, wclass in self.config.get('workers', {}).items():
            self.add_worker(wname, wclass, self.config[wname])
        yield self.setup_control()

    @inlineCallbacks
    def setup_control(self):
        self.control = yield self.consume(
            '%s.control' % (self.config['application_name'],),
            self.receive_control_message,
            message_class=Message)

    def add_worker(self, worker_name, worker_class, worker_config):
        if worker_name in self.workers:
            log.error('Cannot create worker, already exist: %s' % (worker_name,))
            return
        for key in worker_config.keys():
            worker_config[key] = worker_config[key].encode('utf-8')
        self.config[worker_name] = worker_config
        self.workers[worker_name] = self.create_worker(worker_name,worker_class)

    @inlineCallbacks
    def remove_worker(self, worker_name):
        if not worker_name in self.workers:
            log.error('Cannot remove worker, name unknown: %s' % (worker_name))
            return
        yield self.workers[worker_name].stopService()
        self.workers[worker_name].disownServiceParent()
        self.workers.pop(worker_name)

    def receive_control_message(self, msg):
        log.debug('Received control! %s' % (msg,))

        try:
            if msg['message_type'] == 'add_worker':
                self.add_worker(msg['worker_name'], msg['worker_class'], msg['config'])
            if msg['message_type'] == 'remove_worker':
                self.remove_worker(msg['worker_name'])
                
        except Exception as ex:
            log.error("Control received: %s" % (msg))
            log.error("Unexpected error %s" % repr(ex))
