# -*- test-case-name: tests.test_multiworker

from twisted.internet.defer import Deferred, DeferredList, inlineCallbacks

import pymongo

from copy import deepcopy

from vumi.service import Worker, WorkerCreator
from vumi.message import Message
from vumi import log

from vusion.utils import DataLayerUtils

class VusionMultiWorker(Worker, DataLayerUtils):

    WORKER_CREATOR = WorkerCreator

    def startService(self):
        log.debug('Starting Multiworker %s' % (self.config,))
        #Jump the constructor of Multiworker to use a dictionary of workers rather than a list
        super(VusionMultiWorker, self).startService()
        DataLayerUtils.__init__(self)
        
        self.workers = {}
        self.worker_creator = self.WORKER_CREATOR(self.options)

        connection = pymongo.Connection('localhost', 27017)
        self.db = connection[self.config['vusion_database_name']]
        self.setup_collection('workers')
    
    @inlineCallbacks
    def startWorker(self):
        self.reload_workers_from_config_file()
        #Start workers defined in the database
        workers = self.collections['workers'].find()
        for worker in workers:
            self.add_worker(worker['name'], worker['class'], worker['config'])
        yield self.setup_control()

    def construct_worker_config(self, worker_name):
        """
        Construct an appropriate configuration for the child worker.
        """
        config = deepcopy(self.config.get('defaults', {}))
        config.update(self.config.get(worker_name, {}))
        return config

    def create_worker(self, worker_name, worker_class, worker_config):
        """
        Create a child worker.
        """
        worker = self.worker_creator.create_worker(worker_class, worker_config)
        worker.setName(worker_name)
        worker.setServiceParent(self)
        return worker

    def reload_workers_from_config_file(self):
        for wname, wclass in self.config.get('workers', {}).items():\
            self.save_worker(wname, wclass, self.construct_worker_config(wname))
    
    def save_worker(self, worker_name, worker_class, worker_config):
        return  self.collections['workers'].update(
            {'name': worker_name},
            {'$set': {'class': worker_class,'config': worker_config}},
            True)

    @inlineCallbacks
    def setup_control(self):
        self.control = yield self.consume(
            '%s.control' % (self.config['application_name'],),
            self.receive_control_message,
            message_class=Message)

    def add_worker(self, worker_name, worker_class, worker_config):
        if worker_name in self.workers:
            log.error('Cannot create worker, name already exist: %s' % (worker_name,))
            return
        #Must make sure to provide utf-8 parameters and not unicode as Mongodb and Rabbitmq are providing
        #TODO: manage this encoding conversion at another level
        for key in worker_config.keys():
            worker_config[key] = worker_config[key].encode('utf-8')
        worker_name = worker_name.encode('utf-8')
        worker_class = worker_class.encode('utf-8')

        self.save_worker(worker_name, worker_class, worker_config)
        self.workers[worker_name] = self.create_worker(worker_name,worker_class,worker_config)

    @inlineCallbacks
    def remove_worker(self, worker_name):
        if not worker_name in self.workers:
            log.error('Cannot remove worker, name unknown: %s' % (worker_name))
            return
        yield self.workers[worker_name].stopService()
        self.workers[worker_name].disownServiceParent()
        #TODO needed due to parent class, remove the storage into the config
        #self.config.pop(worker_name)
        self.collections['workers'].remove({'name': worker_name})
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
