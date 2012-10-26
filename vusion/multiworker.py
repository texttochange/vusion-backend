# -*- test-case-name: tests.test_multiworker

from twisted.internet.defer import Deferred, DeferredList, inlineCallbacks

import pymongo

from copy import deepcopy

from vumi.service import Worker, WorkerCreator
from vumi.message import Message
from vumi import log

from vusion.utils import DataLayerUtils
from vusion.persist import WorkerConfig


class VusionMultiWorker(Worker, DataLayerUtils):

    WORKER_CREATOR = WorkerCreator

    def startService(self):
        log.debug('Starting Multiworker %s' % (self.config,))
        super(VusionMultiWorker, self).startService()
        DataLayerUtils.__init__(self)

        self.workers = {}
        self.worker_creator = self.WORKER_CREATOR(self.options)

        connection = pymongo.Connection(self.config['mongodb_host'],
                                        self.config['mongodb_port'])
        self.db = connection[self.config['vusion_database_name']]
        self.setup_collection('workers')

    @inlineCallbacks
    def startWorker(self):
        self.reload_workers_from_config_file()
        self.reload_workers_from_mongodb()
        yield self.setup_control()

    def construct_worker_config(self, worker_config={}):
        """
        Construct an appropriate configuration for the child worker.
        """
        config = deepcopy(self.config.get('defaults', {}))
        config.update(worker_config)
        return config

    def create_worker(self, worker_config):
        """
        Create a child worker.
        """
        worker = self.worker_creator.create_worker(worker_config['class'],
                                                   worker_config['config'])
        worker.setName(worker_config['name'])
        worker.setServiceParent(self)
        return worker

    def reload_workers_from_config_file(self):
        for wname, wclass in self.config.get('workers', {}).items():
            worker_config = WorkerConfig()
            worker_config['name'] = wname
            worker_config['class'] = wclass
            worker_config['config'] = self.config.get(wname, {})
            self.save_worker_config(worker_config)
            self.add_worker(worker_config)

    def reload_workers_from_mongodb(self):
        worker_configs = self.collections['workers'].find()
        for worker_config_raw in worker_configs:
            worker_config = WorkerConfig(**worker_config_raw)
            self.add_worker(worker_config)

    def save_worker_config(self, worker_config):
        if worker_config.is_already_saved():
            return self.collections['workers'].save(worker_config.get_as_dict())
        # need a update in case it's a overwriting from the config file
        return self.collections['workers'].update(
            {'name': worker_config['name']},
            {'$set': worker_config.get_as_dict()},
            True)

    @inlineCallbacks
    def setup_control(self):
        self.control = yield self.consume(
            '%s.control' % (self.config['application_name'],),
            self.receive_control_message,
            message_class=Message)

    def add_worker(self, worker_config):
        if worker_config['name'] in self.workers:
            log.error('Cannot create worker, name already exist: %s'
                      % (worker_config['name'],))
            return

        self.save_worker_config(worker_config)
        worker_config['config'] = self.construct_worker_config(worker_config['config'])
        self.workers[worker_config['name']] = self.create_worker(worker_config)

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
                self.add_worker(WorkerConfig(**{
                    'name': msg['worker_name'],
                    'class': msg['worker_class'],
                    'config': msg['config']}))
            if msg['message_type'] == 'remove_worker':
                self.remove_worker(msg['worker_name'])
        except Exception as ex:
            log.error("Control received: %s" % (msg))
            log.error("Unexpected error %s" % repr(ex))
