# -*- test-case-name: tests.test_multiworker

from twisted.internet.defer import (
    Deferred, DeferredList, inlineCallbacks, maybeDeferred)

from pymongo import MongoClient

from copy import deepcopy

from vumi.config import ConfigText
from vumi.worker import BaseWorker
from vumi.service import WorkerCreator
from vumi.message import Message
from vumi import log

from vusion.connectors import ReceiveMultiworkerControlConnector
from vusion.persist import WorkerConfig, WorkerConfigManager
from vusion.message import MultiWorkerControl


class VusionMultiworkerConfig(BaseWorker.CONFIG_CLASS):
    """Base config definition for applications.

    You should subclass this and add application-specific fields.
    """

    application_name = ConfigText(
        "The name this application instance will use to create its queues.",
        required=True, static=True)


class VusionMultiWorker(BaseWorker):

    WORKER_CREATOR = WorkerCreator
    CONFIG_CLASS = VusionMultiworkerConfig
    UNPAUSE_CONNECTORS = True

    def _validate_config(self):
        config = self.get_static_config()
        self.application_name = config.application_name
        self.validate_config()

    def setup_connectors(self):
        d = self.setup_connector(ReceiveMultiworkerControlConnector, self.application_name)

        def cb(connector):
            connector.set_control_handler(self.dispatch_control)
            return connector
        
        return d.addCallback(cb)

    def setup_worker(self):
        d = maybeDeferred(self.setup_application)
        if self.UNPAUSE_CONNECTORS:
            d.addCallback(lambda r: self.unpause_connectors())        
        return d
    
    def setup_application(self):
        log.debug('Starting Multiworker %s' % (self.config,))
        
        self.workers = {}
        self.worker_creator = self.WORKER_CREATOR(self.options)

        mongo_client = MongoClient(
            self.config['mongodb_host'],
            self.config['mongodb_port'],
            w=1)
        db = mongo_client[self.config['vusion_database_name']]
        self.collections = {}
        self.collections['worker_config'] = WorkerConfigManager(db, 'workers')

        self.reload_workers_from_config_file()
        self.reload_workers_from_mongodb()
        
    def teardown_worker(self):
        d = self.pause_connectors()
        d.addCallback(lambda r: self.teardown_application())
        return d
    
    @inlineCallbacks
    def teardown_application(self):
        for worker in self.workers.itervalues():
            #in the unit test the worker.running is at 0 so the worker is not stopped
            yield worker.stopWorker()
            yield worker.stopService()
            worker.disownServiceParent()

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
        worker = self.worker_creator.create_worker(
            worker_config['class'],
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
            self.collections['worker_config'].save_worker_config(worker_config)
            self.add_worker(worker_config)

    def reload_workers_from_mongodb(self):
        for worker_config in self.collections['worker_config'].get_worker_configs():
            self.add_worker(worker_config)

    def add_worker(self, worker_config):
        if worker_config['name'] in self.workers:
            log.error('Cannot create worker, name already exist: %s'
                      % (worker_config['name'],))
            return

        self.collections['worker_config'].save_worker_config(worker_config)
        worker_config['config'] = self.construct_worker_config(worker_config['config'])
        self.workers[worker_config['name']] = self.create_worker(worker_config)

    @inlineCallbacks
    def remove_worker(self, worker_name):
        if not worker_name in self.workers:
            log.error('Cannot remove worker, name unknown: %s' % (worker_name))
            return
        yield self.workers[worker_name].stopWorker()
        yield self.workers[worker_name].stopService()
        self.workers[worker_name].disownServiceParent()
        #TODO needed due to parent class, remove the storage into the config
        #self.config.pop(worker_name)
        self.collections['worker_config'].remove_worker_config(worker_name)
        self.workers.pop(worker_name)

    @inlineCallbacks
    def dispatch_control(self, msg):
        yield self.consume_control(msg)

    @inlineCallbacks
    def consume_control(self, msg):
        log.debug('Received Control %r' % (msg,))
        try:
            if msg['message_type'] == 'add_worker':
                self.add_worker(WorkerConfig(**{
                    'name': msg['worker_name'],
                    'class': msg['worker_class'],
                    'config': msg['config']}))
            if msg['message_type'] == 'remove_worker':
                yield self.remove_worker(msg['worker_name'])
        except Exception as ex:
            log.error("Unexpected error %s" % repr(ex))
