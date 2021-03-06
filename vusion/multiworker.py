# -*- test-case-name: tests.test_multiworker
import MySQLdb
from pymongo import MongoClient

from copy import deepcopy

from twisted.internet.defer import (
    Deferred, DeferredList, inlineCallbacks, maybeDeferred)

from vumi.config import ConfigText, ConfigInt
from vumi.worker import BaseWorker
from vumi.service import WorkerCreator
from vumi.message import Message
from vumi import log

from vusion.connectors import ReceiveMultiworkerControlConnector
from vusion.persist import WorkerConfig, WorkerConfigManager, ProgramManager
from vusion.message import MultiWorkerControl


class VusionMultiworkerConfig(BaseWorker.CONFIG_CLASS):
    """Base config definition for applications.

    You should subclass this and add application-specific fields.
    """
    application_name = ConfigText(
        "The name this application instance will use to create its queues.",
        required=True, static=True)

    vusion_database_name = ConfigText(
        "The database name on both mysql and mongodb engine.",
        required=True, static=True)
    mongodb_host = ConfigText(
        "The host of the mongodb instance.",
        required=True, static=True)
    mongodb_port = ConfigInt(
        "The port of the mongodb instance.",
        required=True, static=True)

    mysql_host = ConfigText(
        "The host of the mysql instance.",
        required=True, static=True)
    mysql_port = ConfigInt(
        "The port of the mysql instance.",
        required=True, static=True)
    mysql_user = ConfigText(
        "The user of the mysql instance.",
        required=True, static=True)
    mysql_password = ConfigText(
        "The password of the mysql instance.",
        required=True, static=True)
    mysql_db = ConfigText(
        "The db of the mysql instance.",
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
        config = self.get_static_config()
        log.debug('Starting Multiworker %s' % (config,))
        
        self.workers = {}
        self.worker_creator = self.WORKER_CREATOR(self.options)

        self.mongo_client = MongoClient(
            config.mongodb_host,
            config.mongodb_port,
            w=1)
        mongo_db = self.mongo_client[config.vusion_database_name]
        self.collections = {}
        self.collections['worker_config'] = WorkerConfigManager(mongo_db, 'workers')

        self.mysql_db = MySQLdb.connect(
            host=config.mysql_host,
            port=config.mysql_port,
            user=config.mysql_user,
            passwd=config.mysql_password,
            db=config.mysql_db)
        self.collections['worker'] = ProgramManager(self.mysql_db)

        self.reload_workers_from_config_file()
        self.reload_workers_from_db()

    def teardown_worker(self):
        d = self.pause_connectors()
        d.addCallback(lambda r: self.teardown_application())
        return d

    @inlineCallbacks
    def teardown_application(self):
        for worker in self.workers.itervalues():
            #in the unit test the worker.running is at 0 so the worker is not stopped
            if "before_teardown_application" in dir(worker):
                worker.before_teardown_application()
            yield worker.stopWorker()
            worker.disownServiceParent()
        for manager in self.collections.itervalues():
            manager.close_connection()

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

    def reload_workers_from_db(self):
        for program in self.collections['worker'].get_running():
            worker_config = self.collections['worker_config'].get_worker_config_from_url(program['url'])
            if worker_config is None:
                log.error("ERROR SKIP START of %r worker as config is missing" % program)
                continue
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
        if "before_teardown_application" in dir(self.workers[worker_name]):
            self.workers[worker_name].before_teardown_application()
        yield self.workers[worker_name].stopWorker()
        self.workers[worker_name].disownServiceParent()
        #TODO needed due to parent class, remove the storage into the config
        #self.config.pop(worker_name)
        self.collections['worker_config'].remove_worker_config(worker_name)
        self.workers.pop(worker_name)
        log.msg('Worker has been removed %s' % worker_name)

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
