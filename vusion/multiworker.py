# -*- test-case-name: tests.test_multiworker

from twisted.internet.defer import Deferred, DeferredList, inlineCallbacks

from vumi.multiworker import MultiWorker
from vumi.message import Message
from vumi import log


class VusionMultiWorker(MultiWorker):

    def startService(self):
        log.debug('Starting Multiworker %s' % (self.config,))
        super(MultiWorker, self).startService()

        self.workers = {}
        self.worker_creator = self.WORKER_CREATOR(self.options)
        for wname, wclass in self.config.get('workers', {}).items():
            worker = self.create_worker(wname, wclass)
            self.workers['wname'] = worker

    @inlineCallbacks
    def startWorker(self):
        yield self.setup_control()

    @inlineCallbacks
    def setup_control(self):
        self.control = yield self.consume(
        '%s.control' % (self.config['application_name'],),
        self.receive_control_message,
        message_class=Message)

    @inlineCallbacks
    def receive_control_message(self, msg):
        log.debug('Received control! %s' % (msg,))

        try:
            if msg['message_type'] == 'add_worker':
                if msg['worker_name'] in self.workers:
                    log.error('Cannot create worker, name already exist: %s'
                              % (msg['worker_name'],))
                    return
                for key in msg['config'].keys():
                    msg['config'][key] = msg['config'][key].encode('utf-8')
                self.config[msg['worker_name']] = msg['config']
                worker = self.create_worker(msg['worker_name'],
                                            msg['worker_class'])
                self.workers[msg['worker_name']] = worker

            if msg['message_type'] == 'remove_worker':
                if not msg['worker_name'] in self.workers:
                    log.error('Cannot remove worker, name unknown: %s'
                              % (msg['worker_name']))
                    return
                yield self.workers[msg['worker_name']].stopService()
                self.workers[msg['worker_name']].disownServiceParent()
                self.workers.pop(msg['worker_name'])

        except Exception as ex:
            log.error("Control received: %s" % (msg))
            log.error("Unexpected error %s" % repr(ex))
