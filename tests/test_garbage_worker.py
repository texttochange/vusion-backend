
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

import pymongo

from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker, UTCNearNow

from tests.utils import MessageMaker
from vusion import GarbageWorker


class GarabageWorkerTestCase(TestCase, MessageMaker):

    @inlineCallbacks
    def setUp(self):
        self.config = {
            'database_name': 'test',
            'application_name': 'garbage'
        }

        connection = pymongo.Connection('localhost', 27017)
        self.db = connection[self.config['database_name']]
        self.unmatchable_reply = self.db['unmatchable_reply']
        self.unmatchable_reply.drop()

        self.worker = get_stubbed_worker(GarbageWorker,
                                         config=self.config)
        self.broker = self.worker._amqp_client.broker
        self.worker.startService()
        yield self.worker.startWorker()

    def tearDown(self):
        self.unmatchable_reply.drop()

    @inlineCallbacks
    def send(self, msg):
        self.broker.publish_message(
            'vumi', self.config['application_name'], msg)
        yield self.broker.kick_delivery()

    @inlineCallbacks
    def test_receive_user_message(self):
        msg = self.mkmsg_in()

        yield self.send(msg)

        stored = self.unmatchable_reply.find_one()

        self.assertTrue(stored)
