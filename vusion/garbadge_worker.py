# -*- test-case-name: tests.test_ttc -*-

from twisted.internet.defer import Deferred, inlineCallbacks

import pymongo

from vumi.service import Worker
from vumi.message import TransportUserMessage
from vumi import log

from vusion.utils import time_to_vusion_format


class GarbageWorker(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Garbage Worker is starting")

        connection = pymongo.Connection('localhost', 27017)
        self.db = connection[self.config['database_name']]
        if not 'unmatchable_reply' in self.db.collection_names():
            self.db.create_collection('unmatchable_reply')
        self.unmatchable_reply_collection = self.db['unmatchable_reply']

        self.consumer = yield self.consume(
            self.config['application_name'],
            self.consume_user_message,
            message_class=TransportUserMessage)

    def consume_user_message(self, msg):
        log.debug("Consumer user message %s" % (msg,))
        if msg['timestamp']:
            timestamp = time_to_vusion_format(msg['timestamp'])
        self.unmatchable_reply_collection.save({
            'participant-phone': msg['from_addr'],
            'to': msg['to_addr'],
            'message-content': msg['content'],
            'timestamp': timestamp,
        })
