# -*- test-case-name: tests.test_ttc -*-

from twisted.internet.defer import Deferred, inlineCallbacks

import pymongo

from vumi.service import Worker
from vumi.message import TransportUserMessage
from vumi import log


class GarbageWorker(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        log.msg("Garbage Worker is starting")

        connection = pymongo.Connection('localhost', 27017)
        self.db = connection['vusion']
        if not 'unmatchable_reply' in self.db.collection_names():
            self.db.create_collection('unmatchable_reply')
        self.unmatchable_reply_collection = self.db['unmatchable_reply']

        self.consumer = yield self.consume(
            'garbage.inbound',
            self.consume_user_message,
            message_class=TransportUserMessage)

    def consume_user_message(self, msg):
        self.save_message({
            'from': msg['from_addr'],
            'to': msg['to_addr'],
            'message': msg['content'],
            'time': msg['timestamp'],
        })
