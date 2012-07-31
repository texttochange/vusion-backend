# -*- test-case-name: tests.test_ttc -*-

from twisted.internet.defer import Deferred, inlineCallbacks

import pymongo

from vumi.application import ApplicationWorker
from vumi.message import TransportUserMessage
from vumi import log

from vusion.utils import time_to_vusion_format


class GarbageWorker(ApplicationWorker):

    def startWorker(self):
        log.msg("Garbage Worker is starting")
        super(GarbageWorker, self).startWorker()

        connection = pymongo.Connection('localhost', 27017)
        db = connection[self.config['database_name']]
        if not 'unmatchable_reply' in db.collection_names():
            db.create_collection('unmatchable_reply')
        self.unmatchable_reply_collection = db['unmatchable_reply']

    def consume_user_message(self, msg):
        log.debug("Consumer user message %s" % (msg,))
        if msg['timestamp']:
            timestamp = time_to_vusion_format(msg['timestamp'])
        self.unmatchable_reply_collection.save(
            {'participant-phone': msg['from_addr'],
             'to': msg['to_addr'],
             'message-content': msg['content'],
             'timestamp': timestamp,
             })
        error_message_from_template = TransportUserMessage(**{
                'from_addr': '8282',
                'to_addr': '0712747841',
                'transport_name': None,
                'transport_type': None,
                'transport_metadata': None,
                'content': 'This message does not match any keywords'})
        self.transport_publisher.publish_message(error_message_from_template)

    def dispatch_event(self, msg):
        pass

