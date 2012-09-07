# -*- test-case-name: tests.test_ttc -*-
import re
import sys
import traceback

from twisted.internet.defer import Deferred, inlineCallbacks

import pymongo
from bson.objectid import ObjectId

from vumi.application import ApplicationWorker
from vumi.message import TransportUserMessage
from vumi import log
from vumi.utils import get_first_word

from vusion.utils import time_to_vusion_format


class GarbageWorker(ApplicationWorker):

    def startWorker(self):
        log.msg("Garbage Worker is starting")
        super(GarbageWorker, self).startWorker()

        connection = pymongo.Connection(self.config['mongodb_host'],
                                        self.config['mongodb_port'])
        db = connection[self.config['database_name']]
        if not 'unmatchable_reply' in db.collection_names():
            db.create_collection('unmatchable_reply')
        self.unmatchable_reply_collection = db['unmatchable_reply']
        #if not 'shortcodes' in db.collection_names():
            #db.create_collection('shortcodes')
        self.shortcodes_collection = db['shortcodes']
        self.templates_collection = db['templates']

    @inlineCallbacks
    def consume_user_message(self, msg):
        try:
            regex_KEYWORD = re.compile('KEYWORD')
            log.debug("Consumer user message %s" % (msg,))
            if msg['timestamp']:
                timestamp = time_to_vusion_format(msg['timestamp'])
            self.unmatchable_reply_collection.save(
                {'participant-phone': msg['from_addr'],
                 'to': msg['to_addr'],
                 'message-content': msg['content'],
                 'timestamp': timestamp,
                 })

            code = self.shortcodes_collection.find_one({
                'shortcode': msg['to_addr']})
            if code is None:
                return
            template = self.templates_collection.find_one({
                '_id': ObjectId(code['error-template'])})
            if template is None:
                return
            error_message = TransportUserMessage(**{
                'from_addr': msg['to_addr'],
                'to_addr': msg['from_addr'],
                'transport_name': msg['transport_name'],
                'transport_type': msg['transport_type'],
                'transport_metadata': msg['transport_metadata'],
                'content': re.sub(
                    regex_KEYWORD, get_first_word(msg['content']),
                    template['template']
                )
            })
            yield self.transport_publisher.publish_message(error_message)
            log.debug("Reply '%s' sent to %s" %
                      (error_message['content'], error_message['to_addr']))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    def dispatch_event(self, msg):
        pass
