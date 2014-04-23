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

from vusion.utils import (time_to_vusion_format, get_shortcode_value,
                          get_shortcode_address)
from vusion.persist import (TemplateManager, ShortcodeManager, 
                            UnmatchableReplyManager, UnmatchableReply,
                            CreditLogManager)


class GarbageWorker(ApplicationWorker):

    regex_KEYWORD = re.compile('KEYWORD')

    def startWorker(self):
        log.msg("Garbage Worker is starting")
        super(GarbageWorker, self).startWorker()

        connection = pymongo.Connection(self.config['mongodb_host'],
                                        self.config['mongodb_port'],
                                        safe=self.config.get('mongodb_safe', False))        
        db = connection[self.config['database_name']]
        self.collections = {}
        self.collections['unmatchable_reply'] = UnmatchableReplyManager(
            db, 'unmatchable_reply')
        self.collections['shortcode'] = ShortcodeManager(db, 'shortcodes')
        self.collections['template'] = TemplateManager(db, 'templates')
        self.collections['credit_log'] = CreditLogManager(db, 'credit_logs', self.transport_name)

        self.log_manager = Logger()        
        for manager in self.collections.itervalues():
            manager.set_log_helper(self.log_manager)

    @inlineCallbacks
    def consume_user_message(self, msg):
        self.log_manager.log("Consumer user message %s" % (msg,), 'debug')        
        try:
            unmatchable_reply = UnmatchableReply(**{
                'participant-phone': msg['from_addr'],
                'to': msg['to_addr'],
                'direction': 'incoming',
                'message-content': msg['content'],
                'timestamp': time_to_vusion_format(msg['timestamp'])})
            self.collections['unmatchable_reply'].save_document(unmatchable_reply)

            matching_code = self.collections['shortcode'].get_shortcode(
                msg['to_addr'], msg['from_addr'])
            if matching_code is None:
                self.log_manager.log("Incoming message not matching any shorcode to %s from %s" % (msg['to_addr'], msg['from_addr']), 'err')
                return
            self.collections['credit_log'].increment_incoming(
                matching_code.get_message_credits(msg['content']),
                msg['timestamp'],
                matching_code.get_vusion_reference())

            template = self.collections['template'].find_one({
                '_id': ObjectId(matching_code['error-template'])})
            if template is None:
                return

            error_message = TransportUserMessage(**{
                'from_addr': get_shortcode_address(matching_code),
                'to_addr': msg['from_addr'],
                'transport_name': msg['transport_name'],
                'transport_type': msg['transport_type'],
                'transport_metadata': msg['transport_metadata'],
                'content': re.sub(
                    self.regex_KEYWORD, 
                    get_first_word(msg['content']),
                    template['template'])})

            yield self.transport_publisher.publish_message(error_message)
            response = UnmatchableReply(**{
                'participant-phone': error_message['from_addr'],
                'to': error_message['to_addr'],
                'direction': 'outgoing',
                'message-content': error_message['content'],
                'timestamp': time_to_vusion_format(msg['timestamp'])})
            self.collections['unmatchable_reply'].save_document(response)
            self.collections['credit_log'].increment_outgoing(
                matching_code.get_message_credits(error_message['content']),
                            msg['timestamp'],
                            matching_code.get_vusion_reference())
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log_manager.log(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback),
                'err')

    def dispatch_event(self, msg):
        pass


class Logger(object):
    
    def log(self, msg, level='msg'):
        if level == 'err':
            log.err(msg)
        elif level == 'debug':
            log.debug(msg)
        else:
            log.msg(msg)