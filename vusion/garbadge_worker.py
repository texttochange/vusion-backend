# -*- test-case-name: tests.test_ttc -*-
import re
import sys
import traceback

from twisted.internet.defer import Deferred, inlineCallbacks

from pymongo import MongoClient
from bson.objectid import ObjectId

from vumi.application import ApplicationWorker
from vumi.message import TransportUserMessage
from vumi import log
from vumi.utils import get_first_word

from vusion.utils import (time_to_vusion_format, get_shortcode_value,
                          get_shortcode_address)
from vusion.persist import (TemplateManager, ShortcodeManager, 
                            UnmatchableReplyManager, UnmatchableReply,
                            GarbageCreditLogManager)
from vusion.component import BasicLogger


class GarbageWorker(ApplicationWorker):

    regex_KEYWORD = re.compile('KEYWORD')

    def setup_application(self):
        log.msg("Garbage Worker is starting with %r"% self.config)

        mongo_client = MongoClient(
            self.config['mongodb_host'],
            self.config['mongodb_port'],
            safe=self.config.get('mongodb_safe', False))
        if self.config.get('mongodb_safe', False):
            mongo_client.write_concern['j'] = True
        db = mongo_client[self.config['database_name']]
        self.collections = {}
        self.collections['unmatchable_reply'] = UnmatchableReplyManager(
            db, 'unmatchable_reply')
        self.collections['shortcode'] = ShortcodeManager(db, 'shortcodes')
        self.collections['template'] = TemplateManager(db, 'templates')
        self.collections['credit_log'] = GarbageCreditLogManager(db, 'credit_logs')

        self.logger = BasicLogger()        
        for manager in self.collections.itervalues():
            manager.set_log_helper(self.logger)

    def teardown_application(self):
        pass

    @inlineCallbacks
    def consume_user_message(self, msg):
        self.logger.log("Consumer user message %s" % (msg,), 'debug')        
        try:
            unmatchable_reply = UnmatchableReply(**{
                'participant-phone': msg['from_addr'],
                'to': msg['to_addr'],
                'direction': 'incoming',
                'message-content': msg['content'],
                'timestamp': msg['timestamp']})
            self.collections['unmatchable_reply'].save_document(unmatchable_reply)

            matching_code = self.collections['shortcode'].get_shortcode(
                msg['to_addr'], msg['from_addr'])
            if matching_code is None:
                self.logger.log("Incoming message not matching any shorcode to %s from %s" % (msg['to_addr'], msg['from_addr']), 'err')
                return
            self.collections['credit_log'].increment_incoming(
                matching_code.get_message_credits(msg['content']),
                code=matching_code.get_vusion_reference())

            template = self.collections['template'].find_one({
                '_id': ObjectId(matching_code['error-template'])})
            if template is None:
                return

            response_to_addr = msg['from_addr']
            response_content = re.sub(
                    self.regex_KEYWORD, 
                    get_first_word(msg['content']),
                    template['template'])
            response_options = {
                'from_addr': get_shortcode_address(matching_code),
                'transport_type': msg['transport_type'],
                'transport_metadata': msg['transport_metadata']}

            yield self.send_to(response_to_addr, response_content, **response_options)
            response = UnmatchableReply(**{
                'participant-phone': response_options['from_addr'],
                'to': response_to_addr,
                'direction': 'outgoing',
                'message-content': response_content,
                'timestamp': msg['timestamp']})
            self.collections['unmatchable_reply'].save_document(response)
            self.collections['credit_log'].increment_outgoing(
                matching_code.get_message_credits(response_content),
                code=matching_code.get_vusion_reference())
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.log(
                "Error during consume user message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback),
                'err')

    def dispatch_event(self, msg):
        pass
