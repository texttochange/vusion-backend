#from twisted.internet.defer import inlineCallbacks, returnValue
from vumi.middleware import BaseMiddleware
from vumi.log import log

class TrimingMiddleware(BaseMiddleware):
    
    def handle_inbound(self, msg, endpoint):
        log.msg("Triming inbound message %s" % msg['content'])
        msg['content'] = msg['content'].strip()
        return msg
