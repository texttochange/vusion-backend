#from twisted.internet.defer import inlineCallbacks, returnValue
from vumi.middleware import BaseMiddleware
from vumi.log import log

class TrimingMiddleware(BaseMiddleware):
    
    def handle_inbound(self, msg, endpoint):
        msg['content'] = (msg['content'] or '').strip()
        return msg
