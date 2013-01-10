from vumi.middleware import BaseMiddleware
from vumi.log import log

class TrimingMiddleware(BaseMiddleware):
    
    default_trim = ' \t\n'
    
    def setup_middleware(self):
        self.extra_trim = self.config.get('extra_trim', '')
        self.trim = self.default_trim + (self.extra_trim or '')
    
    def handle_inbound(self, msg, endpoint):        
        msg['content'] = (msg['content'] or '').strip(self.trim)
        return msg
