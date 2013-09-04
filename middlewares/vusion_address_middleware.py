import re

from vumi.middleware import BaseMiddleware
from vumi.log import log

from vusion.utils import get_shortcode_value

class VusionAddressMiddleware(BaseMiddleware):
    
    regex_plus = re.compile("^\+")
    regex_zeros = re.compile("^00")
    regex_trim = re.compile("\s")
    
    def setup_middleware(self):
        self.trim_plus_outbound = self.config.get('trim_plus_outbound', False)
        self.ensure_international_prefix = self.config.get('ensure_international_prefix', False)
        if self.ensure_international_prefix:
            self.regex_internation_prefix = re.compile(("^\+%s" % self.ensure_international_prefix))
    
    def handle_inbound(self, msg, endpoint):
        msg['from_addr'] = re.sub(self.regex_trim, "", msg['from_addr'])
        msg['to_addr'] = re.sub(self.regex_trim, "", msg['to_addr'])        
        msg['from_addr'] = re.sub(self.regex_zeros, "", msg['from_addr'])
        if (not re.match(self.regex_plus, msg['from_addr'])):
            msg['from_addr'] = '+%s' % msg['from_addr']
        if (re.match(self.regex_zeros, msg['to_addr'])):
            msg['to_addr'] = re.sub(self.regex_zeros, "+", msg['to_addr'])
        if self.ensure_international_prefix and not re.match(self.regex_internation_prefix, msg['from_addr']):
            msg['from_addr'] = re.sub(self.regex_plus, '', msg['from_addr'])
            msg['from_addr'] = '+%s%s' % (self.ensure_international_prefix, msg['from_addr'])
        return msg

    def handle_outbound(self, msg, endpoint):
        msg['from_addr'] = get_shortcode_value(msg['from_addr'])
        if self.trim_plus_outbound:
            msg['to_addr'] = re.sub(self.regex_plus, '', msg['to_addr'])
        return msg
