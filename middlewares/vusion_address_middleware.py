import re

from vumi.middleware import BaseMiddleware
from vumi.log import log

from vusion.utils import get_shortcode_value

class VusionAddressMiddleware(BaseMiddleware):
    
    regex_plus = re.compile("^\+")
    regex_zeros = re.compile("^00")
    
    def handle_inbound(self, msg, endpoint):
        msg['from_addr'] = re.sub(self.regex_zeros, "", msg['from_addr'])
        if (not re.match(self.regex_plus, msg['from_addr'])):
            msg['from_addr'] = '+%s' % msg['from_addr']
        return msg

    def handle_outbound(self, msg, endpoint):
        msg['from_addr'] = get_shortcode_value(msg['from_addr'])
        return msg
