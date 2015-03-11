import re

from vumi.middleware import BaseMiddleware
from vumi.log import log

from vusion.utils import get_shortcode_value

class VusionAddressMiddleware(BaseMiddleware):
    
    regex_plus = re.compile("^\+")
    regex_zeros = re.compile("^00")
    regex_trim = re.compile("\s")
    
    def setup_middleware(self):
        self.international_prefix = self.config.get('international_prefix', False)
        self.trim_plus_outbound = self.config.get('trim_plus_outbound', False)
        self.ensure_plus_inbound = self.config.get('ensure_plus_inbound', False)
        self.trim_international_prefix_outbound = self.config.get('trim_international_prefix_outbound', False)
        if self.international_prefix is not None:
            self.regex_internation_prefix = re.compile(("^\+?%s" % self.international_prefix))
        self.overwrite_from_addr = self.config.get('overwrite_from_addr', False)
    
    def handle_inbound(self, msg, endpoint):
        ##from_address modification
        msg['from_addr'] = re.sub(self.regex_trim, "", msg['from_addr'])
        msg['from_addr'] = re.sub(self.regex_zeros, "", msg['from_addr'])
        if (not re.match(self.regex_plus, msg['from_addr'])):
            msg['from_addr'] = '+%s' % msg['from_addr']
        if self.international_prefix and not re.match(self.regex_internation_prefix, msg['from_addr']):
            msg['from_addr'] = re.sub(self.regex_plus, '', msg['from_addr'])
            msg['from_addr'] = '+%s%s' % (self.international_prefix, msg['from_addr'])
        ##to_addr modification
        msg['to_addr'] = re.sub(self.regex_trim, "", msg['to_addr'])
        msg['to_addr'] = re.sub(self.regex_plus, "", msg['to_addr'])
        if (re.match(self.regex_zeros, msg['to_addr'])):
            msg['to_addr'] = re.sub(self.regex_zeros, "", msg['to_addr'])
        if self.ensure_plus_inbound:
            msg['to_addr'] = '+%s' % msg['to_addr']
        return msg

    def handle_outbound(self, msg, endpoint):
        ##from_addr modification
        msg['from_addr'] = get_shortcode_value(msg['from_addr'])
        if self.overwrite_from_addr is not False:
            msg['from_addr'] = self.overwrite_from_addr
        ##to_addr modification
        if self.trim_international_prefix_outbound:
            msg['to_addr'] = re.sub(self.regex_internation_prefix, '', msg['to_addr'])        
        if self.trim_plus_outbound:
            msg['to_addr'] = re.sub(self.regex_plus, '', msg['to_addr'])
        if 'customized_id' in msg['transport_metadata']:
            msg['from_addr'] = msg['transport_metadata']['customized_id']
        return msg
