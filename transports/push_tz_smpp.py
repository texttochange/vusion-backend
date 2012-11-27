import re
from vumi.transports.smpp import SmppTransport

class PushTzSmppTransport(SmppTransport):
    
    regex_plus = re.compile("^\+")
    
    def handle_outbound_message(self, message):
        message['from_addr'] = message['from_addr'] if 'default_origin' not in self.config else self.config['default_origin']
        super(PushTzSmppTransport, self).handle_outbound_message(message)

    def deliver_sm(self, *args, **kwargs):
        if (not re.match(self.regex_plus, kwargs.get('source_addr'))):
            kwargs.update({'source_addr': ('+%s' % kwargs.get('source_addr'))})        
        super(PushTzSmppTransport, self).deliver_sm(*args, **kwargs)
