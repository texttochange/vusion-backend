from twisted.internet.defer import inlineCallbacks

from vumi.transports.smpp import SmppTransceiverTransport


class SmsghSmppTransport(SmppTransceiverTransport):

    @inlineCallbacks
    def handle_outbound_message(self, message):
        if 'customized_id' in message['transport_metadata']:
            message['from_addr'] = message['transport_metadata']['customized_id']
        yield super(SmsghSmppTransport, self).handle_outbound_message(message)
