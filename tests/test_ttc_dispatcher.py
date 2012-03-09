

from twisted.trial.unittest import TestCase

from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.utils import FakeRedis

from dispatchers import ContentKeywordRouter


class MessageMakerMixIn(object):
    """TestCase mixin for creating transport messages."""

    def mkmsg_ack(self, transport_name, **kw):
        event_kw = dict(
            event_type='ack',
            user_message_id='1',
            sent_message_id='abc',
            transport_name=transport_name,
            transport_metadata={},
            )
        event_kw.update(kw)
        return TransportEvent(**event_kw)

    def mkmsg_in(self, transport_name, content='foo', **kw):
        msg_kw = dict(
            from_addr='+41791234567',
            to_addr='9292',
            transport_name=transport_name,
            transport_type='sms',
            transport_metadata={},
            content=content,
            )
        msg_kw.update(kw)
        return TransportUserMessage(**msg_kw)

    def mkmsg_out(self, transport_name, content='hello world', **kw):
        msg_kw = dict(
            to_addr='41791234567',
            from_addr='9292',
            transport_name=transport_name,
            transport_type='sms',
            transport_metadata={},
            content=content,
            )
        msg_kw.update(kw)
        return TransportUserMessage(**msg_kw)


class TestContentKeywordRouter(TestCase, MessageMakerMixIn):

    def setUp(self):
        self.config = {
            'transport_names': ['transport1'],
            'exposed_names': ['m4h', 'mrs'],
            'router_class': 'dispatchers.ContentKeywordRouter',
            'keyword_mappings': {
                'm4h': 'BT',
                'mrs': 'LOVE'
                }
            }
        self.dispatcher = DummyDispatcher(self.config)
        self.router = ContentKeywordRouter(self.dispatcher, self.config)
        self.router.r_server = FakeRedis()
        
    def tearDown(self):
        self.router.r_server.teardown()
    
    def test01_dispatch_inbound_message(self):
        msg = self.mkmsg_in(content='BT rest of a msg', transport_name='transport1')
        self.router.dispatch_inbound_message(msg)
        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['m4h'].msgs, [msg])
    
    def test02_dispatch_ack_event_ok(self):
        msg = self.mkmsg_ack(content='LOVE is in the air', transport_name='transport1')
        self.router.r_server.set(msg['user_message_id'],'mrs')
        
        self.router.dispatch_inbound_event(msg)
        
        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['mrs'].msgs, [msg])
    
    def test02_dispatch_ack_event_fail_publisher_not_defined(self):
        msg = self.mkmsg_ack(content='LOVE is in the air', transport_name='transport1')
        
        self.router.dispatch_inbound_event(msg)
        
        publishers = self.dispatcher.exposed_publisher
        self.assertEqual(publishers['mrs'].msgs, [])
        self.assertEqual(publishers['m4h'].msgs, [])
    
        
    def test03_dispatch_outbound_message(self):
        msg = self.mkmsg_out(content="BT rest of msg", transport_name='transport1')
        self.router.dispatch_outbound_message(msg)
        publishers = self.dispatcher.transport_publisher
        self.assertEqual(publishers['transport1'].msgs, [msg])
        
class DummyDispatcher(object):

    class DummyPublisher(object):
        def __init__(self):
            self.msgs = []

        def publish_message(self, msg):
            self.msgs.append(msg)

    def __init__(self, config):
        self.transport_publisher = {}
        for transport in config['transport_names']:
            self.transport_publisher[transport] = self.DummyPublisher()
        self.exposed_publisher = {}
        self.exposed_event_publisher = {}
        for exposed in config['exposed_names']:
            self.exposed_publisher[exposed] = self.DummyPublisher()
            self.exposed_event_publisher[exposed] = self.DummyPublisher()


