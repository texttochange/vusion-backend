
from datetime import datetime

#from vumi.tests.utils import UTCNearNow
from vumi.message import (TransportEvent, TransportMessage,
                          TransportUserMessage, Message)
from vumi.transports.failures import FailureMessage


class DataLayerUtils:

    def setup_collections(self, names):
        for name in names:
            self.setup_collection(name)

    def setup_collection(self, name):
        if name in self.db.collection_names():
            self.collections[name] = self.db[name]
        else:
            self.collections[name] = self.db.create_collection(name)

    def drop_collections(self):
        for name, collection in self.collections.items():
            collection.drop()


#TODO use UTC time rather than datetime
class MessageMaker:

    def mkmsg_ack(self, user_message_id='1', sent_message_id='abc',
                  transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
            return TransportEvent(
                event_id=RegexMatcher(r'^[0-9a-fA-F]{32}$'),
                event_type='ack',
                user_message_id=user_message_id,
                sent_message_id=sent_message_id,
                timestamp=datetime.now(),
                transport_name=self.transport_name,
                transport_metadata=transport_metadata,
            )

    def mkmsg_delivery(self, status='delivered', user_message_id='abc',
                       transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
        return TransportEvent(
            event_id=RegexMatcher(r'^[0-9a-fA-F]{32}$'),
            event_type='delivery_report',
            transport_name=self.transport_name,
            user_message_id=user_message_id,
            delivery_status=status,
            to_addr='+41791234567',
            timestamp=datetime.now(),
            transport_metadata=transport_metadata,
            )

    def mkmsg_in(self, content='hello world',
                 session_event=TransportUserMessage.SESSION_NONE,
                 message_id='abc', transport_type=None,
                 transport_metadata=None, transport_name=None,
                 timestamp=None):
        if timestamp is None:
            timestamp = datetime.now()
        if transport_type is None:
            transport_type = transport_type
        if transport_metadata is None:
            transport_metadata = {}
        return TransportUserMessage(
            from_addr='+41791234567',
            to_addr='9292',
            group=None,
            message_id=message_id,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            timestamp=timestamp,
            )

    def mkmsg_out(self, content='hello world',
                  session_event=TransportUserMessage.SESSION_NONE,
                  message_id='1', to_addr='+41791234567', from_addr='9292',
                  group=None, in_reply_to=None, transport_type=None,
                  transport_metadata=None, helper_metadata=None,
                  transport_name=None):
        if transport_type is None:
            transport_type = transport_type
        if transport_metadata is None:
            transport_metadata = {}
        if helper_metadata is None:
            helper_metadata = {}
        params = dict(
            to_addr=to_addr,
            from_addr=from_addr,
            group=group,
            message_id=message_id,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            in_reply_to=in_reply_to,
            helper_metadata=helper_metadata,
            )
        return TransportUserMessage(**params)

    def mkmsg_fail(self, message, reason,
                   failure_code=FailureMessage.FC_UNSPECIFIED):
        return FailureMessage(
            timestamp=datetime.now(),
            failure_code=failure_code,
            message=message,
            reason=reason,
            )

    def mkmsg_dispatcher_control(self, message_type='add_exposed',
                                 exposed_name='app2', keyword_mappings=None):
        if keyword_mappings == None:
            keyword_mappings = []
        return Message(
            message_type=message_type,
            exposed_name=exposed_name,
            keyword_mappings=keyword_mappings)

    def mkmsg_multiworker_control(self, message_type='add_worker',
                                  worker_name='m4h', worker_class=None,
                                  config=None):
        if config == None:
            config = []
        return Message(
            message_type=message_type,
            worker_name=worker_name,
            worker_class=worker_class,
            config=config)
    
    def mkmsg_dialogueworker_control(self, action, dialogue_obj_id,
                                     phone_number=None):
        return Message(
            action=action,
            dialogue_obj_id=dialogue_obj_id,
            phone_number=phone_number)
