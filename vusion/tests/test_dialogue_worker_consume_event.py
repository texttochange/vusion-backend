from datetime import datetime, time, date, timedelta

from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.tests.test_dialogue_worker import DialogueWorkerTestCase


class DialogueWorkerTestCase_consumeEvent(DialogueWorkerTestCase):

    @inlineCallbacks
    def test_ack(self):
        self.initialize_properties()
        past = self.worker.get_local_time() - timedelta(hours=5)

        event = self.mkmsg_delivery_for_send(
                   event_type='ack',
                   user_message_id='1')

        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past), 
            message_direction='outgoing',
            message_status='pending',
            message_id='1')

        self.worker.collections['history'].save_history(**history)

        yield self.app_helper.dispatch_event(event)

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})
        self.assertEqual('ack', status['message-status'])
        credit_log = self.collections['credit_logs'].find_one()
        self.assertEqual(1, credit_log['outgoing-acked'])

    @inlineCallbacks
    def test_nack(self):
        self.initialize_properties()
        past = self.worker.get_local_time() - timedelta(hours=5)

        event = self.mkmsg_delivery_for_send(
                   event_type='nack',
                   user_message_id='1',
                   nack_reason ='HTTP ERROR ...')

        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past),
            message_direction='outgoing',
            message_status='pending',
            message_id='1')

        self.worker.collections['history'].save_history(**history)

        yield self.app_helper.dispatch_event(event)

        history = self.collections['history'].find_one({
            'message-id': event['user_message_id']})
        self.assertEqual('nack', history['message-status'])
        credit_log = self.collections['credit_logs'].find_one()
        self.assertEqual(1, credit_log['outgoing-nacked'])
        self.assertEqual('HTTP ERROR ...', history['failure-reason'])

    @inlineCallbacks
    def test_ack_forward(self):
        self.initialize_properties()
        past = self.worker.get_local_time() - timedelta(hours=2)
        
        event = self.mkmsg_delivery_for_send(
            event_type='ack',
            user_message_id='2',
            transport_metadata={'transport_type':'http_api'})

        history = self.mkobj_history_dialogue(
            dialogue_id='1',
            interaction_id='1',
            timestamp=time_to_vusion_format(past), 
            direction='incoming',
            message_id='1')

        history_id = self.collections['history'].save_history(**history)
        self.worker.collections['history'].update_forwarding(history_id, '2', 'http://partner.com')

        yield self.app_helper.dispatch_event(event)

        history = self.collections['history'].find_one()
        self.assertEqual('ack', history['forwards'][0]['status'])

    @inlineCallbacks
    def test_nack_forward(self):
        self.initialize_properties()
        past = self.worker.get_local_time() - timedelta(hours=2)

        event = self.mkmsg_delivery_for_send(
            event_type='nack',
            user_message_id='2',
            nack_reason='HTTP ERROR ...',
            transport_metadata={'transport_type':'http_api'})

        history = self.mkobj_history_dialogue(
            dialogue_id='1',
            interaction_id='1',
            timestamp=time_to_vusion_format(past),
            direction='incoming',
            message_id='1')

        history_id = self.collections['history'].save(history)
        self.worker.collections['history'].update_forwarding(history_id, '2', 'http://partner.com')

        yield self.app_helper.dispatch_event(event)

        history = self.collections['history'].find_one()
        self.assertEqual('nack', history['forwards'][0]['status'])
        self.assertEqual('HTTP ERROR ...', history['forwards'][0]['failure-reason'])

    @inlineCallbacks
    def test_delivery(self):
        self.initialize_properties()
        
        past = self.worker.get_local_time() - timedelta(hours=5)        
        event = self.mkmsg_delivery_for_send(user_message_id='1')
        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past), 
            message_direction='outgoing',
            message_status='pending',
            message_id='1',)
        
        self.worker.collections['history'].save_history(**history)

        yield self.app_helper.dispatch_event(event)

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})
        self.assertEqual('delivered', status['message-status'])
        credit_log = self.collections['credit_logs'].find_one()
        self.assertEqual(1, credit_log['outgoing-delivered'])
        
        credit_log = self.collections['credit_logs'].find_one()
        self.assertEqual(1, credit_log['outgoing-delivered'])        

    @inlineCallbacks
    def test_delivery_no_reference(self):
        self.initialize_properties()
        event = self.mkmsg_delivery_for_send()

        yield self.app_helper.dispatch_event(event)

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertNot(status)

    @inlineCallbacks
    def test_delivery_failure(self):
        self.initialize_properties()
        
        past = self.worker.get_local_time() - timedelta(hours=5)                    
        event = self.mkmsg_delivery_for_send(
            delivery_status='failed',
            failure_code='404',
            failure_level='http',
            failure_reason='some reason',
            user_message_id='1')

        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past), 
            message_direction='outgoing',
            message_status='pending',
            message_id='1')
       
        self.worker.collections['history'].save_history(**history)

        yield self.app_helper.dispatch_event(event)

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('failed', status['message-status'])
        self.assertEqual('Level:http Code:404 Message:some reason',
                         status['failure-reason'])
        credit_log = self.collections['credit_logs'].find_one()
        self.assertEqual(1, credit_log['outgoing-failed'])        

    @inlineCallbacks
    def test_delivery_failure_no_details(self):
        self.initialize_properties()
        past = self.worker.get_local_time() - timedelta(hours=5)
                            
        event = self.mkmsg_delivery_for_send(
            delivery_status='failed',
            user_message_id='1')

        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past), 
            message_direction='outgoing',
            message_status='pending',
            message_id='1')
       
        self.worker.collections['history'].save_history(**history)

        yield self.app_helper.dispatch_event(event)

        history = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('failed', history['message-status'])
        self.assertEqual('Level:unknown Code:unknown Message:unknown', history['failure-reason'])
