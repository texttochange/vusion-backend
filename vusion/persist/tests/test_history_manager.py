"""Tests for vusion.persist.HistoryManager"""
import pymongo
from datetime import timedelta

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper
from vusion.persist import HistoryManager, history_generator
from vusion.utils import time_to_vusion_format


class TestHistoryManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        c.safe = True
        db = c.test_program_db
        self.history_manager = HistoryManager(db, 'history')
        self.clearData()

        #parameters:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)
        self.property_helper['timezone'] = 'Africa/Kampala'
        
        self.history_manager.set_property_helper(self.property_helper)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.history_manager.drop()

    def test_update_status_history_ack(self):
        past = self.property_helper.get_local_time() - timedelta(hours=2)
        
        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past), 
            message_direction='outgoing',
            message_status='pending',
            message_id='1')
        self.history_manager.save(history)
        
        self.history_manager.update_status('1', 'ack')

        updated_history = self.history_manager.find_one()
        self.assertEqual(
            updated_history['message-status'],
            'ack')

    def test_update_status_history_fail(self):
        past = self.property_helper.get_local_time() - timedelta(hours=2)
        
        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past), 
            message_direction='outgoing',
            message_status='pending',
            message_id='1')
        self.history_manager.save(history)
        
        self.history_manager.update_status('1', {'status': 'failed', 'reason': 'something happend'})

        updated_history = self.history_manager.find_one()
        updated_history = history_generator(**updated_history)
        self.assertEqual(
            updated_history['message-status'],
            'failed')

    def test_update_forwared_status_history_ack(self):
        past = self.property_helper.get_local_time() - timedelta(hours=2)
        
        history = self.mkobj_history_dialogue(
            dialogue_id='1',
            interaction_id='1',
            timestamp=time_to_vusion_format(past),
            direction='incoming',
            message_status='forwarded', 
            forwards=[{'status': 'pending', 'message-id': '2','timestamp': '2013-08-06T15:15:01', 'to-addr':'http://something'}])
        self.history_manager.save(history)
        
        self.history_manager.update_forwarded_status('2', 'ack')
        
        updated_history = self.history_manager.find_one()
        self.assertEqual(
            updated_history['forwards'][0],
            {'status': 'ack', 'message-id': '2','timestamp': '2013-08-06T15:15:01', 'to-addr':'http://something'})

    def test_update_forwared_status_history_failed(self):
        past = self.property_helper.get_local_time() - timedelta(hours=2)
        
        history = self.mkobj_history_dialogue(
            dialogue_id='1',
            interaction_id='1',
            timestamp=time_to_vusion_format(past),
            direction='incoming',
            message_status='forwarded', 
            forwards=[{'status': 'pending', 'message-id': '2','timestamp': '2013-08-06T15:15:01', 'to-addr':'http://something'}])
        self.history_manager.save(history)
        
        self.history_manager.update_forwarded_status('2', {'status': 'failed', 'reason': 'some issue'})
        
        updated_history = self.history_manager.find_one()
        self.assertEqual(
            updated_history['forwards'][0],
            {'status': 'failed', 'message-id': '2','timestamp': '2013-08-06T15:15:01', 'to-addr':'http://something', 'failure-reason': 'some issue'})

    def test_update_forwarding_history(self):
        past = self.property_helper.get_local_time() - timedelta(minutes=1)
        
        history = self.mkobj_history_dialogue(
            message_id='1',
            dialogue_id='1',
            interaction_id='1',
            timestamp=time_to_vusion_format(past),
            direction='incoming',
            message_status='received')
        history_id = self.history_manager.save(history)
        
        self.history_manager.update_forwarding(history_id, '2', 'http://partner.com')
        
        updated_history = self.history_manager.find_one()
        self.assertEqual(
            updated_history['forwards'][0],
            {'status': 'pending', 
             'message-id': '2',
             'timestamp': self.property_helper.get_local_time('vusion'), 
             'to-addr':'http://partner.com'})

        
        