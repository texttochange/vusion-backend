"""Tests for vusion.persist.HistoryManager"""
import pymongo
from datetime import timedelta

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper, PrintLogger
from vusion.persist import HistoryManager, history_generator
from vusion.utils import time_to_vusion_format, date_from_vusion_format, time_to_vusion_format_date



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
        updated_history = history_generator(**updated_history)        
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
        updated_history = history_generator(**updated_history)        
        self.assertEqual(
            updated_history['forwards'][0],
            {'status': 'failed', 'message-id': '2','timestamp': '2013-08-06T15:15:01', 'to-addr':'http://something', 'failure-reason': 'some issue'})

    def test_update_forwarding_history(self):
        past = self.property_helper.get_local_time() - timedelta(minutes=1)
        more_past = past - timedelta(hours=2)        

        history = self.mkobj_history_dialogue(
            message_id='1',
            dialogue_id='1',
            interaction_id='1',
            timestamp=time_to_vusion_format(past),
            direction='incoming',
            message_status='received')
        history_id = self.history_manager.save(history)
        
        history_other = self.mkobj_history_dialogue(
             dialogue_id='1',
             interaction_id='1',
             timestamp=time_to_vusion_format(more_past),
             direction='incoming',
             message_status='received')
        history_other_id = self.history_manager.save(history_other)                
        
        self.history_manager.update_forwarding(history_id, '2', 'http://partner.com')
        
        updated_history = self.history_manager.find_one({'_id': history_id})
        updated_history = history_generator(**updated_history)
        self.assertEqual(
            updated_history['forwards'][0],
            {'status': 'pending', 
             'message-id': '2',
             'timestamp': self.property_helper.get_local_time('vusion'), 
             'to-addr':'http://partner.com'})
        
        not_updated_history = self.history_manager.find_one({'_id': history_other_id})
        not_updated_history = history_generator(**not_updated_history)
        self.assertEqual(
            not_updated_history['message-status'],
            'received')
        self.assertFalse('forwards' in not_updated_history)


    def test_count_day_credits(self):
        past = self.property_helper.get_local_time() - timedelta(hours=2)
            
        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past), 
            message_direction='outgoing',
            message_status='ack',
            message_id='1')
        self.history_manager.save(history)
        
        result = self.history_manager.count_day_credits(past)
        self.assertEqual({"incoming": 0,
                          "outgoing": 1,
                          "outgoing-pending": 0,
                          "outgoing-acked": 1,
                          "outgoing-nacked": 0,
                          "outgoing-failed": 0,
                          "outgoing-delivered": 0}, 
                         result)        
        
        history = self.mkobj_history_dialogue(
            '1', '4',
            time_to_vusion_format(past),
            message_status='delivered')
        self.history_manager.save(history)        
        
        result = self.history_manager.count_day_credits(past)
        self.assertEqual({"incoming": 0,
                          "outgoing": 2,
                          "outgoing-pending": 0,
                          "outgoing-acked": 1,
                          "outgoing-nacked": 0,
                          "outgoing-failed": 0,
                          "outgoing-delivered": 1}, 
                         result)
        
        history = self.mkobj_history_request(
            '4',
            time_to_vusion_format(past), 
            message_direction='incoming',
            message_status='received')
        self.history_manager.save(history)        

        result = self.history_manager.count_day_credits(past)
        self.assertEqual({"incoming": 1,
                          "outgoing": 2,
                          "outgoing-pending": 0,
                          "outgoing-acked": 1,
                          "outgoing-nacked": 0,
                          "outgoing-failed": 0,
                          "outgoing-delivered": 1}, 
                         result)
        
        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past), 
            message_direction='outgoing',
            message_status='pending')
        self.history_manager.save(history)
        result = self.history_manager.count_day_credits(past)
        self.assertEqual({"incoming": 1,
                          "outgoing": 3,
                          "outgoing-pending": 1,
                          "outgoing-acked": 1,
                          "outgoing-nacked": 0,
                          "outgoing-failed": 0,
                          "outgoing-delivered": 1}, 
                         result)

        history = self.mkobj_history_unattach_failed(
            '4',
            time_to_vusion_format(past))
        self.history_manager.save(history)
        result = self.history_manager.count_day_credits(past)
        self.assertEqual({"incoming": 1,
                          "outgoing": 4,
                          "outgoing-pending": 1,
                          "outgoing-acked": 1,
                          "outgoing-nacked": 0,
                          "outgoing-failed": 1,
                          "outgoing-delivered": 1}, 
                         result)

    def test_get_older_date(self):
        now = self.property_helper.get_local_time()
        past = self.property_helper.get_local_time() - timedelta(days=1)
        past_more = past - timedelta(days=1)
        past_more_more = past_more - timedelta(days=2)

        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(now))
        self.history_manager.save(history)
        
        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past))
        self.history_manager.save(history)

        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past_more))
        self.history_manager.save(history)

        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past_more_more))
        self.history_manager.save(history)
        
        date = self.history_manager.get_older_date()
        self.assertEqual(date.date(), now.date())
        
        date = self.history_manager.get_older_date(now)
        self.assertEqual(date.date(), past.date())
        
        date = self.history_manager.get_older_date(past_more)
        self.assertEqual(date.date(), past_more_more.date())
        
        date = self.history_manager.get_older_date(past_more_more)
        self.assertTrue(date is None)

    def test_get_older_date_midnight(self):
        
        history = self.mkobj_history_unattach(
            '4',
            '2013-05-20T00:00:00')
        self.history_manager.save(history)
        
        history = self.mkobj_history_unattach(
            '4',
            '2013-05-19T23:58:00')
        self.history_manager.save(history)
        
        date = self.history_manager.get_older_date(
            date_from_vusion_format('2013-05-20T00:00:00'))
        self.assertEqual(
            '2013-05-19', 
            time_to_vusion_format_date(date))

    def test_get_status_and_credits(self):
        past = self.property_helper.get_local_time() - timedelta(hours=3)
        history = self.mkobj_history_unattach(
            '4',
            time_to_vusion_format(past), 
            message_direction='outgoing',
            message_status='pending',
            message_id='1')
        self.history_manager.save(history)

        history = self.history_manager.get_status_and_credits('1')
        self.assertEqual(history['message-status'], 'pending')
        self.assertEqual(history['message-credits'], 1)

    @inlineCallbacks
    def test_was_unattach_sent(self):
        history = self.mkobj_history_unattach(
            '4',
            '2013-01-01T10:10:10',
            participant_phone='06')
        self.history_manager.save(history)

        result = yield self.history_manager.was_unattach_sent('06', '4')
        self.assertTrue(result)
        result = yield self.history_manager.was_unattach_sent('07', '4')
        self.assertFalse(result)
