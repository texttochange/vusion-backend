from datetime import datetime, time, date, timedelta
import pytz

import json
import pymongo
from bson.objectid import ObjectId
from bson.timestamp import Timestamp

from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from vumi.message import Message, TransportEvent, TransportUserMessage
from vumi.tests.utils import get_stubbed_worker, UTCNearNow, RegexMatcher

from vusion.dialogue_worker import DialogueWorker
from vusion.utils import time_to_vusion_format, time_from_vusion_format

from tests.utils import MessageMaker, DataLayerUtils, ObjectMaker
from vusion.tests.test_dialogue_worker import DialogueWorkerTestCase

from vusion.persist import Dialogue, history_generator
from vusion.action import Actions


class DialogueWorkerTestCase_consumeParticipantMessage(DialogueWorkerTestCase):
    
    @inlineCallbacks
    def test_receive_inbound_message_matching(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time()
        self.collections['dialogues'].save(self.mkobj_dialogue_question_offset_days())
        self.collections['participants'].save(self.mkobj_participant(
            '06',
            enrolled=[{'dialogue-id': '01', 
                       'date-time': time_to_vusion_format(dNow)}]))

        inbound_msg_matching = self.mkmsg_in(
            from_addr='06',
            content='Feel ok')
        yield self.send(inbound_msg_matching, 'inbound')
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
        self.assertEqual(2, self.collections['history'].count())
        
        #Only message matching keyword should be forwarded to the worker
        inbound_msg_non_matching_keyword = self.mkmsg_in(
            from_addr='06',
            content='ok')
        yield self.send(inbound_msg_non_matching_keyword, 'inbound')
        self.assertEqual(3, self.collections['history'].count())
        history_unmatching = self.collections['history'].find_one({
            'object-type': 'unmatching-history'})
        self.assertTrue(history_unmatching is not None)
        
        inbound_msg_non_matching_answer = self.mkmsg_in(
            from_addr='06',
            content='Feel good')
        yield self.send(inbound_msg_non_matching_answer, 'inbound')

        self.assertEqual(4, self.collections['history'].count())
        histories = self.collections['history'].find({'object-type': 'dialogue-history'})
        self.assertEqual('01-01', histories[0]['interaction-id'])
        self.assertEqual('01', histories[0]['dialogue-id'])
        self.assertEqual('Ok', histories[0]['matching-answer'])
        self.assertEqual(None, histories[2]['matching-answer'])
        for history in histories:
            self.assertEqual('1', history['participant-session-id'])
        self.assertEqual(0, self.collections['schedules'].count())

    @inlineCallbacks
    def test_receive_inbound_message_matching_offset_condition(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()
        dNow = self.worker.get_local_time()
        self.collections['dialogues'].save(self.mkobj_dialogue_open_question_offset_conditional())
        self.collections['participants'].save(self.mkobj_participant(
            '06',
            enrolled=[{'dialogue-id': '04', 
                       'date-time': time_to_vusion_format(dNow)}]))
        self.collections['history'].save(self.mkobj_history_dialogue(
            participant_phone='06',
            participant_session_id='1',
            dialogue_id='04',
            interaction_id='01-01',
            direction='outgoing',
            timestamp=time_to_vusion_format(dNow)
        ))
        
        inbound_msg_matching = self.mkmsg_in(
            from_addr='06',
            content='name olivier')
        yield self.send(inbound_msg_matching, 'inbound')
        
        self.assertEqual(1, self.collections['schedules'].count())

    @inlineCallbacks
    def test_receive_inbound_message_matching_with_reminder(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=4)
        dFuture = dNow + timedelta(minutes=10)

        dialogue = self.mkobj_dialogue_open_question_reminder()
        self.collections['dialogues'].save(dialogue)
        participant = self.mkobj_participant(
            '06',
            session_id='1',
            enrolled=[{'dialogue-id':'04',
                       'date-time': time_to_vusion_format(dNow)}])
        self.collections['participants'].save(participant)
                
        dialogue['interactions'][0]['date-time'] = time_to_vusion_format(dPast)
        
        self.collections['history'].save(self.mkobj_history_dialogue(
            participant_phone='06',
            participant_session_id='1',
            direction='outgoing',
            dialogue_id='04',
            interaction_id='01-01',
            timestamp= time_to_vusion_format(dPast)))
        
        self.collections['schedules'].save(self.mkobj_schedule(
            dialogue_id='04',
            interaction_id='01-01',
            object_type='reminder-schedule',
            participant_phone='06',
            date_time=time_to_vusion_format(dFuture)))
        
        self.collections['schedules'].save(self.mkobj_schedule(
            dialogue_id='04',
            interaction_id='01-01',
            object_type = 'deadline-schedule',
            participant_phone='06',
            date_time=time_to_vusion_format(dFuture)))
          
        inbound_msg_matching = self.mkmsg_in(
            from_addr='06',
            content='name ok')
        
        yield self.send(inbound_msg_matching, 'inbound')

        self.assertEqual(0, self.collections['schedules'].count())   

    @inlineCallbacks
    def test_receive_inbound_message_no_repeat_dialogue_action(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.collections['program_settings'].save({
            'key': 'double-matching-answer-feedback',
            'value': 'you have already answer this message'})
        self.worker.load_data()

        dNow = self.worker.get_local_time()
        
        self.collections['dialogues'].save(self.mkobj_dialogue_open_question())
        self.collections['participants'].save(self.mkobj_participant(
            '06',
            enrolled=[{'dialogue-id':'04',
                       'date-time': time_to_vusion_format(dNow)}]))

        inbound_msg_matching_request = self.mkmsg_in(
            from_addr='06',
            content='name john doe')
        yield self.send(inbound_msg_matching_request, 'inbound')

        participant = self.collections['participants'].find_one({'phone': '06'})
        self.assertTrue('name' in participant['profile'][0]['label'])
        self.assertEqual('john doe', participant['profile'][0]['value'])
        
        ## One Way road, and action is not replayed
        inbound_msg_matching_request = self.mkmsg_in(
            from_addr='06',
            content='name olivier')
        yield self.send(inbound_msg_matching_request, 'inbound')
        participant = self.collections['participants'].find_one({'phone': '06'})
        self.assertEqual('john doe', participant['profile'][0]['value'])
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
        history = self.collections['history'].find_one({
            'object-type': 'dialogue-history',
            'message-direction': 'outgoing'})
        self.assertEqual('you have already answer this message', history['message-content'])

    @inlineCallbacks
    def test_receive_inbound_message_no_repeat_dialogue_enroll(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dNow = self.worker.get_local_time()
        
        self.collections['dialogues'].save(self.mkobj_dialogue_open_question_enroll_action('0'))
        self.collections['participants'].save(self.mkobj_participant(
            '06',
            enrolled=[{'dialogue-id':'04',
                       'date-time': time_to_vusion_format(dNow)}]))

        self.collections['history'].save(self.mkobj_history_dialogue(
            participant_phone='06',
            participant_session_id='1',
            direction = 'incoming',
            dialogue_id='04',
            interaction_id='01-01',
            matching_answer='1',
            timestamp=time_to_vusion_format(dNow)))

        inbound_msg_matching_request = self.mkmsg_in(
            from_addr='06',
            content='name john doe')

        yield self.send(inbound_msg_matching_request, 'inbound')

        participant = self.collections['participants'].find_one({'phone': '06'})
        self.assertEqual(1, len(participant['enrolled'])) 

    @inlineCallbacks
    def test_receive_inbound_message_only_enrolled(self):
        self.collections['dialogues'].save(self.mkobj_dialogue_open_question())
        self.collections['participants'].save(self.mkobj_participant('06'))

        inbound_msg_matching_request = self.mkmsg_in(
            from_addr='06',
            content='name john doe')
        yield self.send(inbound_msg_matching_request, 'inbound')

        participant = self.collections['participants'].find_one({'phone': '06'})
        self.assertEqual(participant['profile'], [])

    @inlineCallbacks
    def test_receive_inbound_request_not_optin(self):
        request_id = self.collections['requests'].save(self.mkobj_request_response())
      
        inbound_msg_matching_request = self.mkmsg_in(from_addr='07',
                                                     content='wWw info')
        yield self.send(inbound_msg_matching_request, 'inbound')
        
        self.assertEqual(1, self.collections['history'].count())
        self.assertEqual(0, self.collections['schedules'].count())
        
    @inlineCallbacks
    def test_receive_inbound_request_optin(self):
        request_id = self.collections['requests'].save(self.mkobj_request_join())
      
        inbound_msg_matching_request = self.mkmsg_in(from_addr='07',
                                                     content='www')
        yield self.send(inbound_msg_matching_request, 'inbound')
        
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 2)
        self.assertEqual(0, self.collections['schedules'].count())
        self.assertFalse(self.collections['participants'].find_one({'phone': '07'}) is None)

    @inlineCallbacks
    def test_receive_inbound_message_request_optin(self):
        request_id = self.collections['requests'].save(self.mkobj_request_join())
      
        inbound_msg_matching_request = self.mkmsg_in(from_addr='07',
                                                     content='wWw')
        yield self.send(inbound_msg_matching_request, 'inbound')

        inbound_msg_matching_request = self.mkmsg_in(from_addr='08',
                                                     content='www join')
        yield self.send(inbound_msg_matching_request, 'inbound')

        self.assertEqual(6, self.collections['history'].count())
        self.assertEqual(2, self.collections['participants'].count())
        self.assertEqual(0, self.collections['schedules'].count())
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 4)
        
        participant = self.collections['participants'].find_one({'phone': '07'})        
        histories = self.collections['history'].find()
        self.assertEqual(histories.count(), 6)
        for history in histories:
            self.assertEqual(history['request-id'], request_id)
        histories = self.collections['history'].find({'participant-phone': '07'})
        for history in histories:
            self.assertEqual(participant['session-id'], history['participant-session-id'])

    @inlineCallbacks
    def test_receive_inbound_message_from_non_participant(self):
        self.collections['requests'].save(self.mkobj_request_join())
        self.collections['requests'].save(self.mkobj_request_tag())
        self.collections['requests'].save(self.mkobj_request_leave())
        
        # No action in case never optin
        inbound_msg_matching = self.mkmsg_in(from_addr='06',
                                             content='www tagme')
        yield self.send(inbound_msg_matching, 'inbound')
        
        self.assertEqual(0, self.collections['participants'].count())
        self.assertEqual(0, self.collections['schedules'].count())
        self.assertEqual(1, self.collections['history'].count())

        # Still participant can optin
        inbound_msg_matching = self.mkmsg_in(from_addr='06',
                                             content='www join')
        yield self.send(inbound_msg_matching, 'inbound')
        self.assertEqual(1, self.collections['participants'].count())
        self.assertEqual(0, self.collections['schedules'].count())
        self.assertEqual(4, self.collections['history'].count())
        
        # When they optout no action is performed
        inbound_msg_matching = self.mkmsg_in(from_addr='06',
                                             content='www quit')
        yield self.send(inbound_msg_matching, 'inbound')
        self.assertEqual(0, self.collections['schedules'].count())
        self.assertEqual(5, self.collections['history'].count())
        
        inbound_msg_matching = self.mkmsg_in(from_addr='06',
                                             content='www tagme')
        yield self.send(inbound_msg_matching, 'inbound')
        self.assertEqual(None, self.collections['participants'].find_one({'tags':'onetag'}))
        self.assertEqual(0, self.collections['schedules'].count())
        self.assertEqual(7, self.collections['history'].count())

    @inlineCallbacks
    def test_receive_delivery(self):
        event = self.mkmsg_delivery_for_send()

        self.collections['history'].save({
            'message-id': event['user_message_id'],
            'message-direction': 'outgoing',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('delivered', status['message-status'])

    @inlineCallbacks
    def test_receive_delivery_no_reference(self):
        event = self.mkmsg_delivery_for_send()

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertNot(status)

    @inlineCallbacks
    def test_receive_delivery_failure(self):
        event = self.mkmsg_delivery_for_send(delivery_status='failed',
                                             failure_code='404',
                                             failure_level='http',
                                             failure_reason='some reason')

        self.collections['history'].save({
            'message-id': event['user_message_id'],
            'message-direction': 'outgoing',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('failed', status['message-status'])
        self.assertEqual('Code:404 Level:http Message:some reason',
                         status['failure-reason'])

    @inlineCallbacks
    def test_receive_ack(self):
        event = self.mkmsg_delivery_for_send(event_type='ack',
                                             user_message_id='2')

        self.collections['history'].save({
            'message-id': event['user_message_id'],
            'message-direction': 'outgoing',
            'message-status': 'pending'
        })

        yield self.send(event, 'event')

        status = self.collections['history'].find_one({
            'message-id': event['user_message_id']})

        self.assertEqual('ack', status['message-status'])
        
    @inlineCallbacks
    def test_limit_max_unmatching_answers(self):
        for program_setting in self.mkobj_program_settings():
            self.collections['program_settings'].save(program_setting)
        self.worker.load_data()

        dNow = self.worker.get_local_time()
        dFuture = dNow + timedelta(minutes=10)
        
        dialogue = self.mkobj_dialogue_question_max_unmatching()
        self.collections['dialogues'].save(dialogue)
        
        #dialogue_helper = Dialogue(**dialogue)
        participant = self.mkobj_participant(
            '06',
            enrolled = [{'dialogue-id': dialogue['dialogue-id'],
                         'date-time': dNow}])
        self.collections['participants'].save(participant)
        
        self.collections['schedules'].save(self.mkobj_schedule(
            dialogue_id='01',
            interaction_id='01-01',
            object_type='reminder-schedule',
            participant_phone='06',
            date_time=time_to_vusion_format(dFuture)))
        
        self.collections['schedules'].save(self.mkobj_schedule(
            dialogue_id='01',
            interaction_id='01-01',
            object_type = 'deadline-schedule',
            participant_phone='06',
            date_time=time_to_vusion_format(dFuture)))
        
        inbound_msg_unmatching = self.mkmsg_in(from_addr='06',
                                             content='feel weird')
        #participant = self.collections['participants'].find_one({'phone': '06'})
        for num in range(5):
            self.assertEqual(self.collections['schedules'].count(), 2)
            yield self.send(inbound_msg_unmatching, 'inbound')
        self.assertEqual(7, self.collections['history'].count())
        history = self.collections['history'].find_one({'object-type': 'oneway-marker-history'})
        self.assertTrue(history is not None)
        history_feedback = self.collections['history'].find_one({
            'object-type': 'dialogue-history',
            'message-direction': 'outgoing'})
        self.assertEqual(history_feedback['message-content'], 'You reached the limit')
        
        self.assertEqual(self.collections['schedules'].count(), 0)
        
        inbound_msg_matching = self.mkmsg_in(from_addr='06',
                                             content='feel ok')        
        yield self.send(inbound_msg_matching, 'inbound')
        self.assertEqual(self.collections['schedules'].count(), 0)
        
        yield self.send(inbound_msg_unmatching, 'inbound')
        self.assertEqual(self.collections['schedules'].count(), 0)
        