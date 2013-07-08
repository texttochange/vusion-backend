from datetime import datetime, time, date, timedelta

from twisted.internet.defer import inlineCallbacks

from vusion.persist import Dialogue, schedule_generator
from vusion.utils import time_to_vusion_format, time_from_vusion_format

from test_dialogue_worker import DialogueWorkerTestCase


class DialogueWorkerTestCase_sendSchedule(DialogueWorkerTestCase):

    def test_send_scheduled_messages(self):
        self.initialize_properties()
        
        dNow = self.worker.get_local_time()
        dNow = dNow - timedelta(minutes=2)
        dPast = dNow - timedelta(minutes=61)
        dFuture = dNow + timedelta(minutes=30)

        dialogue = self.mkobj_dialogue_announcement_2()
        participant_transport_metadata = {'some_key': 'some_value'}
        participant = self.mkobj_participant('09', transport_metadata=participant_transport_metadata)

        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
       
        unattached_message = self.collections['unattached_messages'].save({
            'date-time': time_to_vusion_format(dNow),
            'content': 'Hello unattached',
            'to': 'all participants',
            'type-interaction': 'annoucement'
        })
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dPast.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='0',
                participant_phone='09'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dNow.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='1',
                participant_phone='09'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dFuture.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='2',
                participant_phone='09'))
        self.collections['schedules'].save(
            self.mkobj_schedule_unattach(
                date_time=time_to_vusion_format(dNow),
                unattach_id=unattached_message,
                participant_phone='09'))
        self.collections['schedules'].save(
            self.mkobj_schedule_feedback(
                date_time=time_to_vusion_format(dNow),
                content='Thank you',
                participant_phone='09',
                context={'dialogue-id': '2', 'interaction-id': '1'}))

        self.worker.send_scheduled()

        participant_transport_metadata.update({'customized_id': 'myid'})
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[0]['content'], 'Today will be sunny')
        self.assertEqual(messages[0]['transport_metadata'], participant_transport_metadata)
        self.assertEqual(messages[1]['content'], 'Hello unattached')
        self.assertEqual(messages[1]['transport_metadata'], participant_transport_metadata)        
        self.assertEqual(messages[2]['content'], 'Thank you')
        participant_transport_metadata.update({'priority': 'prioritized'})        
        self.assertEqual(messages[2]['transport_metadata'], participant_transport_metadata) 
        for message in messages:
            self.assertTrue('customized_id' in message['transport_metadata'])

        self.assertEquals(self.collections['schedules'].count(), 1)
        self.assertEquals(self.collections['history'].count(), 4)
        histories = self.collections['history'].find()
        for history in histories:
            self.assertTrue(history['participant-session-id'] is not None)
            
    def test_send_scheduled_messages_with_priority(self):
        self.initialize_properties()
                
        dNow = self.worker.get_local_time()
        dNow = dNow - timedelta(minutes=2)
       
        dialogue = self.mkobj_dialogue_announcement_prioritized()
        participant = self.mkobj_participant('10')
        
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dNow.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='0',
                participant_phone='10'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dNow.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='1',
                participant_phone='10'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dNow.strftime(self.time_format),
                dialogue_id='2',
                interaction_id='2',
                participant_phone='10'))
        self.collections['schedules'].save(
            self.mkobj_schedule_feedback(
                date_time=time_to_vusion_format(dNow),
                content='Thank you',
                participant_phone='10',
                context={'dialogue-id': '2', 'interaction-id': '1'}))
        
        self.worker.send_scheduled()

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 4)
        self.assertTrue('priority' in messages[0]['transport_metadata'])
        self.assertTrue('priority' in messages[1]['transport_metadata'])
        self.assertTrue('priority' in messages[3]['transport_metadata'])
        
        self.assertFalse('priority' in messages[2]['transport_metadata'])
        
        ## Assert interaction specific priority
        self.assertEqual(messages[0]['transport_metadata']['priority'],
            dialogue['interactions'][0]['prioritized'])
        self.assertEqual(messages[1]['transport_metadata']['priority'],
            dialogue['interactions'][1]['prioritized'])
        ## Assert program specific priority
        self.assertEqual(messages[3]['transport_metadata']['priority'],
            self.worker.properties['request-and-feedback-prioritized'])

    @inlineCallbacks
    def test_send_scheduled_deadline(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=2)

        dialogue = self.mkobj_dialogue_open_question_reminder_offset_time()
        dialogue['interactions'][0]['reminder-actions'].append(
            {'type-action': 'feedback', 'content': 'Bye'})
        dialogue = Dialogue(**dialogue)
        participant = self.mkobj_participant('06')
        self.collections['dialogues'].save(dialogue.get_as_dict())
        self.collections['participants'].save(participant)
        schedule = schedule_generator(**{
            'object-type': 'deadline-schedule',
            'model-version': '2',
            'date-time': dPast.strftime(self.time_format),
            'dialogue-id': '04',
            'interaction-id': '01-01',
            'participant-phone': '06',
            'participant-session-id': '1'})
        self.collections['schedules'].save(schedule.get_as_dict())

        yield self.worker.send_scheduled()

        saved_participant = self.collections['participants'].find_one()
        self.assertEqual(saved_participant['session-id'], None)
        history = self.collections['history'].find_one({'object-type': 'oneway-marker-history'})
        self.assertTrue(history is not None)
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
        history = self.collections['history'].find_one({'object-type': 'dialogue-history'})
        self.assertEqual(history['participant-session-id'], '1')
        self.assertEqual(history['message-content'], 'Bye')

    @inlineCallbacks
    def test_send_scheduled_run_action(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=2)

        dialogue = self.mkobj_dialogue_open_question()
        participant = self.mkobj_participant('06')
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        schedule = schedule_generator(**{
            'participant-phone': '06',
            'date-time': dPast.strftime(self.time_format),
            'object-type': 'action-schedule',
            'action': {'type-action': 'enrolling',
                       'enroll': '04'},
            'context': {'request-id': '1'}})
        self.collections['schedules'].save(schedule.get_as_dict())
        yield self.worker.send_scheduled()

        saved_participant = self.collections['participants'].find_one({
            'enrolled.dialogue-id': '04'})
        self.assertTrue(saved_participant)

    @inlineCallbacks
    def test_send_scheduled_run_action_expired(self):
        self.initialize_properties()
        
        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=61)
        
        dialogue = self.mkobj_dialogue_open_question()
        participant = self.mkobj_participant('06')
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        schedule = schedule_generator(**{
            'participant-phone': '06',
            'date-time': dPast.strftime(self.time_format),
            'object-type': 'action-schedule',
            'action': {'type-action': 'enrolling',
                       'enroll': '04'},
            'context': {'request-id': '1'}})

        self.collections['schedules'].save(schedule.get_as_dict())
        yield self.worker.send_scheduled()
  
        saved_participant = self.collections['participants'].find_one({
            'enrolled.dialogue-id': '04'})
        self.assertTrue(saved_participant is None)
        self.assertEqual(1, self.collections['history'].count())

    @inlineCallbacks
    def test_send_scheduled_question_multi_keyword(self):
        self.initialize_properties()

        dNow = self.worker.get_local_time()
        dPast = dNow - timedelta(minutes=2)

        dialogue = self.mkobj_dialogue_question_multi_keyword()
        participant = self.mkobj_participant('06')
        self.collections['dialogues'].save(dialogue)
        self.collections['participants'].save(participant)
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=dPast.strftime(self.time_format),
                dialogue_id='05',
                interaction_id='05',
                participant_phone='06'))

        yield self.worker.send_scheduled()

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['content'],
                         'What is your gender?\n male or female')
