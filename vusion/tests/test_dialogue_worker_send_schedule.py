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
                date_time=time_to_vusion_format(dPast),
                dialogue_id='2',
                interaction_id='0',
                participant_phone='09'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=time_to_vusion_format(dNow),
                dialogue_id='2',
                interaction_id='1',
                participant_phone='09'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=time_to_vusion_format(dFuture),
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
                date_time=time_to_vusion_format(dNow),
                dialogue_id='2',
                interaction_id='0',
                participant_phone='10'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=time_to_vusion_format(dNow),
                dialogue_id='2',
                interaction_id='1',
                participant_phone='10'))
        self.collections['schedules'].save(
            self.mkobj_schedule(
                date_time=time_to_vusion_format(dNow),
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
            'date-time': dPast,
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
            'date-time': dPast,
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
            'date-time': dPast,
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
                date_time=time_to_vusion_format(dPast),
                dialogue_id='05',
                interaction_id='05',
                participant_phone='06'))

        yield self.worker.send_scheduled()

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['content'],
                         'What is your gender?\n male or female')

    @inlineCallbacks
    def test_send_scheduled_messages_sms_limit_no_credit(self):
        settings = self.mk_program_settings(
            sms_limit_type='outgoing-incoming',
            sms_limit_number=4,
            sms_limit_from_date='2013-01-01T00:00:00',
            sms_limit_to_date='2020-01-01T00:00:00')
        self.initialize_properties(program_settings=settings)
        
        dNow = self.worker.get_local_time()
        dJustPast = dNow - timedelta(minutes=1)
        dPast = dNow - timedelta(minutes=60)
        dFuture = dNow + timedelta(minutes=30)        
        
        ## A first to be send
        unattached = self.mkobj_unattach_message(
            content=self.mk_content(280),
            fixed_time=time_to_vusion_format(dJustPast))
        unattached_id = self.collections['unattached_messages'].save(unattached)
        
        participant = self.mkobj_participant('+1', session_id='1')
        self.collections['participants'].save(participant)
        participant = self.mkobj_participant('+2', session_id='1')
        self.collections['participants'].save(participant)

        schedule_first = self.mkobj_schedule_unattach(
            participant_phone='+1',
            participant_session_id='1',
            unattach_id=str(unattached_id),
            date_time=time_to_vusion_format(dJustPast))
        schedule_second = self.mkobj_schedule_unattach(
            participant_phone='+2',
            participant_session_id='1',            
            unattach_id=str(unattached_id),
            date_time=time_to_vusion_format(dJustPast))
        self.collections['schedules'].save(schedule_first)
        self.collections['schedules'].save(schedule_second)
        
        ## A second for which, the program doesn't have enougth credit
        unattached = self.mkobj_unattach_message(
            content=self.mk_content(),
            fixed_time=time_to_vusion_format(dJustPast))
        unattached_id = self.collections['unattached_messages'].save(unattached)
        
        schedule_no_credit = self.mkobj_schedule_unattach(
            participant_phone='+1',
            participant_session_id='1',
            unattach_id=str(unattached_id),
            date_time=time_to_vusion_format(dJustPast))
        self.collections['schedules'].save(schedule_no_credit)
        
        yield self.worker.send_scheduled()
        
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 2)
        histories = self.collections['history'].find()
        self.assertEqual(histories.count(), 3)
        self.assertEqual(histories[0]['message-status'], 'pending')
        self.assertEqual(histories[0]['message-credits'], 2)
        self.assertEqual(histories[1]['message-status'], 'pending')
        self.assertEqual(histories[1]['message-credits'], 2)
        self.assertEqual(histories[2]['message-status'], 'no-credit')
        self.assertEqual(histories[2]['message-credits'], 0)
        

    @inlineCallbacks
    def test_send_scheduled_messages_sms_limit_no_credit_timeframe(self):
        settings = self.mk_program_settings(
            sms_limit_type='outgoing-incoming',
            sms_limit_number=4,
            sms_limit_from_date='2013-01-01T00:00:00',
            sms_limit_to_date='2013-01-02T00:00:00')
        self.initialize_properties(program_settings=settings)
        
        dNow = self.worker.get_local_time()
        dJustPast = dNow - timedelta(minutes=1)
        
        ## A first to be send
        unattached = self.mkobj_unattach_message(
            content=self.mk_content(),
            fixed_time=time_to_vusion_format(dJustPast))
        unattached_id = self.collections['unattached_messages'].save(unattached)
        
        participant = self.mkobj_participant('+1', session_id='1')
        self.collections['participants'].save(participant)

        schedule_first = self.mkobj_schedule_unattach(
            participant_phone='+1',
            participant_session_id='1',
            unattach_id=str(unattached_id),
            date_time=time_to_vusion_format(dJustPast))
        self.collections['schedules'].save(schedule_first)
        
        yield self.worker.send_scheduled()
        
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 0)
        histories = self.collections['history'].find()
        self.assertEqual(histories.count(), 1)
        self.assertEqual(histories[0]['message-status'], 'no-credit-timeframe')
        self.assertEqual(histories[0]['message-credits'], 0)

    @inlineCallbacks
    def test_send_scheduled_messages_fail_missing_data(self):
        self.initialize_properties()
        
        dNow = self.worker.get_local_time()
        dNow = dNow - timedelta(minutes=2)

        participant = self.mkobj_participant(
            '06',
            profile=[{'label': 'name',
                      'value': 'oliv'}])
        self.collections['participants'].save(participant)
       
        unattached_message = self.mkobj_unattach_message(
            content="Hello [participant.firstname]",
            fixed_time=time_to_vusion_format(dNow))
        
        unattached_message_id = self.collections['unattached_messages'].save(unattached_message)

        self.collections['schedules'].save(
            self.mkobj_schedule_unattach(
                date_time=time_to_vusion_format(dNow),
                unattach_id=str(unattached_message_id),
                participant_phone='06'))

        yield self.worker.send_scheduled()

        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 0)
        histories = self.collections['history'].find()
        self.assertEqual(histories.count(), 1)
        self.assertEqual(histories[0]['message-status'], 'missing-data')
        self.assertEqual(histories[0]['missing-data'], ['Participant 06 doesn\'t have a label firstname'])
        self.assertEqual(histories[0]['unattach-id'], str(unattached_message_id))
