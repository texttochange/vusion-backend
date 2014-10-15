from twisted.internet.defer import inlineCallbacks

from vumi.message import Message

from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.tests.test_dialogue_worker import DialogueWorkerTestCase
from vusion.persist import Participant


class DialogueWorkerTestCase_consumeControlMessage(DialogueWorkerTestCase):

    @inlineCallbacks
    def test_consume_control_update_schedule_dialogue(self):
        self.initialize_properties()
        self.broker.dispatched = {}
        dNow = self.worker.get_local_time()
    
        #save dialogues
        dialogue_1 = self.mkobj_dialogue_announcement_offset_days()
        dialogue_2 = self.mkobj_dialogue_question_offset_days()
        self.collections['dialogues'].save(dialogue_1)
        self.collections['dialogues'].save(dialogue_2)

        #save participant
        self.collections['participants'].save(
            self.mkobj_participant(
                participant_phone='08'))
    
        event = self.mkmsg_dialogueworker_control(**{
            'action':'update_schedule',
            'schedule_type': 'dialogue',
            'object_id': dialogue_1['dialogue-id']})
        yield self.send(event, 'control')
        
        self.assertEqual(2, self.collections['schedules'].count())
        enrolled_participant = Participant(**self.collections['participants'].find_one())
        self.assertTrue(enrolled_participant.is_enrolled('0'))
    
        # the second dialogue is auto-enrollment = 'none'
        event = self.mkmsg_dialogueworker_control(**{
            'action':'update_schedule',
            'schedule_type': 'dialogue',
            'object_id': dialogue_2['dialogue-id']})
        yield self.send(event, 'control')
        self.assertEqual(2, self.collections['schedules'].count())
    
    @inlineCallbacks
    def test_consume_control_update_schedule_unattached(self):
        self.initialize_properties()
        self.broker.dispatched = {}
        dNow = self.worker.get_local_time()

        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='10'))
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='11', session_id=None))
        unattach = self.mkobj_unattach_message_1()
        unattach_id = self.collections['unattached_messages'].save(unattach)

        event = self.mkmsg_dialogueworker_control(**{
            'action':'update_schedule',
            'schedule_type': 'unattach',
            'object_id': str(unattach_id)})
        yield self.send(event, 'control')
        self.assertEqual(1, self.collections['schedules'].count())

    @inlineCallbacks
    def test_consume_control_mass_tag(self):
        self.initialize_properties()
        self.broker.dispatched = {}
        dNow = self.worker.get_local_time()

        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='10', tags=['geek', 'mombasa']))
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='11', tags=[]))
        unattach = self.mkobj_unattach_message(
            send_to_type='match',
            send_to_match_operator='all',
            send_to_match_conditions=['mombasa'])
        unattach_id = self.collections['unattached_messages'].save(unattach)

        event = self.mkmsg_dialogueworker_control(**{
            'action':'mass_tag',
            'tag': 'mombasa',
            'selector': {'tags': 'geek'}})
        yield self.send(event, 'control')
        self.assertEqual(1, self.collections['schedules'].count())

    @inlineCallbacks
    def test_consume_control_mass_untag(self):
        self.initialize_properties()
        self.broker.dispatched = {}
        dNow = self.worker.get_local_time()

        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='10', tags=['geek']))
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='11', tags=[]))
        unattach = self.mkobj_unattach_message(
            send_to_type='match',
            send_to_match_operator='all',
            send_to_match_conditions=['geek'])
        unattach_id = self.collections['unattached_messages'].save(unattach)

        schedule = self.mkobj_schedule_unattach(
            participant_phone='10',
            date_time='2213-12-20T08:00:00',
            unattach_id=str(unattach_id))
        self.collections['schedules'].save(schedule)

        schedule = self.mkobj_schedule_unattach(
            participant_phone='11',
            date_time='2213-12-20T08:00:00',
            unattach_id=str(unattach_id))
        self.collections['schedules'].save(schedule)

        event = self.mkmsg_dialogueworker_control(**{
            'action':'mass_untag',
            'tag': 'geek'})
        yield self.send(event, 'control')
        self.assertEqual(1, self.collections['schedules'].count())
        schedule = self.collections['schedules'].find_one()
        self.assertEqual('10', schedule['participant-phone'])

    @inlineCallbacks
    def test_consume_control_test_send_all_messages(self):
        self.initialize_properties()
        dialogue_id = self.collections['dialogues'].save(
            self.mkobj_dialogue_annoucement())
        self.collections['participants'].save(self.mkobj_participant('08'))
    
        event = self.mkmsg_dialogueworker_control(**{
            'action': 'test_send_all_messages',
            'dialogue_obj_id': str(dialogue_id),
            'phone_number': '08'})
        yield self.send(event, 'control')
    
        messages = self.broker.get_messages('vumi', 'test.outbound')
        self.assertEqual(len(messages), 1)
    
    @inlineCallbacks
    def test_consume_control_update_keywords(self):
        self.initialize_properties()
        self.broker.dispatched = {}
    
        event = self.mkmsg_dialogueworker_control(**{
            'action': 'update_registered_keywords'})
        yield self.send(event, 'control')
    
        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(len(messages), 1)
    
    @inlineCallbacks
    def test_consume_control_reload_request(self):
        self.initialize_properties()
        self.broker.dispatched = {}
    
        join_id = self.collections['requests'].save(self.mkobj_request_join())
    
        event = self.mkmsg_dialogueworker_control(**{
            'action': 'reload_request', 'object_id': str(join_id)})
        yield self.send(event, 'control')
        
        messages = self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['action'], 'add_exposed')
        self.assertEqual(messages[0]['exposed_name'], 'test')
        self.assertEqual(messages[0]['rules'], [{
            'app': 'test', 'keyword':'www', 'prefix':'+256', 'to_addr': '8181'}])

    @inlineCallbacks
    def test_consume_control_reload_program_settings(self):
        self.initialize_properties()
    
        program_setting = self.collections['program_settings'].find_one({'key': 'timezone'})
        program_setting['value'] = 'Europe/Paris'
        self.collections['program_settings'].save(program_setting)
    
        event = self.mkmsg_dialogueworker_control(**{
            'action': 'reload_program_settings'})
        yield self.send(event, 'control')
    
        self.assertEqual(self.worker.properties['timezone'], 'Europe/Paris')
    
    @inlineCallbacks
    def test_consume_control_badly_formated(self):
        self.initialize_properties()
    
        program_setting = self.collections['program_settings'].find_one({'key': 'timezone'})
        program_setting['value'] = 'Europe/Paris'
        self.collections['program_settings'].save(program_setting)
    
        event = Message(**{'action': 'reload-program_settings'})
        yield self.send(event, 'control')
        self.assertEqual(self.worker.properties['timezone'], 'Africa/Kampala')
