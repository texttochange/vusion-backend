from twisted.internet.defer import inlineCallbacks

from vumi.message import Message

from vusion.utils import time_to_vusion_format, time_from_vusion_format
from vusion.tests.test_dialogue_worker import DialogueWorkerTestCase
from vusion.persist import Participant


class DialogueWorkerTestCase_consumeControlMessage(DialogueWorkerTestCase):

    @inlineCallbacks
    def test_consume_control_update_schedule_dialogue(self):
        self.initialize_properties()
        self.app_helper.clear_all_dispatched()
        
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
    
        control = self.mkmsg_dialogueworker_control(**{
            'action':'update_schedule',
            'schedule_type': 'dialogue',
            'object_id': dialogue_1['dialogue-id']})
        yield self.dispatch_control(control)
        
        self.assertEqual(2, self.collections['schedules'].count())
        enrolled_participant = Participant(**self.collections['participants'].find_one())
        self.assertTrue(enrolled_participant.is_enrolled('0'))
    
        # the second dialogue is auto-enrollment = 'none'
        control = self.mkmsg_dialogueworker_control(**{
            'action':'update_schedule',
            'schedule_type': 'dialogue',
            'object_id': dialogue_2['dialogue-id']})
        yield self.dispatch_control(control)
        self.assertEqual(2, self.collections['schedules'].count())
    
    @inlineCallbacks
    def test_consume_control_update_schedule_unattached(self):
        self.initialize_properties()
        self.app_helper.clear_all_dispatched()
        
        dNow = self.worker.get_local_time()

        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='10'))
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='11', session_id=None))
        unattach = self.mkobj_unattach_message_1()
        unattach_id = self.collections['unattached_messages'].save(unattach)

        control = self.mkmsg_dialogueworker_control(**{
            'action':'update_schedule',
            'schedule_type': 'unattach',
            'object_id': str(unattach_id)})
        yield self.dispatch_control(control)
        self.assertEqual(1, self.collections['schedules'].count())

    @inlineCallbacks
    def test_consume_control_mass_tag(self):
        self.initialize_properties()
        self.app_helper.clear_all_dispatched()
       
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

        control = self.mkmsg_dialogueworker_control(**{
            'action':'mass_tag',
            'tag': 'mombasa',
            'selector': {'tags': 'geek'}})
        yield self.dispatch_control(control)
        self.assertEqual(1, self.collections['schedules'].count())

    @inlineCallbacks
    def test_consume_control_mass_tag_empty_selector(self):
        self.initialize_properties()
        dNow = self.worker.get_local_time()

        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='10', tags=['mombasa']))
        self.collections['participants'].save(
            self.mkobj_participant(participant_phone='11', tags=['mombasa']))
        unattach = self.mkobj_unattach_message(
            send_to_type='match',
            send_to_match_operator='all',
            send_to_match_conditions=['mombasa'])
        unattach_id = self.collections['unattached_messages'].save(unattach)

        control = self.mkmsg_dialogueworker_control(**{
            'action':'mass_tag',
            'tag': 'mombasa',
            'selector': None})
        yield self.dispatch_control(control)
        self.assertEqual(2, self.collections['schedules'].count())

    @inlineCallbacks
    def test_consume_control_mass_untag(self):
        self.initialize_properties()
        self.app_helper.clear_all_dispatched()
                        
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

        control = self.mkmsg_dialogueworker_control(**{
            'action':'mass_untag',
            'tag': 'geek'})
        yield self.dispatch_control(control)
        self.assertEqual(1, self.collections['schedules'].count())
        schedule = self.collections['schedules'].find_one()
        self.assertEqual('10', schedule['participant-phone'])

    @inlineCallbacks
    def test_consume_control_test_send_all_messages(self):
        self.initialize_properties()
        dialogue_id = self.collections['dialogues'].save(
            self.mkobj_dialogue_annoucement())
        self.collections['participants'].save(self.mkobj_participant('08'))
    
        control = self.mkmsg_dialogueworker_control(**{
            'action': 'test_send_all_messages',
            'dialogue_obj_id': str(dialogue_id),
            'phone_number': '08'})
        yield self.dispatch_control(control)
    
        messages = yield self.app_helper.get_dispatched_outbound()
        self.assertEqual(len(messages), 1)
    
    @inlineCallbacks
    def test_consume_control_update_keywords(self):
        self.initialize_properties()
        self.app_helper.clear_all_dispatched()
                
        control = self.mkmsg_dialogueworker_control(**{
            'action': 'update_registered_keywords'})
        yield self.dispatch_control(control)
    
        messages = yield self.wait_for_dispatched_dispatcher_control(1)
        self.assertEqual(len(messages), 1)
    
    @inlineCallbacks
    def test_consume_control_reload_request(self):
        self.initialize_properties()
        self.app_helper.clear_all_dispatched()
        
        join_id = self.collections['requests'].save(self.mkobj_request_join())
    
        control = self.mkmsg_dialogueworker_control(**{
            'action': 'reload_request', 'object_id': str(join_id)})
        yield self.dispatch_control(control)
        
        messages = yield self.wait_for_dispatched_dispatcher_control(1) #self.broker.get_messages('vumi', 'dispatcher.control')
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['action'], 'add_exposed')
        self.assertEqual(messages[0]['exposed_name'], 'sphex')
        self.assertEqual(messages[0]['rules'], [{
            'app': 'sphex', 'keyword':'www', 'prefix':'+256', 'to_addr': '8181'}])

    @inlineCallbacks
    def test_consume_control_reload_program_settings(self):
        self.initialize_properties()
    
        program_setting = self.collections['program_settings'].find_one({'key': 'timezone'})
        program_setting['value'] = 'Europe/Paris'
        self.collections['program_settings'].save(program_setting)
    
        control = self.mkmsg_dialogueworker_control(**{
            'action': 'reload_program_settings'})       
        yield self.dispatch_control(control)
        self.assertEqual(self.worker.properties['timezone'], 'Europe/Paris')
    
    #@inlineCallbacks
    #def test_consume_control_badly_formated(self):
        #self.initialize_properties()
    
        #program_setting = self.collections['program_settings'].find_one({'key': 'timezone'})
        #program_setting['value'] = 'Europe/Paris'
        #self.collections['program_settings'].save(program_setting)
    
        #event = Message(**{'action': 'reload-program_settings'})
        #yield self.send(event, 'control')
        #self.assertEqual(self.worker.properties['timezone'], 'Africa/Kampala')

    @inlineCallbacks
    def test_consume_control_run_actions(self):
        self.initialize_properties()
        
        dialogue_01 = self.mkobj_dialogue_question_offset_days()
        self.collections['dialogues'].save(dialogue_01)
        dialogue_02 = self.mkobj_dialogue_announcement_offset_days()
        self.collections['dialogues'].save(dialogue_02)

        #save participant
        self.collections['participants'].save(
            self.mkobj_participant(
                participant_phone='+06'))

        control = self.mkmsg_dialogueworker_control(**{
            'action':'run_actions',
            'participant_phone': '+06',
            'dialogue_id': dialogue_01['dialogue-id'],
            'interaction_id': dialogue_01['interactions'][0]['interaction-id'],
            'answer': 'ok'})
        yield self.dispatch_control(control)

        saved_participant = self.collections['participants'].find_one({'phone': '+06'})
        self.assertEqual(saved_participant['enrolled'][0]['dialogue-id'], '0')
        self.assertEqual(2, self.collections['schedules'].count())
