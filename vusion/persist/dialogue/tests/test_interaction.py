#encoding=utf-8
"""Tests for vusion.persist.interaction."""

from twisted.trial.unittest import TestCase

from datetime import timedelta

from vusion.error import FailingModelUpgrade
from vusion.persist import Interaction
from vusion.persist.action import Actions
from vusion.utils import time_from_vusion_format

from tests.utils import ObjectMaker


class TestInteraction(TestCase, ObjectMaker):

    def test_validation_announcement(self):
        dialogue = self.mkobj_dialogue_annoucement()
        interaction = Interaction(**dialogue['interactions'][0])
        self.assertTrue(interaction is not None)

    def test_validation_open_question(self):
        dialogue = self.mkobj_dialogue_open_question()
        interaction = Interaction(**dialogue['interactions'][0])
        self.assertTrue(interaction is not None)
        self.assertEqual(interaction['feedbacks'], [])

    def test_validation_closed_question(self):
        dialogue = self.dialogue_question
        interaction = Interaction(**dialogue['interactions'][0])
        self.assertTrue(interaction is not None)

    def test_validation_multi_keyword_question(self):
        dialogue = self.mkobj_dialogue_question_multi_keyword()
        interaction = Interaction(**dialogue['interactions'][0])
        self.assertTrue(interaction is not None)

    def test_upgrade_question_1_to_current(self):
        interaction = {
            'activated': 1,
            'interaction-id': '01-01',
            'type-interaction': 'question-answer',
            'content': 'What is your name?',
            'keyword': 'name',
            'set-use-template': 'use-template',
            'type-question': 'open-question',
            'answer-label': 'name',
            'type-schedule': 'offset-days',
            'days': '1',
            'at-time': '22:30'}

        interaction = Interaction(**interaction)

        self.assertTrue(interaction is not None)
        self.assertEqual(Interaction.MODEL_VERSION, interaction['model-version'])

    def test_get_actions_from_matching_answer_multi_keyword(self):
        dialogue = self.mkobj_dialogue_question_multi_keyword()
        interaction = Interaction(**dialogue['interactions'][0])

        actions = Actions()
        matching_answer = interaction.get_matching_answer_keyword(
            interaction['answer-keywords'], "male")
        interaction.get_actions_from_interaction(
            dialogue['dialogue-id'],
            'male',
            actions)
        interaction.get_actions_from_matching_answer(
            dialogue['dialogue-id'],
            matching_answer,
            'male',
            actions)
        self.assertEqual(1, len(actions))
        self.assertEqual('profiling', actions[0].get_type())

    def test_get_actions_from_matching_answer_closed_question(self):
        interaction = self.mkobj_interaction_question_answer()
        interaction = Interaction(**interaction)

        actions = Actions()
        matching_answer = interaction.get_matching_answer("feel", "bad")
        interaction.get_actions_from_matching_answer(
            'dialogue-id',
            matching_answer,
            'ok',
            actions)
        self.assertEqual(1, len(actions))
        self.assertEqual('feedback', actions[0].get_type())

    def test_get_matching_answer_closed_question_index(self):
        interaction = self.mkobj_interaction_question_answer()
        interaction['answers'] = []
        for i in range(0, 16):
            interaction['answers'].append({'choice': '%s' % i, 'answer-actions':[], 'feedbacks':[]})
        interaction = Interaction(**interaction)        

        matching_answer = interaction.get_matching_answer("feel", "10") 
        self.assertEqual(matching_answer['choice'], '10')

        matching_answer = interaction.get_matching_answer("feel", "1") 
        self.assertEqual(matching_answer['choice'], '1')

        matching_answer = interaction.get_matching_answer("feel", "20") 
        self.assertEqual(matching_answer, None)

    def test_get_matching_answer_closed_question_unsensitive(self):
        interaction = Interaction(**self.mkobj_interaction_question_answer())

        matching_answer = interaction.get_matching_answer("feel", "good") 
        self.assertEqual(matching_answer['choice'], 'Good')

        matching_answer = interaction.get_matching_answer("feel", "bad") 
        self.assertEqual(matching_answer['choice'], 'Bâd')

        matching_answer = interaction.get_matching_answer("feel", "bad headarch") 
        self.assertEqual(matching_answer['choice'], 'Bâd')
        
        ## choice can have multiple words
        interaction['answers'][1]['choice'] = "Bâd heâdarch"
        
        matching_answer = interaction.get_matching_answer("feel", "bad") 
        self.assertEqual(matching_answer, None)
        
        matching_answer = interaction.get_matching_answer("feel", "bad headarch") 
        self.assertEqual(matching_answer['choice'], "Bâd heâdarch")
        
        matching_answer = interaction.get_matching_answer("feel", "bad headarch this morning") 
        self.assertEqual(matching_answer['choice'], "Bâd heâdarch")

    def test_get_matching_answer_closed_question_unsensitive_no_space(self):
        interaction = self.mkobj_interaction_question_answer()
        interaction['set-answer-accept-no-space'] = 'answer-accept-no-space'
        interaction = Interaction(**interaction)
        
        matching_answer = interaction.get_matching_answer("feel", "good") 
        self.assertEqual(matching_answer['choice'], 'Good')

        matching_answer = interaction.get_matching_answer("feelgood", None) 
        self.assertEqual(matching_answer['choice'], 'Good')        
        
        matching_answer = interaction.get_matching_answer("feel", "bad") 
        self.assertEqual(matching_answer['choice'], 'Bâd')
        
        matching_answer = interaction.get_matching_answer("feelbad", None) 
        self.assertEqual(matching_answer['choice'], 'Bâd')
    
        matching_answer = interaction.get_matching_answer("feel", "bad headarch") 
        self.assertEqual(matching_answer['choice'], 'Bâd')

        ## Cannot append on production as 
        ## this keyword would not be registered on the dispatcher
        matching_answer = interaction.get_matching_answer("feelbadheadarch", None) 
        self.assertEqual(matching_answer, None)
        
        ## choice can have multiple words
        interaction['answers'][1]['choice'] = "Bâd heâdarch"
        
        matching_answer = interaction.get_matching_answer("feelbad", None) 
        self.assertEqual(matching_answer, None)
        
        matching_answer = interaction.get_matching_answer("feelbadheadarch", None) 
        self.assertEqual(matching_answer['choice'], "Bâd heâdarch")

        ## Cannot append on production as 
        ## this keyword would not be registered on the dispatcher            
        matching_answer = interaction.get_matching_answer("feelbadheadarchthismorning", None) 
        self.assertEqual(matching_answer, None)


    def test_get_actions_from_matching_answer_open_question(self):                
        dialogue = self.mkobj_dialogue_open_question()
        interaction = Interaction(**dialogue['interactions'][0])

        actions = Actions()
        interaction.get_actions_from_interaction(
            dialogue['dialogue-id'],
            'olivier',
            actions)
        self.assertEqual(1, len(actions))
        self.assertEqual('profiling', actions[0].get_type())

    def test_get_offset_time_delta(self):
        dialogue = self.mkobj_dialogue_announcement_offset_time()
        interaction = Interaction(**dialogue['interactions'][0])
        
        self.assertEqual(
            timedelta(seconds=10),
            interaction.get_offset_time_delta())
        
        interaction['minutes'] = "10:10"
        self.assertEqual(
                    timedelta(minutes=10, seconds=10),
                    interaction.get_offset_time_delta())

        interaction['minutes'] = "10"
        self.assertEqual(
            timedelta(minutes=10),
            interaction.get_offset_time_delta())

    def test_get_reminder_times_offset_time(self):
        dialogue = self.mkobj_dialogue_open_question_reminder_offset_time()
        interaction_offset_time = Interaction(**dialogue['interactions'][0])
        interaction_time = time_from_vusion_format('2012-04-04T09:00:00')
        
        reminder_times = interaction_offset_time.get_reminder_times(interaction_time)
        
        self.assertEqual(2, len(reminder_times))
        self.assertEqual(time_from_vusion_format('2012-04-04T09:30:00'), reminder_times[0])
        self.assertEqual(time_from_vusion_format('2012-04-04T10:00:00'), reminder_times[1])

    def test_get_reminder_times_offset_days(self):
        dialogue = self.mkobj_dialogue_open_question_reminder_offset_days()
        interaction_offset_time = Interaction(**dialogue['interactions'][0])
        interaction_time = time_from_vusion_format('2012-04-04T09:00:00')
        
        reminder_times = interaction_offset_time.get_reminder_times(interaction_time)
        
        self.assertEqual(2, len(reminder_times))
        self.assertEqual(time_from_vusion_format('2012-04-06T09:00:00'), reminder_times[0])
        self.assertEqual(time_from_vusion_format('2012-04-08T09:00:00'), reminder_times[1])

    def test_get_deadline_time_offset_time(self):
        dialogue = self.mkobj_dialogue_open_question_reminder_offset_time()
        interaction_offset_time = Interaction(**dialogue['interactions'][0])
        interaction_time = time_from_vusion_format('2012-04-04T09:00:00')
        
        deadline_time = interaction_offset_time.get_deadline_time(interaction_time)
        
        self.assertEqual(time_from_vusion_format('2012-04-04T10:30:00'), deadline_time)

    def test_get_deadline_time_offset_days(self):
        dialogue = self.mkobj_dialogue_open_question_reminder_offset_days()
        interaction_offset_time = Interaction(**dialogue['interactions'][0])
        interaction_time = time_from_vusion_format('2012-04-04T09:00:00')
        
        deadline_time = interaction_offset_time.get_deadline_time(interaction_time)
        
        self.assertEqual(time_from_vusion_format('2012-04-10T09:00:00'), deadline_time)

    def test_get_keywords_question_answer_nospace(self):
        interaction = Interaction(**self.mkobj_interaction_question_answer_nospace('GÉN'))
        self.assertEqual(
            interaction.get_keywords(),
            ['gen', 'genmale','genbad'])

    def test_get_keywords_question_multikeyword(self):
        interaction = Interaction(**self.mkobj_interaction_question_multikeyword())
        self.assertEqual(
            interaction.get_keywords(),
            ['male', 'female'])

    def test_get_keywords_question_answer(self):
        interaction = Interaction(**self.mkobj_interaction_question_answer())
        self.assertEqual(
            interaction.get_keywords(),
            ['feel'])
    