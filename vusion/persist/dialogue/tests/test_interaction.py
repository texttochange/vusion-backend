#encoding=utf-8
"""Tests for vusion.persist.interaction."""

from twisted.trial.unittest import TestCase

from datetime import timedelta

from vusion.error import FailingModelUpgrade
from vusion.persist import Interaction
from vusion.persist.action import Actions, ResetAction, EnrollingAction
from vusion.utils import time_from_vusion_format

from tests.utils import ObjectMaker


class TestInteraction(TestCase, ObjectMaker):

    def test_validation_announcement(self):
        dialogue = self.mkobj_dialogue_announcement()
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
        matching_answer = interaction.get_matching_answer_multikeyword(
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
        matching_answer = interaction.get_matching_answer_closed_question("feel", "bad")
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

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "10")
        self.assertEqual(matching_answer['choice'], '10')

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "1")
        self.assertEqual(matching_answer['choice'], '1')

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "20")
        self.assertEqual(matching_answer, None)

    def test_get_matching_answer_closed_question_unsensitive(self):
        interaction = Interaction(**self.mkobj_interaction_question_answer())

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "good")
        self.assertEqual(matching_answer['choice'], 'Good')

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "bad")
        self.assertEqual(matching_answer['choice'], 'Bâd')

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "bad headarch")
        self.assertEqual(matching_answer['choice'], 'Bâd')

        ## choice can have multiple words
        interaction['answers'][1]['choice'] = "Bâd heâdarch"

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "bad") 
        self.assertEqual(matching_answer, None)

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "bad headarch") 
        self.assertEqual(matching_answer['choice'], "Bâd heâdarch")

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "bad headarch this morning")
        self.assertEqual(matching_answer['choice'], "Bâd heâdarch")

    def test_get_matching_answer_closed_question_unsensitive_no_space(self):
        interaction = self.mkobj_interaction_question_answer()
        interaction['set-answer-accept-no-space'] = 'answer-accept-no-space'
        interaction = Interaction(**interaction)

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "good")
        self.assertEqual(matching_answer['choice'], 'Good')

        matching_answer = interaction.get_matching_answer_closed_question(
            "feelgood", None)
        self.assertEqual(matching_answer['choice'], 'Good')

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "bad")
        self.assertEqual(matching_answer['choice'], 'Bâd')

        matching_answer = interaction.get_matching_answer_closed_question(
            "feelbad", None)
        self.assertEqual(matching_answer['choice'], 'Bâd')

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel", "bad headarch")
        self.assertEqual(matching_answer['choice'], 'Bâd')

        ## Cannot append on production as
        ## this keyword would not be registered on the dispatcher
        matching_answer = interaction.get_matching_answer_closed_question(
            "feelbadheadarch", None)
        self.assertEqual(matching_answer, None)

        ##choice can have multiple words
        interaction['answers'][1]['choice'] = "Bâd heâdarch"

        matching_answer = interaction.get_matching_answer_closed_question(
            "feelbad", None)
        self.assertEqual(matching_answer, None)

        matching_answer = interaction.get_matching_answer_closed_question(
            "feelbadheadarch", None)
        self.assertEqual(matching_answer['choice'], "Bâd heâdarch")

        ## Cannot append on production as
        ## this keyword would not be registered on the dispatcher
        matching_answer = interaction.get_matching_answer_closed_question(
            "feelbadheadarchthismorning", None)
        self.assertEqual(matching_answer, None)

        matching_answer = interaction.get_matching_answer_closed_question(
            "feel1", None)
        self.assertEqual(matching_answer['choice'], 'Good')

    def test_get_actions_from_matching_answer_open_question(self):
        dialogue = self.mkobj_dialogue_open_question()
        interaction = Interaction(**dialogue['interactions'][0])

        actions = Actions()
        interaction.get_actions_from_interaction(
            dialogue['dialogue-id'],
            'olivier',
            actions)
        self.assertEqual(2, len(actions))
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
            ['gen', 'genmale', 'gen1', 'genbad', 'gen2'])

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

    def test_get_answer_keywords(self):
        interaction = Interaction(**self.mkobj_interaction_question_answer())
        answer_keywords = interaction.get_answer_keywords_accept_no_space(
            ['feel'],
            {'choice': 'Good'},
            1)
        self.assertEqual(answer_keywords, ['feelgood', 'feel1'])
        
    def test_get_answer_keywords_two_keywords(self):
        interaction = Interaction(**self.mkobj_interaction_question_answer())
        answer_keywords = interaction.get_answer_keywords_accept_no_space(
            ['gen', 'gender'],
            {'choice': 'male'},
            1)
        self.assertEqual(answer_keywords, ['genmale', 'gendermale', 'gen1', 'gender1'])

    def test_get_sending_actions(self):
        expectedActions = Actions()
        expectedActions.append(ResetAction())
        expectedActions.append(EnrollingAction(**{'enroll': '01'}))

        interaction = Interaction(**self.mkobj_interaction_announcement())
        actions = interaction.get_sending_actions()
        self.assertEqual(
            expectedActions, actions)
        
    def test_get_label_closed_question(self):
        interaction_closed_qn = self.mkobj_interaction_question_answer()
        interaction_closed_qn['label-for-participant-profiling'] = 'feels'
        interaction = Interaction(**interaction_closed_qn)
        label = interaction.get_label()        
        self.assertEqual(label, 'feels')
        
    def test_get_label_open_question(self):
        interaction_open_qn = self.mkobj_interaction_question_answer_open()
        interaction_open_qn['answer-label'] = 'greeting'
        interaction = Interaction(**interaction_open_qn)
        label = interaction.get_label()
        self.assertEqual(label, 'greeting')
        
    def test_get_label_question_multikeyword(self):
        interaction_qn_multi_keyword = self.mkobj_interaction_question_multikeyword()
        interaction = Interaction(**interaction_qn_multi_keyword)
        label = interaction.get_label()
        self.assertEqual(label, 'gender')        
