from twisted.trial.unittest import TestCase

from vusion.persist import Dialogue
from vusion.action import (FeedbackAction, UnMatchingAnswerAction,
                           ProfilingAction, OffsetConditionAction,
                           RemoveRemindersAction, RemoveDeadlineAction,
                           RemoveQuestionAction, Actions)
from vusion.context import Context

from tests.utils import ObjectMaker


class TestDialogue(TestCase, ObjectMaker):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_as_dict(self):
        dialogue = Dialogue(**self.mkobj_dialogue_open_question())
        dialogue_as_dict = dialogue.get_as_dict()
        self.assertTrue('model-version' in dialogue_as_dict['interactions'][0])

    def test_get_matching_failed_dialogues_empty_interaction(self):
        dialogue = self.mkobj_dialogue_open_question()
        dialogue['interactions'] = None
        dialogue_helper = Dialogue(**dialogue)

        context = Context()
        actions = Actions()
        dialogue_helper.get_matching_reference_and_actions(
            "feel 1", actions, context)
        self.assertFalse(context.is_matching())
        self.assertEqual(len(actions), 0)

    def test_get_matching_closed_question_answer_actions(self):
        script = Dialogue(**self.mkobj_dialogue_question_answer())

        context = Context()
        actions = Actions()        
        script.get_matching_reference_and_actions("feel 1", actions, context)
        self.assertEqual(context['matching-answer'], 'Fine')
        self.assertEqual(len(actions), 3)
        self.assertEqual(
            actions[0],
            RemoveQuestionAction(**{'dialogue-id': '01',
                                    'interaction-id': '01-01'}))
        self.assertEqual(
            actions[1], 
            FeedbackAction(**{'content': 'thank you'}))
        self.assertEqual(
            actions[2],
            FeedbackAction(**{'content': 'thank you again'}))

        context = Context()
        actions = Actions()
        script.get_matching_reference_and_actions("feel 0", actions, context)
        self.assertEqual(context['matching-answer'], None)
        self.assertEqual(len(actions), 2)
        self.assertEqual(
            actions[0],
            RemoveQuestionAction(**{'dialogue-id': '01',
                                    'interaction-id': '01-01'}))
        self.assertEqual(
            actions[1],
            UnMatchingAnswerAction(**{'answer': 'feel 0'}))

        context = Context()
        actions = Actions()        
        script.get_matching_reference_and_actions("feel 3", actions, context)
        self.assertEqual(context['matching-answer'], None)
        self.assertEqual(len(actions), 2)

        context = Context()
        actions = Actions()        
        script.get_matching_reference_and_actions("feel ok", actions, context)
        self.assertEqual(context['matching-answer'], 'Ok')
        self.assertEqual(len(actions), 1)

        context = Context()
        actions = Actions()                
        script.get_matching_reference_and_actions("fel ok", actions, context)
        self.assertEqual(context['matching-answer'], 'Ok')
        self.assertEqual(len(actions), 1)

        script = Dialogue(**self.dialogue_other_question_answer)
        context = Context()
        actions = Actions()        
        script.get_matching_reference_and_actions("something good", actions, context)

        self.assertFalse(context.is_matching())
        self.assertEqual(len(actions), 0)

        context = Context()
        actions = Actions()        
        script.get_matching_reference_and_actions('Gen Male', actions, context)
        self.assertEqual(context['matching-answer'], 'Male')
        self.assertEqual(len(actions), 2)
        self.assertEqual(
            actions[1],
            ProfilingAction(**{'label': 'gender', 'value': 'Male'}))

    ## TODO: add a test with double word choice    
    def test_get_matching_closed_question_answer_no_space(self):
        dialogue_raw = self.mkobj_dialogue_answer_not_space_supported()
        dialogue_raw['interactions'][1]['answers'].append({'choice': 'third sex'})
        dialogue = Dialogue(**dialogue_raw)

        # test close question no space
        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("genMale", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(
            context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'Male')

        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("genthirdsex", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(
            context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'third sex')        
        
        # test close question no space with extra message content
        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("genMale and other stuff", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(
            context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'Male')        

    ## TODO: add a test with double word choice
    def test_get_matching_closed_question_answer_with_space(self):
        dialogue = Dialogue(**self.mkobj_dialogue_answer_not_space_supported())

        # test close question space
        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("gen Male", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(
            context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'Male')

        # test close question space with extra message content
        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("gen Male and proud", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(
            context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'Male')

    def test_get_matching_closed_question_answer_with_index(self):
        dialogue = Dialogue(**self.mkobj_dialogue_answer_not_space_supported())

        # test close question using index
        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("gen 1", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(
            context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'Male')

        # test close question using index with extra message
        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("gen 1 and proud", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(
            context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'Male')
        
        # test index matching one of the choice
        dialogue = Dialogue(**self.mkobj_dialogue_closed_question_index())
        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("choice 1", actions, context)
        self.assertEqual(context['dialogue-id'], '01')
        self.assertEqual(
            context['interaction-id'], '01-01')
        self.assertEqual(context['matching-answer'], '1')        


    def test_get_matching_closed_question_answer_fail(self):
        dialogue = Dialogue(**self.mkobj_dialogue_answer_not_space_supported())
        
        # test non matching closed question
        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("Genok", actions, context)
        self.assertFalse(context.is_matching())

    def test_get_matching_open_question(self):
        script = Dialogue(**self.mkobj_dialogue_question_answer())

        context = Context()
        actions = Actions()
        script.get_matching_reference_and_actions("name john doe", actions, context)
        self.assertEqual(context['dialogue-id'], '01')
        self.assertEqual(context['interaction-id'], '01-02')
        self.assertEqual(context['matching-answer'], "john doe")

        self.assertEqual(len(actions), 3)
        self.assertEqual(
            actions[0],
            RemoveQuestionAction(**{'dialogue-id': '01',
                                    'interaction-id': '01-02'}))
        self.assertEqual(
            actions[2],
            FeedbackAction(**{'content': 'thank you for this answer'}))
        self.assertEqual(
            actions[1],
            ProfilingAction(**{'label': 'name', 'value': 'john doe'}))

        context = Context()
        actions = Actions()
        script.get_matching_reference_and_actions("name", actions, context)
        self.assertEqual(context['dialogue-id'], '01')
        self.assertEqual(context['interaction-id'], '01-02')
        self.assertEqual(context['matching-answer'], None)

        self.assertEqual(len(actions), 2)
        self.assertEqual(
            actions[1],
            UnMatchingAnswerAction(**{'answer': ''}))

    def test_get_matching_question_multi_keyword(self):
        dialogue = Dialogue(**self.mkobj_dialogue_question_multi_keyword())

        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("Male", actions, context)
        self.assertEqual(context['dialogue-id'], '05')
        self.assertEqual(context['interaction-id'], '05')
        self.assertEqual(context['matching-answer'], 'maLe')

        self.assertEqual(len(actions), 2)
        self.assertEqual(
            actions[0],
            RemoveQuestionAction(**{'dialogue-id': '05',
                                    'interaction-id': '05'}))
        self.assertEqual(
            actions[1],
            ProfilingAction(**{'label': 'gender','value': 'maLe'}))
        
        # test with extra content
        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("Male and proud", actions, context)
        self.assertEqual(context['dialogue-id'], '05')
        self.assertEqual(context['interaction-id'], '05')
        self.assertEqual(context['matching-answer'], 'maLe')        

    def test_get_all_keywords_empty_interactions(self):
        dialogue = self.mkobj_dialogue_open_question()
        dialogue['interactions'] = None
        dialogue_helper = Dialogue(**dialogue)

        self.assertEqual(
            dialogue_helper.get_all_keywords(),
            [])

    def test_get_all_keywords(self):
        dialogue_helper = Dialogue(**self.mkobj_dialogue_question_answer())

        self.assertEqual(
            dialogue_helper.get_all_keywords(),
            ['feel', 'fel', 'name'])

        dialogue_helper = Dialogue(**self.mkobj_dialogue_answer_not_space_supported())

        self.assertEqual(
            dialogue_helper.get_all_keywords(),
            ['fool', 'gen', 'genmale', 'genbad'])

    def test_get_all_keywords_question_multi_keyword(self):
        dialogue_helper = Dialogue(**self.mkobj_dialogue_question_multi_keyword())
        self.assertEqual(dialogue_helper.get_all_keywords(), ['male', 'female'])

    def test_get_offset_condition_action(self):
        script = Dialogue(**self.mkobj_dialogue_question_offset_conditional())
        context = Context()
        actions = Actions()
        script.get_matching_reference_and_actions("feel 1", actions, context)

        self.assertEqual(
            actions[1],
            OffsetConditionAction(**{'interaction-id': '01-02',
                                     'dialogue-id': '01'}))

        self.assertEqual(
            actions[2],
            OffsetConditionAction(**{'interaction-id': '01-03',
                                     'dialogue-id': '01'}))

    def test_get_remove_reminders_action(self):
        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder_offset_time())
        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("name", actions, context)
        self.assertEqual(2, len(actions))
        self.assertEqual(
            actions[1],
            UnMatchingAnswerAction(**{'answer': ''}))

        context = Context()
        actions = Actions()
        dialogue.get_matching_reference_and_actions("name John", actions, context)
        self.assertEqual(
            actions[1],
            RemoveRemindersAction(**{'dialogue-id': '04',
                                     'interaction-id': '01-01'}))
        self.assertEqual(
            actions[2],
            RemoveDeadlineAction(**{'dialogue-id': '04',
                                    'interaction-id': '01-01'}))
        self.assertEqual(
            actions[3],
            ProfilingAction(**{'value': 'John',
                               'label': 'name'}))
        
    def test_upgrade_dialogue_to_current(self):
        dialogue = {
        'name': 'test dialogue',
        'activated': 1,
        'auto-enrollment': 'all',
        'dialogue-id': '0',
        'interactions': [
            {'activated': 0,
             'type-interaction': 'announcement',
             'interaction-id': '0',
             'content': 'Hello',
             'type-schedule': 'offset-days',
             'days': '1',
             'at-time': '22:30'},
            {'activated': 0,
             'type-interaction': 'announcement',
             'interaction-id': '1',
             'content': 'How are you',
             'type-schedule': 'offset-days',
             'days': '2',
             'at-time': '22:30'}]}
        
        dialogue = Dialogue(**dialogue)
        
        self.assertTrue(dialogue is not None)
        self.assertEqual(Dialogue.MODEL_VERSION, dialogue['model-version'])
