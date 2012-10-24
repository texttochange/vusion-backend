from twisted.trial.unittest import TestCase

from vusion.persist import Dialogue
from vusion.action import (FeedbackAction, UnMatchingAnswerAction,
                           ProfilingAction, OffsetConditionAction,
                           RemoveRemindersAction, RemoveDeadlineAction,
                           RemoveQuestionAction, Actions)

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
        
        context = {}
        actions = Actions()
        dialogue_helper.get_matching_reference_and_actions("feel 1", actions, context)
        self.assertEqual(context, {})
        self.assertEqual(len(actions), 0)

    def test_get_matching_closed_question_answer(self):
        script = Dialogue(**self.mkobj_dialogue_question_answer())

        ref, actions = script.get_matching_reference_and_actions("feel 1", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-01',
                               'matching-answer': 'Fine'})
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

        ref, actions = script.get_matching_reference_and_actions("feel 0", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-01',
                               'matching-answer': None})
        self.assertEqual(len(actions), 2)
        self.assertEqual(
            actions[0], 
            RemoveQuestionAction(**{'dialogue-id': '01',
                                    'interaction-id': '01-01'}))        
        self.assertEqual(
            actions[1],
            UnMatchingAnswerAction(**{'answer': '0'}))

        ref, actions = script.get_matching_reference_and_actions("feel 3", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-01',
                               'matching-answer': None})
        self.assertEqual(len(actions), 2)

        ref, actions = script.get_matching_reference_and_actions("feel ok", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-01',
                               'matching-answer': 'Ok'})
        self.assertEqual(len(actions), 1)

        ref, actions = script.get_matching_reference_and_actions("fel ok", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-01',
                               'matching-answer': 'Ok'})
        self.assertEqual(len(actions), 1)

        script = Dialogue(self.dialogue_other_question_answer)
        ref, actions = script.get_matching_reference_and_actions("something good", [])

        self.assertEqual(ref, None)
        self.assertEqual(actions, [])

        ref, actions = script.get_matching_reference_and_actions('Gen Male', [])
        self.assertEqual(ref, {'dialogue-id': 'script.dialogues[0]',
                               'interaction-id': 'script.dialogues[0].interactions[2]',
                               'matching-answer': 'Male'})
        self.assertEqual(len(actions), 2)
        self.assertEqual(
            actions[1],
            ProfilingAction(**{'label': 'gender', 'value': 'Male'}))

    def test_get_matching_closed_question_answer(self):
        dialogue = Dialogue(**self.mkobj_dialogue_answer_not_space_supported())

        context = {}
        actions = Actions()
        dialogue.get_matching_reference_and_actions("genMale", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'Male')

        context = {}
        actions = Actions()
        dialogue.get_matching_reference_and_actions("gen Male", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'Male')

        context = {}
        actions = Actions()        
        dialogue.get_matching_reference_and_actions("gen 1", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'Male')
       
        context = {}
        actions = Actions() 
        dialogue.get_matching_reference_and_actions("genBad", actions, context)
        self.assertEqual(context['dialogue-id'], 'script.dialogues[0]')
        self.assertEqual(context['interaction-id'], 'script.dialogues[0].interactions[2]')
        self.assertEqual(context['matching-answer'], 'Bad')
       
        context = {}
        actions = Actions() 
        dialogue.get_matching_reference_and_actions("Genok", actions, context)
        self.assertEqual(context, {})

    def test_get_matching_open_question(self):
        script = Dialogue(**self.mkobj_dialogue_question_answer())

        context = {}
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
            ProfilingAction(**{'label': 'name','value': 'john doe'}))

        context = {}
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

        context = {}
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
        self.assertEqual(dialogue_helper.get_all_keywords(),['male', 'female'])

    def test_get_offset_condition_action(self):
        script = Dialogue(**self.mkobj_dialogue_question_offset_conditional())
        actions = Actions()
        script.get_matching_reference_and_actions("feel 1", actions, {})
        
        self.assertEqual(
            actions[1],
            OffsetConditionAction(**{'interaction-id': '01-02',
                                     'dialogue-id': '01'}))

        self.assertEqual(
            actions[2],
            OffsetConditionAction(**{'interaction-id': '01-03',
                                     'dialogue-id': '01'})) 

    def test_get_remove_reminders_action(self):
        dialogue = Dialogue(**self.mkobj_dialogue_open_question_reminder())
        actions = Actions()
        dialogue.get_matching_reference_and_actions("name", actions, {})
        self.assertEqual(2, len(actions))
        self.assertEqual(
            actions[1],
            UnMatchingAnswerAction(**{'answer': ''}))
        
        actions = Actions()
        dialogue.get_matching_reference_and_actions("name John", actions, {})
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
        
  
