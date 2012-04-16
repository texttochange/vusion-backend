from twisted.trial.unittest import TestCase

from vusion.vusion_script import VusionScript


class VusionScriptTestCase(TestCase):

    question_answer = {
        'shortcode': '8282',
        'dialogues': [
            {
                'dialogue-id': '01',
                'interactions': [
                    {
                        'interaction-id': '01-01',
                        'type-interaction': 'question-answer',
                        "content": 'How are you?',
                        'keyword': 'FEEL',
                        'answers': [
                            {'choice': 'Fine',
                             'feedbacks': [
                                 {'content':'thank you'},
                                 {'content':'thank you again'}]
                             },
                            {'choice': 'Ok'}
                            ],
                        'type-schedule': 'immediately'
                        },
                    {
                        'interaction-id': '01-02',
                        'type-interaction': 'question-answer',
                        "content": 'How are you?',
                        'keyword': 'HOW',
                        'answer-label': 'reply',
                        'feedbacks': [
                                 {'content':'thank you for this answer'}],
                        'type-schedule': 'immediately'
                    }
                ]
            }
        ]
    }

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_matchin_question_answer_id(self):
        script = VusionScript(self.question_answer)

        self.assertEqual(script.get_matching_question_answer("feel 1"),
                         {'dialogue-id': '01',
                          'interaction-id': '01-01',
                          'matching-answer': 'Fine',
                          'feedbacks': [{'content':'thank you'},
                                        {'content':'thank you again'}]})

        self.assertEqual(
            script.get_matching_question_answer("feel 0"),
            {'dialogue-id': '01',
             'interaction-id': '01-01',
             'matching-answer': None,
             'feedbacks': None})

        self.assertEqual(
            script.get_matching_question_answer("feel 3"),
            {'dialogue-id': '01',
             'interaction-id': '01-01',
             'matching-answer': None,
             'feedbacks': None})

        self.assertEqual(
            script.get_matching_question_answer("feel ok"),
            {'dialogue-id': '01',
             'interaction-id': '01-01',
             'matching-answer': 'Ok',
             'feedbacks': None})

        self.assertEqual(script.get_matching_question_answer("HOW good"),
                         {'dialogue-id': '01',
                          'interaction-id': '01-02',
                          'matching-answer': None,
                          'feedbacks': [
                              {'content':'thank you for this answer'}]})
