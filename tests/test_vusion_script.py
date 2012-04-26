from twisted.trial.unittest import TestCase

from vusion.vusion_script import VusionScript


class VusionScriptTestCase(TestCase):

    question_answer = {
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

    other_question_answer = {
        "dialogues": [
            {"name": "something",
             "interactions": [
                  {"type-schedule": "immediately",
                   "type-interaction": "question-answer",
                   "content": "How are you [participant.name]?",
                   "keyword": "Fool",
                   "type-reminder": "no-reminder",
                   "type-question": "close-question",
                   "answers": [
                       {"choice": "Good",
                        "feedbacks": [
                            { "content": "So have a nice day [participant.name]" }
                        ]},
                       {"choice": "Bad",
                        "feedbacks": [
                            { "content": "Come one [participant.name], you can get over it!" }
                        ]}
                       ],
                   "interaction-id": "script.dialogues[0].interactions[0]"
                   },
                  {"type-schedule": "immediately",
                   "type-interaction": "question-answer",
                   "content": "What is your gender?",
                   'label-for-participant-profiling': 'gender',
                   "keyword": "GEN",
                   "answers": [
                       {"choice": "Male"},
                       {"choice": "Bad"}
                       ],
                   "interaction-id": "script.dialogues[0].interactions[2]"
                   },
                  ],
             "dialogue-id": "script.dialogues[0]" }
        ]
    }

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_matchin_question_answer(self):
        script = VusionScript(self.question_answer)

        self.assertEqual(script.get_matching_question_answer("feel 1"),
                         {'dialogue-id': '01',
                          'interaction-id': '01-01',
                          'matching-answer': 'Fine',
                          'label-for-participant-profiling': None,
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
             'label-for-participant-profiling': None,
             'feedbacks': None})

        self.assertEqual(script.get_matching_question_answer("HOW good"),
                         {'dialogue-id': '01',
                          'interaction-id': '01-02',
                          'matching-answer': None,
                          'feedbacks': [
                              {'content':'thank you for this answer'}]})

        script = VusionScript(self.other_question_answer)
        self.assertEqual(script.get_matching_question_answer("something good"),
                         None)
        
        self.assertEqual(script.get_matching_question_answer('Gen Male'), 
                         {'dialogue-id': 'script.dialogues[0]',
                          'interaction-id': 'script.dialogues[0].interactions[2]',
                          'matching-answer': 'Male',
                          'label-for-participant-profiling': 'gender',
                          'feedbacks': None})
        
