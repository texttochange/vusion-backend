from twisted.trial.unittest import TestCase

from vusion.dialogue import Dialogue


class VusionScriptTestCase(TestCase):

    question_answer = {
        'dialogue-id': '01',
        'interactions': [
            {
                'interaction-id': '01-01',
                'type-interaction': 'question-answer',
                "content": 'How are you?',
                'keyword': 'FEEL, Fel',
                'type-question': 'closed-question',
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
                "content": 'What is your name?',
                'keyword': 'name',
                'type-question': 'open-question',
                'answer-label': 'name',
                'feedbacks': [
                         {'content':'thank you for this answer'}],
                'type-schedule': 'immediately'
            }
        ]
    }

    other_question_answer = {
        "name": "something",
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
                        {"content": "So have a nice day [participant.name]"}
                    ]},
                   {"choice": "Bad",
                    "feedbacks": [
                        {"content": "Come one [participant.name], you can get over it!"}
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
         "dialogue-id": "script.dialogues[0]"
         }

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_matchin_question_answer(self):
        script = Dialogue(self.question_answer)

        ref, actions = script.get_matching_reference_and_actions("feel 1", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-01',
                               'matching-answer': 'Fine'})
        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[0], {'type-action': 'feedback',
                                      'content': 'thank you'})
        self.assertEqual(actions[1], {'type-action': 'feedback',
                                      'content': 'thank you again'})

        ref, actions = script.get_matching_reference_and_actions("feel 0", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-01',
                               'matching-answer': None})
        self.assertEqual(len(actions), 0)

        ref, actions = script.get_matching_reference_and_actions("feel 3", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-01',
                               'matching-answer': None})
        self.assertEqual(len(actions), 0)

        ref, actions = script.get_matching_reference_and_actions("feel ok", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-01',
                               'matching-answer': 'Ok'})
        self.assertEqual(len(actions), 0)

        ref, actions = script.get_matching_reference_and_actions("fel ok", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-01',
                               'matching-answer': 'Ok'})
        self.assertEqual(len(actions), 0)

        ref, actions = script.get_matching_reference_and_actions("name john doe", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-02'})
        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[0], {'type-action': 'feedback',
                                      'content': 'thank you for this answer'})
        self.assertEqual(actions[1], {'type-action': 'profiling',
                                      'label': 'name',
                                      'value': 'john doe'})

        ref, actions = script.get_matching_reference_and_actions("name", [])
        self.assertEqual(ref, {'dialogue-id': '01',
                               'interaction-id': '01-02'})
        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[0], {'type-action': 'feedback',
                                      'content': 'thank you for this answer'})
        self.assertEqual(actions[1], {'type-action': 'profiling',
                                      'label': 'name',
                                      'value': ''})

        script = Dialogue(self.other_question_answer)
        ref, actions = script.get_matching_reference_and_actions("something good", [])

        self.assertEqual(ref, {})
        self.assertEqual(actions, [])

        ref, actions = script.get_matching_reference_and_actions('Gen Male', [])
        self.assertEqual(ref, {'dialogue-id': 'script.dialogues[0]',
                               'interaction-id': 'script.dialogues[0].interactions[2]',
                               'matching-answer': 'Male'})
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0], {'type-action': 'profiling',
                                      'label': 'gender',
                                      'value': 'Male'})

    def test_get_all_keywords(self):
        script = Dialogue(self.question_answer)

        self.assertTrue(script.get_all_keywords(),
                        ['feel', 'fel'])
