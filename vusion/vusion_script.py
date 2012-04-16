from vumi.utils import get_first_word


class VusionScript:

    def __init__(self, script):
        self.script = script

    def get_reply(self, content, delimiter=' '):
        return (content or '').partition(delimiter)[2]

    def get_matching_interaction(self, keyword):
        for dialogue in self.script['dialogues']:
            for interaction in dialogue['interactions']:
                if not interaction['type-interaction'] == 'question-answer':
                    continue
                if interaction['keyword'].lower() == keyword:
                    return dialogue['dialogue-id'], interaction
        return None

    def get_matching_answer(self, answers, reply):
        try:
            index = int(reply) - 1
            if index < 0 or index > len(answers):
                return None
            return answers[index]
        except:
            pass
        for answer in answers:
            if answer['choice'].lower() == reply:
                return answer
        return None

    def get_matching_question_answer(self, message):
        keyword = get_first_word(message).lower()
        reply = self.get_reply(message).lower()
        dialogue_id, interaction = self.get_matching_interaction(keyword)
        if not interaction:
            return None
        if 'answers' in interaction:
            answer = self.get_matching_answer(interaction['answers'], reply)
            if not answer:
                return {'dialogue-id': dialogue_id,
                        'interaction-id': interaction['interaction-id'],
                        'matching-answer': None,
                        'feedbacks': None}
            return {'dialogue-id': dialogue_id,
                    'interaction-id': interaction['interaction-id'],
                    'matching-answer': answer['choice'],
                    'feedbacks': answer['feedbacks'] if 'feedbacks' in answer
                    else None}
        else:
            return {
                'dialogue-id': dialogue_id,
                'interaction-id': interaction['interaction-id'],
                'matching-answer': None,
                'feedbacks': interaction['feedbacks']}
