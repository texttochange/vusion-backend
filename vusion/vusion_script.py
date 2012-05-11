from vumi.utils import get_first_word


def split_keywords(keywords):
    return [k.lower() for k in (keywords or '').split(', ')]


class VusionScript:

    def __init__(self, dialogue):
        self.dialogue = dialogue

    def get_reply(self, content, delimiter=' '):
        return (content or '').partition(delimiter)[2]

    def split_keywords(self, keywords):
        return [k.lower() for k in (keywords or '').split(', ')]

    def get_matching_interaction(self, keyword):
        for interaction in self.dialogue['interactions']:
            if not interaction['type-interaction'] == 'question-answer':
                continue
            if keyword in self.split_keywords(interaction['keyword']):
                return self.dialogue['dialogue-id'], interaction
        return None, None

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
                        'label-for-participant-profiling': None,
                        'feedbacks': None}
            return {'dialogue-id': dialogue_id,
                    'interaction-id': interaction['interaction-id'],
                    'matching-answer': answer['choice'],
                    'label-for-participant-profiling': interaction['label-for-participant-profiling'] if 'label-for-participant-profiling' in interaction else None,
                    'feedbacks': answer['feedbacks'] if 'feedbacks' in answer
                    else None}
        else:
            return {
                'dialogue-id': dialogue_id,
                'interaction-id': interaction['interaction-id'],
                'matching-answer': None,
                'label-for-participant-profiling': None,
                'feedbacks': interaction['feedbacks'] if 'feedbacks' in interaction else None}

    def get_all_keywords(self):
        keywords = []
        for interaction in self.dialogue['interactions']:
            if 'keyword' in interaction:
                interaction_keywords = self.split_keywords(interaction['keyword'])
                for interaction_keyword in interaction_keywords:
                    keywords.append(interaction_keyword)
        return keywords
