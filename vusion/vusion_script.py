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
        if not 'interactions' in self.dialogue:
            return None, None
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

    def get_matching_reference_and_actions(self, message, actions):
        reference_metadata = {}
        keyword = get_first_word(message).lower()
        reply = self.get_reply(message).lower()
        dialogue_id, interaction = self.get_matching_interaction(keyword)

        if not interaction:
            return reference_metadata, actions

        reference_metadata = {
            'dialogue-id': dialogue_id,
            'interaction-id': interaction['interaction-id']}
        if 'answers' in interaction:
            answer = self.get_matching_answer(interaction['answers'], reply)
            if not answer:
                reference_metadata['matching-answer'] = None
            else:
                reference_metadata['matching-answer'] = answer['choice']
                actions = self.add_feedback_action(actions, answer)
                if 'label-for-participant-profiling' in interaction:
                    actions.append(
                        {'type-action': 'profiling',
                         'label': interaction['label-for-participant-profiling'],
                         'value': answer['choice']})
                if 'answer-actions' in answer:
                    for answer_action in answer['answer-actions']:
                        action = answer_action
                        action['type-action'] = answer_action['type-answer-action']
                        actions.append(action)
        else:
            actions = self.add_feedback_action(actions, interaction)
            if 'answer-label' in interaction:
                actions.append(
                    {'type-action': 'profiling',
                     'label': interaction['answer-label'],
                     'value': self.get_open_answer(message)})
        return reference_metadata, actions

    def get_open_answer(self, message):
          words = (message or '').split(' ')
          return " ".join(words[1:])
        
    def add_feedback_action(self, actions, obj):
        if 'feedbacks' in obj:
            for feedback in obj['feedbacks']:
                actions.append(
                    {'type-action': 'feedback',
                     'content': feedback['content']})
        return actions

    def get_all_keywords(self):
        keywords = []
        if not 'interactions' in self.dialogue:
            return keywords
        for interaction in self.dialogue['interactions']:
            if 'keyword' in interaction:
                interaction_keywords = self.split_keywords(interaction['keyword'])
                for interaction_keyword in interaction_keywords:
                    keywords.append(interaction_keyword)
        return keywords
