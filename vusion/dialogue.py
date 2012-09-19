from vumi import log
from vumi.utils import get_first_word
from vusion.action import (UnMatchingAnswerAction, FeedbackAction,
                           action_generator, ProfilingAction,
                           OffsetConditionAction, RemoveRemindersAction)

def split_keywords(keywords):
    return [k.lower() for k in (keywords or '').split(', ')]


class Dialogue:

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

    def get_offset_condition_interactions(self, interaction_id):
        offset_condition_interactions = []
        for interaction in self.dialogue['interactions']:
            if (interaction['type-schedule'] == 'offset-condition' and
                    interaction['offset-condition-interaction-id'] == interaction_id):
                offset_condition_interactions.append(interaction['interaction-id'])
        return offset_condition_interactions
    
    def get_remove_reminders_action(self, interaction):
        if 'set-reminder' in interaction:
            return interaction
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

    def get_matching_reference_and_actions(self, message, actions):
        reference_metadata = None
        keyword = get_first_word(message).lower()
        reply = self.get_reply(message).lower()
        dialogue_id, interaction = self.get_matching_interaction(keyword)

        if not interaction:
            return reference_metadata, actions

        reference_metadata = {
            'dialogue-id': dialogue_id,
            'interaction-id': interaction['interaction-id']}
        interaction_to_remove_reminders = self.get_remove_reminders_action(interaction)
        if interaction_to_remove_reminders is not None:
            actions.append(RemoveRemindersAction(**{
                'dialogue-id': dialogue_id,
                'interaction-id':interaction['interaction-id']}))
        if 'answers' in interaction:
            answer = self.get_matching_answer(interaction['answers'], reply)
            if not answer or answer is None:
                reference_metadata['matching-answer'] = None
                actions.append(UnMatchingAnswerAction(**{'answer': reply}))
            else:
                reference_metadata['matching-answer'] = answer['choice']
                actions = self.add_feedback_action(actions, answer)
                if 'label-for-participant-profiling' in interaction:
                    action = ProfilingAction(**{
                        'label': interaction['label-for-participant-profiling'],
                        'value': answer['choice']})
                    actions.append(action)
                if 'answer-actions' in answer:
                    for answer_action in answer['answer-actions']:
                        actions.append(action_generator(**answer_action))
        else:
            actions = self.add_feedback_action(actions, interaction)
            if 'answer-label' in interaction:
                actions.append(ProfilingAction(**{
                    'label': interaction['answer-label'],
                    'value': self.get_open_answer(message)}))        
        # Check if offset condition on this answer
        for interaction_to_schedule in self.get_offset_condition_interactions(interaction['interaction-id']):
            actions.append(OffsetConditionAction(**{
                'dialogue-id': dialogue_id,
                'interaction-id': interaction_to_schedule}))
        return reference_metadata, actions

    def get_open_answer(self, message):
        words = (message or '').split(' ')
        return " ".join(words[1:])

    def add_feedback_action(self, actions, obj):
        if 'feedbacks' in obj:
            for feedback in obj['feedbacks']:
                action = FeedbackAction(**{'content': feedback['content']})
                actions.append(action)
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
