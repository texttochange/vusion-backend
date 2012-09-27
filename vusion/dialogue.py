from copy import copy

from vumi import log
from vumi.utils import get_first_word
from vusion.action import (UnMatchingAnswerAction, FeedbackAction,
                           action_generator, ProfilingAction,
                           OffsetConditionAction, RemoveRemindersAction,
                           RemoveDeadlineAction, RemoveQuestionAction)

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
            if interaction['type-interaction'] == 'question-answer-keyword':
                for answer_keyword in interaction['answer-keywords']:
                    if keyword in self.split_keywords(answer_keyword['keyword']):
                        return self.dialogue['dialogue-id'], interaction
            elif interaction['type-interaction'] == 'question-answer':
                if keyword in self.get_interaction_keywords(interaction):
                    return self.dialogue['dialogue-id'], interaction
        return None, None

    def get_interaction_keywords(self, interaction):
        keywords = self.split_keywords(interaction['keyword'])
        if (not 'set-answer-accept-no-space' in interaction 
                or interaction['set-answer-accept-no-space'] is None):
            return keywords
        generated_answer = copy(keywords)
        for answer in interaction['answers']:
            generated_answer += self.get_answer_keywords(keywords, answer)
        return generated_answer    

    def get_answer_keywords(self, keywords, answer):
        return [("%s%s" % (keyword, answer['choice'])).lower() for keyword in keywords]

    def get_offset_condition_interactions(self, interaction_id):
        offset_condition_interactions = []
        for interaction in self.dialogue['interactions']:
            if (interaction['type-schedule'] == 'offset-condition' and
                    interaction['offset-condition-interaction-id'] == interaction_id):
                offset_condition_interactions.append(interaction['interaction-id'])
        return offset_condition_interactions
    
    def has_reminders(self, interaction):
        if 'set-reminder' in interaction:
            return True
        return False

    def get_matching_answer(self, interaction, keyword, reply):
        answers = interaction['answers']
        if 'set-answer-accept-no-space' in interaction:
            keywords = self.split_keywords(interaction['keyword'])
            for answer in interaction['answers']:
                if keyword in self.get_answer_keywords(keywords, answer):
                    return answer
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

    def get_matching_answer_keyword(self, answer_keywords, message):
        try:
            index = int(message) - 1
            if index < 0 or index > len(answer_keywords):
                return None
            return answer_keywords[index]
        except:
            pass
        for answer_keyword in answer_keywords:
            if answer_keyword['keyword'].lower() == message:
                return answer_keyword
        return None

    def get_matching_reference_and_actions(self, message, actions):
        keyword = get_first_word(message).lower()
        reply = self.get_reply(message).lower()
        dialogue_id, interaction = self.get_matching_interaction(keyword)

        if not interaction:
            return None, actions

        reference_metadata = {
            'dialogue-id': dialogue_id,
            'interaction-id': interaction['interaction-id'],
            'matching-answer': None}
        actions.append(RemoveQuestionAction(**{
                'dialogue-id': dialogue_id,
                'interaction-id':interaction['interaction-id']}))
        if self.has_reminders(interaction):
            actions.append(RemoveRemindersAction(**{
                'dialogue-id': dialogue_id,
                'interaction-id':interaction['interaction-id']}))
            
        if 'answer-keywords' in interaction:
            answer_keyword = self.get_matching_answer_keyword(interaction['answer-keywords'], message)
            if not answer_keyword or answer_keyword is None:
                actions.append(UnMatchingAnswerAction(**{'answer': message}))
            else:
                reference_metadata['matching-answer'] = answer_keyword['keyword']
                if self.has_reminders(interaction):
                    actions.append(RemoveDeadlineAction(**{
                        'dialogue-id': dialogue_id,
                        'interaction-id':interaction['interaction-id']}))
                actions = self.add_feedback_action(actions, answer_keyword)
                if 'label-for-participant-profiling' in interaction:
                    action = ProfilingAction(**{
                        'label': interaction['label-for-participant-profiling'],
                        'value': answer_keyword['keyword']})
                    actions.append(action)
                if 'answer-actions' in answer_keyword:
                    for answer_keyword_action in answer_keyword['answer-actions']:
                        actions.append(action_generator(**answer_keyword_action))
        
        elif 'answers' in interaction:
            # Closed questions
            answer = self.get_matching_answer(interaction, keyword, reply)
            if not answer or answer is None:
                actions.append(UnMatchingAnswerAction(**{'answer': reply}))
            else:
                reference_metadata['matching-answer'] = answer['choice']
                if self.has_reminders(interaction):
                    actions.append(RemoveDeadlineAction(**{
                        'dialogue-id': dialogue_id,
                        'interaction-id':interaction['interaction-id']}))
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
            # Open questions
            answer = self.get_open_answer(message)
            if answer == '':
                actions.append(UnMatchingAnswerAction(**{'answer': reply}))
            else:
                reference_metadata['matching-answer'] = answer
                if self.has_reminders(interaction):
                    actions.append(RemoveDeadlineAction(**{
                        'dialogue-id': dialogue_id,
                        'interaction-id':interaction['interaction-id']}))
                actions = self.add_feedback_action(actions, interaction)
                if 'answer-label' in interaction:
                    actions.append(ProfilingAction(**{
                        'label': interaction['answer-label'],
                        'value': self.get_open_answer(message)}))
                if 'answer-actions' in interaction:
                    for answer_action in interaction['answer-actions']:
                        actions.append(action_generator(**answer_action))
        # Check if offset condition on this answer
        if not reference_metadata['matching-answer'] is None:
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
                interaction_keywords = self.get_interaction_keywords(interaction)
                for interaction_keyword in interaction_keywords:
                    keywords.append(interaction_keyword)
            elif 'answer-keywords' in interaction:
                for answer_keyword in interaction['answer-keywords']:
                    keywords += self.split_keywords(answer_keyword['keyword'])
        return keywords
