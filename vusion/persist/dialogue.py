from copy import copy

from vumi import log
from vumi.utils import get_first_word
from vusion.action import (UnMatchingAnswerAction, FeedbackAction,
                           action_generator, ProfilingAction,
                           OffsetConditionAction, RemoveRemindersAction,
                           RemoveDeadlineAction, RemoveQuestionAction)
from vusion.persist.vusion_model import VusionModel
from vusion.persist.interaction import Interaction


class Dialogue(VusionModel):

    MODEL_TYPE = 'dialogue'
    MODEL_VERSION = '1'

    fields = ['name',
              'dialogue-id',
              'auto-enrollment',
              'interactions',
              'activated']

    def validate_fields(self):
        super(Dialogue, self).validate_fields()
        self.interactions = []
        if self.payload['interactions'] is None:
            return
        for interaction_raw in self.payload['interactions']:
            self.interactions.append(Interaction(**interaction_raw))
        self.payload['interactions'] = []
        for interaction in self.interactions:
            self.payload['interactions'].append(interaction.get_as_dict())

    def get_reply(self, content, delimiter=' '):
        return (content or '').partition(delimiter)[2]

    def split_keywords(self, keywords):
        return [k.lower() for k in (keywords or '').split(', ')]

    # refactor with interaction
    def get_matching_interaction(self, keyword):
        if self.payload['interactions'] is None:
            return None, None
        for interaction in self.interactions:
            if interaction['type-interaction'] == 'question-answer-keyword':
                for answer_keyword in interaction['answer-keywords']:
                    if keyword in self.split_keywords(answer_keyword['keyword']):
                        return self.payload['dialogue-id'], interaction
            elif interaction['type-interaction'] == 'question-answer':
                if keyword in interaction.get_interaction_keywords():
                    return self.payload['dialogue-id'], interaction
        return None, None

    def get_offset_condition_interactions(self, interaction_id):
        offset_condition_interactions = []
        for interaction in self.payload['interactions']:
            if (interaction['type-schedule'] == 'offset-condition' and
                    interaction['offset-condition-interaction-id'] == interaction_id):
                offset_condition_interactions.append(interaction['interaction-id'])
        return offset_condition_interactions
        
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
        if interaction.has_reminder():
            actions.append(RemoveRemindersAction(**{
                'dialogue-id': dialogue_id,
                'interaction-id':interaction['interaction-id']}))
        
        interaction.get_actions(dialogue_id, message, keyword, reply, reference_metadata, actions)
            
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

    def get_all_keywords(self):
        keywords = []
        if self.payload['interactions'] is None :
            return keywords
        for interaction in self.interactions:
            keywords += interaction.get_keywords();
        return keywords

    def get_interaction(self, interaction_id):
        for interaction in self.interactions:
            if interaction_id == interaction['interaction-id']:
                return interaction
        return None