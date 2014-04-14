from copy import copy

from vumi import log
from vumi.utils import get_first_word

from vusion.utils import clean_keyword

from vusion.persist.action import (UnMatchingAnswerAction, FeedbackAction,
                                   action_generator, ProfilingAction,
                                   OffsetConditionAction, RemoveRemindersAction,
                                   RemoveDeadlineAction, RemoveQuestionAction)
from vusion.persist import Model, Interaction
from vusion.error import VusionError


## TODO update the validation
class Dialogue(Model):

    MODEL_TYPE = 'dialogue'
    MODEL_VERSION = '2'

    fields = ['name',
              'dialogue-id',
              'auto-enrollment',
              'interactions',
              'activated',
              'set-prioritized']

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

    def upgrade(self, **kwargs):
        if kwargs['model-version'] == '1':
            kwargs['set-prioritized'] = kwargs['set-prioritized'] if 'set-prioritized' in kwargs else None
            kwargs['model-version'] = '2'
        return kwargs
    
    def get_reply(self, content, delimiter=' '):
        return (content or '').partition(delimiter)[2]

    def split_keywords(self, keywords):
        return [k.lower() for k in (keywords or '').split(', ')]

    def get_matching_interaction(self, keyword):
        if self.payload['interactions'] is None:
            return None, None
        for interaction in self.interactions:
            if interaction.is_matching(keyword):
                return self.payload['dialogue-id'], interaction
        return None, None

    def get_offset_condition_interactions(self, interaction_id):
        offset_condition_interactions = []
        for interaction in self.payload['interactions']:
            if (interaction['type-schedule'] == 'offset-condition' and
                    interaction['offset-condition-interaction-id'] == interaction_id):
                offset_condition_interactions.append(interaction['interaction-id'])
        return offset_condition_interactions

    def get_matching_reference_and_actions(self, message, actions, context):
        keyword = clean_keyword(get_first_word(message))
        reply = clean_keyword(self.get_reply(message))
        dialogue_id, interaction = self.get_matching_interaction(keyword)

        if not interaction:
            return

        context.update({
            'dialogue-id': dialogue_id,
            'interaction-id': interaction['interaction-id'],
            'interaction': interaction,
            'matching-answer': None})      

        interaction.get_actions(dialogue_id, message, keyword, reply, context, actions)

        # Check if offset condition on this answer
        if context.is_matching_answer():
            for interaction_to_schedule in self.get_offset_condition_interactions(interaction['interaction-id']):
                actions.append(OffsetConditionAction(**{
                    'dialogue-id': dialogue_id,
                    'interaction-id': interaction_to_schedule}))

    def get_all_keywords(self):
        keywords = []
        if self.payload['interactions'] is None:
            return keywords
        for interaction in self.interactions:
            keywords += interaction.get_keywords()
        return keywords

    def get_interaction(self, interaction_id):
        for interaction in self.interactions:
            if interaction_id == interaction['interaction-id']:
                return interaction
        return None

    def get_auto_enrollment_as_query(self):
        if self['auto-enrollment'] == 'all':
            return {}
        return None   # 'none' case

    def is_enrollable(self, participant):
        if self['auto-enrollment'] == 'all':
            return True
        return False
