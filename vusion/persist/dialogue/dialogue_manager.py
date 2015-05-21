import sys, traceback

import pymongo
from bson.objectid import ObjectId

from vusion.persist import ModelManager, Dialogue
from vusion.persist.action import Actions


class DialogueManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(DialogueManager, self).__init__(db, collection_name, **kwargs)
        self.collection.ensure_index('dialogue-id', background=True)
        self.loaded_dialogues = {}
        self.load_dialogues()

    def load_dialogues(self):
        self._get_active_dialogues()
    
    def load_dialogue(self, dialogue_id):
        self.loaded_dialogues.pop(dialogue_id, False)
        dialogue = self._get_active_dialogues({'dialogue-id': dialogue_id})

    def clear_loaded_dialogues(self):
        self.loaded_dialogues.clear()

    def get_active_dialogues(self, conditions=None):
        if conditions is None:
            #In case there are difference in the number of loaded_dialogue
            # and the one in the database reload all
            conditions = {}
            if len(self.loaded_dialogues) == self._count_active_dialogues():
                return self.loaded_dialogues.itervalues()
            self.clear_loaded_dialogues()
        return self._get_active_dialogues(conditions)

    def _get_active_dialogues(self, conditions={}):
        conditions['activated'] = 1
        dialogues = self.find(conditions)
        active_dialogues = []
        for dialogue in dialogues:
            try:
                active_dialogue = Dialogue(**dialogue)
                active_dialogues.append(active_dialogue)
                # as soon a dialogue loaded we keep it
                self.loaded_dialogues[dialogue['dialogue-id']] = active_dialogue
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log(
                    "Error while applying dialogue model on dialogue %s: %r" %
                    (dialogue['name'],
                     traceback.format_exception(exc_type, exc_value, exc_traceback)))
        return active_dialogues

    def _count_active_dialogues(self):
        return self.find({'activated': 1}).count()

    def get_current_dialogue(self, dialogue_id):
        if dialogue_id in self.loaded_dialogues:
            return self.loaded_dialogues[dialogue_id]
        dialogues = self._get_active_dialogues({'dialogue-id': dialogue_id})
        if dialogues == []:
            return None
        return dialogues[0]

    def get_dialogue_obj(self, dialogue_obj_id):
        dialogue = self.find_one({'_id': ObjectId(dialogue_obj_id)})
        return Dialogue(**dialogue)

    def get_matching_dialogue_actions(self, message_content, actions, context):
        active_dialogues = self.get_active_dialogues()
        for dialogue in active_dialogues:
            dialogue.get_matching_reference_and_actions(
                message_content, actions, context)
            if context.is_matching():
                return

    def get_actions(self, dialogue_id, interaction_id, answer):
        actions = Actions()
        dialogue = self.get_current_dialogue(dialogue_id)
        dialogue.get_interaction_actions(actions, interaction_id, answer)
        return actions

    def get_dialogue_interaction(self, dialogue_id, interaction_id):
        dialogue = self.get_current_dialogue(dialogue_id)
        return dialogue.get_interaction(interaction_id)

    def get_all_keywords(self):
        keywords = []
        for dialogue in self.get_active_dialogues():
            keywords += dialogue.get_all_keywords()
        return keywords

    def get_max_unmatching_answers_interaction(self, dialogue_id, interaction_id):
        dialogue = self.get_current_dialogue(dialogue_id)
        returned_interaction = dialogue.get_interaction(interaction_id)
        if returned_interaction.has_max_unmatching_answers():
            return returned_interaction
        return None

    def get_labels_order_from_dialogues(self, labels):
        ordered_labels = []
        for dialogue in self.get_active_dialogues():
            for interaction in dialogue['interactions']:
                if 'label-for-participant-profiling' in interaction and interaction['label-for-participant-profiling'] is not None:
                    ordered_labels.append(interaction['label-for-participant-profiling'])
                elif 'answer-label' in interaction and interaction['answer-label'] is not None:
                    ordered_labels.append(interaction['answer-label'])
        for label in labels:
            if label not in ordered_labels:
                ordered_labels.append(label)
        return ordered_labels
