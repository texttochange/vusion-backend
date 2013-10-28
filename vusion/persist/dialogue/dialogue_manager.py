import sys, traceback

import pymongo
from bson.objectid import ObjectId

from vusion.persist import ModelManager, Dialogue


class DialogueManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(DialogueManager, self).__init__(db, collection_name, **kwargs)
        self.collection.ensure_index('dialogue-id', background=True)
        self.loaded_dialogues = {}
        self.load_dialogues()

    def load_dialogues(self):
        self._get_active_dialogues()
    
    def load_dialogue(self, dialogue_id):
        self.loaded_dialogues.pop(dialogue_id, false)
        dialogue = self._get_active_dialogues({'dialogue-id': dialogue_id})
        if dialogue is not None:
            self.loaded_dialogues[dialogue_id] = dialogue

    def clear_loaded_dialogues(self, dialogue_id):
        self.loaded_dialogues.clear()

    def get_active_dialogues(self, conditions={}):
        if conditions == {}:
            #In case there are difference in the number of loaded_dialogue
            # and the one in the database reload all
            if len(self.loaded_dialogues) == self._count_active_dialogues():
                return self.loaded_dialogues
        return self._get_active_dialogues(conditions)

    def _get_active_dialogues(self, conditions={}):
        conditions.update({'activated': 1})
        dialogues = self.find(conditions)
        active_dialogues = []
        for dialogue in dialogues:
            try:
                dialogue = Dialogue(**dialogue)
                active_dialogues.append(dialogue)
                # as soon a dialogue loaded we keep it
                self.loaded_dialogues[dialogue['dialogue_id']] = dialogue
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
    