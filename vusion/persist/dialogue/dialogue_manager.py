import sys, traceback

import pymongo
from bson.objectid import ObjectId

from vusion.persist import ModelManager, Dialogue


class DialogueManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(DialogueManager, self).__init__(db, collection_name, **kwargs)
        self.collection.ensure_index('dialogue-id', background=True)

    def get_active_dialogues(self, conditions=None):
        if conditions is None:
            conditions = {}
        conditions.update({'activated': 1})        
        dialogues = self.find(conditions)
        active_dialogues = []
        for dialogue in dialogues:
            try:
                active_dialogues.append(Dialogue(**dialogue))
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log(
                    "Error while applying dialogue model on dialogue %s: %r" %
                    (dialogue['name'],
                     traceback.format_exception(exc_type, exc_value, exc_traceback)))
        return active_dialogues
   
    def get_current_dialogue(self, dialogue_id):
        dialogue = self.get_active_dialogues({'dialogue-id': dialogue_id})
        if dialogue == []:
            return None
        return dialogue[0]

    #TODO wrap it in a Dialouge object
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
    