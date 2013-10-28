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
        dialogues = self.group(
            ['dialogue-id'],
            conditions,
            {'Dialogue': 0},
            """function(obj, prev){
            if (obj.activated==1 &&
            (prev.Dialogue==0 || prev.Dialogue.modified <= obj.modified))
            prev.Dialogue = obj;}"""
        )
        active_dialogues = []
        for dialogue in dialogues:
            if dialogue['Dialogue'] == 0.0:
                continue
            try:
                active_dialogues.append(Dialogue(**dialogue['Dialogue']))
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.log(
                    "Error while applying dialogue model on dialogue %s: %r" %
                    (dialogue['Dialogue']['name'],
                     traceback.format_exception(exc_type, exc_value, exc_traceback)))
        return active_dialogues
   
    def get_current_dialogue(self, dialogue_id):
        try:
            dialogue = self.get_active_dialogues({'dialogue-id': dialogue_id})
            if dialogue == []:
                return None
            return dialogue[0]
        except:
            self.log("Cannot get current dialogue %s" % dialogue_id)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error message: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))
   
    def get_dialogue_obj(self, dialogue_obj_id):
        dialogue = self.find_one(
            {'_id': ObjectId(dialogue_obj_id)})
        return dialogue
   
    def get_matching_dialogue_actions(self):
        pass
    
    def get_dialogue_interaction(self):
        pass
    
    def get_keywords(self):
        pass

    def get_max_unmatching_answers_interaction(self, dialogue_id, interaction_id):
        dialogue = self.get_current_dialogue(dialogue_id)
        returned_interaction = dialogue.get_interaction(interaction_id)
        if returned_interaction.has_max_unmatching_answers():
            return returned_interaction
        return None
    