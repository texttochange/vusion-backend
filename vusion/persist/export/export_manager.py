import sys
import traceback
import os
from bson import ObjectId

from vusion.persist.model_manager import ModelManager
from vusion.persist import Export


class ExportManager(ModelManager):

    def __init__(self, db, collection_name, **kwargs):
        super(ExportManager, self).__init__(db, collection_name, **kwargs)
        ##As default filter index
        self.collection.ensure_index(
            'timestamp',
            background=True)

    def get_export(self, export_id):
        try:
            query = {'_id': ObjectId(str(export_id))}
            document = self.collection.find_one(query)
            if document is None:
                self.log("Export %s is not in collection." % export_id)
                return None
            return Export(**document)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log(
                "Error while loading an export %s  %r" %
                (export_id,
                 traceback.format_exception(exc_type, exc_value, exc_traceback)))
            return None

    def failed(self, export_id, reason):
        update_selector = {'_id': ObjectId(str(export_id))}
        update_query = {
            '$set': {'status': 'failed',
                     'failure-reason': reason}}
        self.collection.update(update_selector, update_query)

    def success(self, export_id, size):
        update_selector = {'_id': ObjectId(str(export_id))}
        update_query = {
            '$set': {'status': 'success',
                     'size': long(size)}}
        self.collection.update(update_selector, update_query)

    def processing(self, export_id):
        update_selector = {'_id': ObjectId(str(export_id))}
        update_query = {
            '$set': {'status': 'processing'}}
        self.collection.update(update_selector, update_query)

    def no_space(self, export_id):
        update_selector = {'_id': ObjectId(str(export_id))}
        update_query = {
            '$set': {'status': 'no-space'}}
        self.collection.update(update_selector, update_query)

    def get_total_export_size(self):
        pipeline = [
            {'$match': {'status': 'success'}},
            {'$group': {
                '_id': None,
                'total': {'$sum': '$size'}}}]
        mongo_return = self.aggregate(pipeline)
        if mongo_return['result'] == []:
            return 0L
        return mongo_return['result'][0]['total']

    def has_export_space(self, limit):
        return limit <= self.get_total_export_size()

    def cancel_processing(self):
        processings = self.collection.find({'status': 'processing'})
        for processing in processings:
            os.remove(processing['file-full-name'])
        self.collection.update(
            {'status': 'processing'},
            {'$set': {'status': 'failed',
                     'failure-reason': 'unknown'}},
            {'multi': True})