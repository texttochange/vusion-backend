import sys, traceback

from bson.objectid import ObjectId

from vusion.persist import ModelManager, Request


class RequestManager(ModelManager):

    def __init__(self, db, collection_name, **kwargs):
        super(RequestManager, self).__init__(db, collection_name, **kwargs)
        #NO index on the request collection
        self.loaded_requests = {}
        self.load_requests()

    def load_requests(self):
        requests = self.find()
        for request in requests:
            request = Request(**request)
            self.loaded_requests[str(request['_id'])] = request

    def load_request(self, request_id):
        request = self.find_one({'_id': ObjectId(request_id)})
        if request is None:
            self.loaded_requests.pop(request_id)
        else:
            request = Request(**request)
            self.loaded_requests.update({request_id: request})

    def get_all_keywords(self):
        pass

    def get_matching_request_actions(self, message_content, actions, context):
        # exact matching
        exact_regex = re.compile(('(,\s|^)%s($|,)' % content), re.IGNORECASE)
        matching_request = self.collections['requests'].find_one(
            {'keyword': {'$regex': exact_regex}})
        if matching_request:
            request = Request(**matching_request)
            request.append_actions(actions)
            context.update({'request-id': matching_request['_id']})
            return
        # lazy keyword matching
        lazy_regex = re.compile(
            ('(,\s|^)%s(\s.*|$|,)' % get_first_word(content)), re.IGNORECASE)
        matching_request = self.collections['requests'].find_one(
            {'keyword': {'$regex': lazy_regex},
             'set-no-request-matching-try-keyword-only': 'no-request-matching-try-keyword-only'})
        if matching_request:
            request = Request(**matching_request)
            request.append_actions(actions)
            context.update({'request-id': matching_request['_id']})
        
