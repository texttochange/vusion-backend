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

    def get_requests(self):
        if len(self.loaded_requests) != self.count():
            self.load_requests()
        return self.loaded_requests

    def get_all_keywords(self):
        keywords = []
        requests = self.get_requests()
        for request_id, request in requests.iteritems():
            for keyword in request.get_keywords():
                if keyword not in keywords:
                    keywords.append(keyword)
        return keywords

    def get_matching_request_actions(self, message_content, actions, context):
        requests = self.get_requests()
        # keyphrase matching
        for request_id, request in requests.iteritems():
            if request.is_matching(message_content):
                actions.extend(request.get_actions())
                context.update({'request-id': ObjectId(request_id)})  
                return

        # keyword/lazy matching
        for request_id, request in requests.iteritems():
            if request.is_matching(message_content, False):
                actions.extend(request.get_actions())
                context.update({'request-id': ObjectId(request_id)})
                return
