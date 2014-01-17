import pymongo
from bson.objectid import ObjectId

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.component import DialogueWorkerPropertyHelper
from vusion.persist import RequestManager, Request
from vusion.context import Context


class TestRequestManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_program_db'
        c = pymongo.Connection()
        c.safe = True
        db = c[self.database_name]
        self.request_manager = RequestManager(db, 'requests')
        
        #parameters:
        self.property_helper = DialogueWorkerPropertyHelper(None, None)
        self.property_helper['timezone'] = 'Africa/Kampala'
        
        self.request_manager.set_property_helper(self.property_helper)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.request_manager.drop()

    def test_load_requests(self):
        request_join_id = self.request_manager.save(self.mkobj_request_join())
        request_leave_id = self.request_manager.save(self.mkobj_request_leave())
        self.request_manager.load_requests()
        self.assertEqual(
            len(self.request_manager.loaded_requests),
            2)

    def test_load_request(self):
        request_id = self.request_manager.save(self.mkobj_request_join())
        self.request_manager.load_requests()
        request_leave = self.mkobj_request_leave()
        request_leave['_id'] = request_id
        self.request_manager.save(request_leave)
        
        self.request_manager.load_request(str(request_id))
        self.assertEqual(
            self.request_manager.loaded_requests[str(request_id)]['keyword'],
            request_leave['keyword'])
        
        self.request_manager.remove({'_id': request_id})
        self.request_manager.load_request(str(request_id))
        self.assertEqual(
            len(self.request_manager.loaded_requests),
            0)

    def test_get_all_keywords(self):
        self.fail()

    def test_get_matching_request_actions(self):
        pass
