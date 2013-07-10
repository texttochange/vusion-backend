"""Tests for vusion.persist.request."""

from twisted.trial.unittest import TestCase

from vusion.error import FailingModelUpgrade
from vusion.persist import Request

from tests.utils import ObjectMaker


class TestRequest(TestCase, ObjectMaker):

    def test_upgrade(self):
        request_raw = {'keyword': 'join',
                       'responses': [],
                       'actions': []}

        request = Request(**request_raw)

        self.assertTrue(request is not None)
        self.assertFalse(request.is_lazy_matching())

    def test_upgrade_fail(self):
        request_raw = {'keyword': 'join',
                       'model-version': '3000',
                       'responses': [],
                       'actions': []}

        self.assertRaises(FailingModelUpgrade, Request, **request_raw)

    def test_get_keywords_no_space(self):
        request = Request(**self.mkobj_request_response("keyword1,keyword2"))
        
        self.assertEqual(
            ["keyword1", "keyword2"],
            request.get_keywords())
    
    def test_get_keywords_space(self):
        request = Request(**self.mkobj_request_response("keyword1, keyword2"))
        
        self.assertEqual(
            ["keyword1", "keyword2"],
            request.get_keywords())

    def test_get_keywords_from_keyphrase(self):
        request = Request(**self.mkobj_request_response("keyword1 something, keyword2 somethingelse"))
        
        self.assertEqual(
            ["keyword1", "keyword2"],
            request.get_keywords())

    def test_get_keywords_uppercase(self):
        request = Request(**self.mkobj_request_response("keyWord1"))
        
        self.assertEqual(
            ["keyword1"],
            request.get_keywords())

    def test_get_keywords_from_keyphrase_samekeyword(self):
        request = Request(**self.mkobj_request_response("keyword1 something, keyword1 somethingelse"))
        
        self.assertEqual(
            ["keyword1"],
            request.get_keywords())