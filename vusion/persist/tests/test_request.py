"""Tests for persist.request."""

from twisted.trial.unittest import TestCase

from vusion.persist import Request

from tests.utils import ObjectMaker


class TestRequest(TestCase, ObjectMaker):
    
    def test_upgrade(self):
        request_raw = {'keyword': 'join',
                       'responses' : [],
                       'actions': []}
        
        request = Request(**request_raw)
        
        self.assertTrue(request is not None)
        self.assertFalse(request.is_lazy_matching())
        