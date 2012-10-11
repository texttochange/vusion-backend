"""Tests for vusion.persist.request."""

from twisted.trial.unittest import TestCase

from vusion.error import FailingModelUpgrade
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
    
    def test_upgrade_fail(self):
        request_raw = {'keyword': 'join',
                       'model-version': '3000',
                       'responses' : [],
                       'actions': []}
        
        self.assertRaises(FailingModelUpgrade, Request, **request_raw)
        