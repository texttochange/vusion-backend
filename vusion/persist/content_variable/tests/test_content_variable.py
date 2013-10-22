from twisted.trial.unittest import TestCase

from vusion.persist import ContentVariable

from tests.utils import ObjectMaker


class TestContentVariable(TestCase, ObjectMaker):
    
    def test_validation(self):
        cv = ContentVariable(
            **self.mkobj_content_variables_one_key(key1='temperature', value='12'))
        self.assertEqual(cv['model-version'], ContentVariable.MODEL_VERSION)
