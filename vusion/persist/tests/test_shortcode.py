from twisted.trial.unittest import TestCase
from tests.utils import ObjectMaker
from vusion.persist.shortcode import Shortcode

class TestShortcode(TestCase, ObjectMaker):
    
    def test_upgrade_noversion_to_current(self):
        shortcode = Shortcode(**self.mkobj_shortcode())
        self.assertEqual(Shortcode.MODEL_VERSION, shortcode['model-version'])
