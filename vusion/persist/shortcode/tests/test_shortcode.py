from datetime import datetime
from twisted.trial.unittest import TestCase
from tests.utils import ObjectMaker
from vusion.persist import Shortcode

class TestShortcode(TestCase, ObjectMaker):
    
    def test_upgrade_noversion_to_current(self):
        shortcode = Shortcode(**self.mkobj_shortcode())
        self.assertEqual(Shortcode.MODEL_VERSION, shortcode['model-version'])

    def test_upgrade(self):
        shortcode = {
            'object-type': None, 
            'supported-internationally': 0, 
            'error-template': u'5072ac721bdb8c2762000000', 
            'created': 'datetime.datetime(2012, 9, 9, 12, 6, 9, 46000)', 
            'country': 'Tanzania', 
            'modified': 'datetime.datetime(2012, 10, 8, 10, 36, 35, 636000)', 
            'support-customized-id': 0, 
            'model-version': None, 
            'international-prefix': u'255', 
            '_id': "ObjectId('504c86311bdb8c3b2c000000')", 
            'shortcode': u'15012'}
        shortcode = Shortcode(**self.mkobj_shortcode())
        self.assertEqual(Shortcode.MODEL_VERSION, shortcode['model-version'])