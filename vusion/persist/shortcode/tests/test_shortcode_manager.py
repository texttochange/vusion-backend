from pymongo import MongoClient

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.persist import ShortcodeManager, Shortcode


class TestShortcodeManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_vusion_db'
        c = MongoClient(w=1)
        db = c[self.database_name]
        self.manager = ShortcodeManager(db, 'shortcodes')
        self.clearData()

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.manager.drop()

    def test_get_shortcode(self):
        self.manager.save_document(
            Shortcode(**self.mkobj_shortcode(code='8181',
                                             international_prefix='256')))
        self.manager.save_document(
            Shortcode(**self.mkobj_shortcode(code='8282',
                                             international_prefix='256')))
        self.manager.save_document(
                    Shortcode(**self.mkobj_shortcode(code='8181',
                                                     international_prefix='255')))
        self.manager.save_document(
            Shortcode(**self.mkobj_shortcode_international(code='+318181')))
        
        shortcode = self.manager.get_shortcode('8181', '+25511111')
        self.assertEqual(shortcode['international-prefix'], '255')
        
        shortcode = self.manager.get_shortcode('8181', '+25611111')
        self.assertEqual(shortcode['international-prefix'], '256')
        
        shortcode = self.manager.get_shortcode('+318181', '+25611111')
        self.assertEqual(shortcode['international-prefix'], '31')
