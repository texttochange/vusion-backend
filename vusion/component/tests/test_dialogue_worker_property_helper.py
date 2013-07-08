import pymongo
from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.component.dialogue_worker_property_helper import DialogueWorkerPropertyHelper


class DialogueWorkerPropertyHelperTestCase(TestCase, ObjectMaker):

    def setUp(self):
        c = pymongo.Connection()
        c.safe = True
        db = c.test_program_db
        self.setting_collection = db.program_settings
        db = c.test_vusion_db
        self.shortcode_collection = db.shortcodes
        self.clearData()
        
        self.dwph = DialogueWorkerPropertyHelper(self.setting_collection, self.shortcode_collection)
        
    def tearDown(self):
        self.clearData()
        delattr(self, "dwph")
        
    def clearData(self):
        self.setting_collection.drop()
        self.shortcode_collection.drop()

    def test_load(self):
        settings = self.mk_program_settings()
        for setting in settings:
            self.setting_collection.save(setting)
        shortcode = self.mkobj_shortcode()
        self.shortcode_collection.save(shortcode)
        
        self.dwph.load()
        
        self.assertEqual('256-8181', self.dwph['shortcode'])
        self.assertEqual(140, self.dwph['shortcode-max-character-per-sms'])

    def test_load_fail(self):
        self.assertTrue(False)

    def test_load_callback(self):
        settings = self.mk_program_settings()
        for setting in settings:
            self.setting_collection.save(setting)
        shortcode = self.mkobj_shortcode()
        self.shortcode_collection.save(shortcode)
        
        self.called = False
        def my_callback():
            self.called = True
            
        self.dwph.load({'shortcode': my_callback})
        self.assertEqual(True, self.called)
        
    def test_use_credits(self):
        settings = self.mk_program_settings()
        for setting in settings:
            self.setting_collection.save(setting)
        shortcode = self.mkobj_shortcode()
        self.shortcode_collection.save(shortcode)
        
        self.dwph.load()

        empty_sms = ''
        self.assertEqual(1, self.dwph.use_credits(empty_sms))
        
        one_sms = 'This is a single sms.'
        self.assertEqual(1, self.dwph.use_credits(one_sms))

        two_sms = (
            'This is two sms. This is two sms. This is two sms. ' #51
            'This is two sms. This is two sms. This is two sms. ' #102
            'This is two sms. This is two sms. This is two sms. ') #153
        self.assertEqual(2, self.dwph.use_credits(two_sms))
        
        beakline_take_only_1_char_sms = (
            'This is two sms. This is two sms. This is two sms. ' #51
            'This is two sms. This is two sms. This is two sms. ' #102
            'This is two sms. This is two sms. Thi\n')            #140
        self.assertEqual(1, self.dwph.use_credits(beakline_take_only_1_char_sms))

    def test_is_ready(self):
        settings = self.mk_program_settings()
        for setting in settings:
            self.setting_collection.save(setting)
        shortcode = self.mkobj_shortcode()
        self.shortcode_collection.save(shortcode)
        
        self.dwph.load()
        self.assertTrue(self.dwph.is_ready())

        self.setting_collection.remove({'key': 'shortcode'})
        
        self.dwph.load()
        self.assertFalse(self.dwph.is_ready())
        