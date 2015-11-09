from pymongo import MongoClient
from datetime import datetime
from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker
from vusion.error import MissingLocalTime, MissingCode
from vusion.component.dialogue_worker_property_helper import DialogueWorkerPropertyHelper


class DialogueWorkerPropertyHelperTestCase(TestCase, ObjectMaker):

    def setUp(self):
        c = MongoClient(w=1)
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

    def save_settings(self, settings):
        for setting in settings:
            self.setting_collection.save(setting)        

    def test_load(self):
        self.save_settings(self.mk_program_settings())
        shortcode = self.mkobj_shortcode()
        self.shortcode_collection.save(shortcode)
        
        self.dwph.load()
        
        self.assertEqual('256-8181', self.dwph['shortcode'])
        self.assertEqual(140, self.dwph['shortcode-max-character-per-sms'])


    def test_load_callback(self):
        self.save_settings(self.mk_program_settings())
        shortcode = self.mkobj_shortcode()
        self.shortcode_collection.save(shortcode)

        self.called = False
        def my_callback():
            self.called = True

        self.dwph.load({'shortcode': my_callback})
        self.assertEqual(True, self.called)

    def test_load_callbacks(self):
        self.save_settings(self.mk_program_settings())
        shortcode = self.mkobj_shortcode()
        self.shortcode_collection.save(shortcode)

        self.first_called = False
        def my_first_callback():
            self.first_called = True

        self.second_called = False
        def my_second_callback():
            self.second_called = True

        self.dwph.load({'shortcode': [my_first_callback, my_second_callback]})
        self.assertEqual(True, self.first_called)
        self.assertEqual(True, self.second_called)

    def test_use_credits(self):
        self.save_settings(self.mk_program_settings())
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
        self.save_settings(self.mk_program_settings())
        shortcode = self.mkobj_shortcode()
        self.shortcode_collection.save(shortcode)
        
        self.dwph.load()
        self.assertTrue(self.dwph.is_ready())

        self.setting_collection.remove({'key': 'shortcode'})
        
        self.dwph.load()
        self.assertFalse(self.dwph.is_ready())

    def test_load_fail(self):
        self.save_settings(self.mk_program_settings())
        try:
            self.dwph.load()
            self.fail()
        except MissingCode:
            return
        self.fail()
    
    def test_get_local_time_timezone_not_set(self):
        local_time = self.dwph.get_local_time()
        self.assertIsInstance(local_time, datetime)

    def test_get_local_time(self):
        self.save_settings(self.mk_program_settings())
        shortcode = self.mkobj_shortcode()
        self.shortcode_collection.save(shortcode)
        
        self.dwph.load()
        local_time = self.dwph.get_local_time()
        
        self.assertIsInstance(local_time, datetime)

    def test_get_seconds_until(self):
        self.assertEqual(None, self.dwph.get_seconds_until(1))

        self.save_settings(self.mk_program_settings())
        shortcode = self.mkobj_shortcode()
        self.shortcode_collection.save(shortcode)
        self.dwph.load()

        seconds_until = self.dwph.get_seconds_until(1)
        self.assertTrue(86400 > seconds_until)
        self.assertTrue(0 < seconds_until)
