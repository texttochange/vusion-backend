#encoding: utf-8
from twisted.trial.unittest import TestCase
from vusion import clean_keyword
from vusion.utils import clean_phone


class TestCleanKeyword(TestCase):
    
    def test_str(self):
        self.assertEqual(
            clean_keyword('STRUFF'),
            'struff')
    
    def test_french_accent(self):
        self.assertEqual(
            clean_keyword(u'áàâä éèêë íîï óô úùûü'),
            'aaaa eeee iii oo uuuu')
        self.assertEqual(
            clean_keyword(u'ÁÀÂÄ ÉÈÊË ÍÎÏ ÓÔ ÚÙÛÜ'), 
            'aaaa eeee iii oo uuuu');
    
    def test_french_ligature(self):
        self.assertEqual(
            clean_keyword(u'æÆœŒ'),
            'aeaeoeoe')

    def test_french_other(self):
        self.assertEqual(
            clean_keyword(u'çÇ'),
            'cc')

    def test_spanish_accent(self):
        self.assertEqual(
            clean_keyword(u'áéíóúüñ'),
            'aeiouun')
        self.assertEqual(
            clean_keyword(u'ÁÉÍÓÚÜÑ'),
            'aeiouun')

    def test_clean_phone(self):
        self.assertEqual(clean_phone('+256111'), '+256111')
        self.assertEqual(clean_phone('256111'), '+256111')
        self.assertEqual(clean_phone('0256111'), '+256111')
        self.assertEqual(clean_phone('00256111'), '+256111')
        self.assertEqual(clean_phone(''), None)
        