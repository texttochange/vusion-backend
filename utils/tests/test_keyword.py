#encoding: utf-8
from twisted.trial.unittest import TestCase
from utils.keyword import clean_keyword

class TestCleanKeyword(TestCase):
    
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