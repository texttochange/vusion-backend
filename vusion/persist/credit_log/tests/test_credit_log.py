from twisted.trial.unittest import TestCase

from vusion.persist import CreditLog
from vusion.error import InvalidField

class TestCreditLog(TestCase):
    
    def test_validation_date_format(self):
        try:
            CreditLog(**{
                'date': '2014/03/02',
                'program-database': 'test-database',
                'code': '8282',
                'incoming': 0,
                'outgoing': 0})
            self.fail()
        except InvalidField:
            pass
        except:
            self.fail()

    def test_validation_code_none(self):
        try:
            CreditLog(**{
                'date': '2014-03-02',
                'program-database': 'test-database',
                'code': None,
                'incoming': 0,
                'outgoing': 0})
            self.fail()
        except InvalidField as e:
            pass
        except:
            self.fail()
