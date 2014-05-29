from twisted.trial.unittest import TestCase

from vusion.persist import CreditLog, ProgramCreditLog, GarbageCreditLog
from vusion.error import InvalidField

class TestCreditLog(TestCase):
    
    def test_validation_date_format(self):
        try:
            CreditLog.instanciate(**{
                'object-type': 'program-credit-log',
                'date': '2014/03/02',
                'program-database': 'test-database',
                'code': '256-8282',
                'incoming': 0,
                'outgoing': 0})
            self.fail()
        except InvalidField:
            pass
        except:
            self.fail()

    def test_validation_code_none(self):
        try:
            CreditLog.instanciate(**{
                'object-type': 'program-credit-log',
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

    def test_validation_program(self):
        pcl = CreditLog.instanciate(**{
            'object-type': 'program-credit-log',
            'date': '2014-03-02',
            'program-database': 'test-database',
            'code': '256-8282',
            'incoming': 0,
            'outgoing': 0})
        self.assertIsInstance(pcl, ProgramCreditLog)
   
        pcl = CreditLog.instanciate(**{
            'object-type': 'program-credit-log',
            'date': '2014-03-02',
            'program-database': 'test-database',
            'code': '+3175847332',
            'incoming': 0,
            'outgoing': 0})
        self.assertIsInstance(pcl, ProgramCreditLog)

    def test_validation_garbage(self):
        gcl = CreditLog.instanciate(**{
            'object-type': 'garbage-credit-log',
            'date': '2014-03-02',
            'code': '256-8282',
            'incoming': 0,
            'outgoing': 0})
        self.assertIsInstance(gcl, GarbageCreditLog)
