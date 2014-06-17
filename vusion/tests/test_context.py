from twisted.trial.unittest import TestCase
from vusion.context import Context


class TestContext(TestCase):
    
    def test_get_message_keyword(self):
        context = Context(**{'message': "NAME Olivier Mark Gerald"})
        self.assertEqual("NAME", context.get_message_keyword())

    def test_get_message_second_word(self):
        context = Context(**{'message': "NAME Olivier Mark Gerald"})
        self.assertEqual("Olivier", context.get_message_second_word())
        
        context = Context(**{'message': "NAME Olivier"})
        self.assertEqual("Olivier", context.get_message_second_word())
        
    