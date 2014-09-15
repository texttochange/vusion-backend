from twisted.trial.unittest import TestCase
from vusion.context import Context
from vusion.error import MissingData


class TestContext(TestCase):
    
    def test_get_message_keyword(self):
        context = Context(**{'message': "NAME Olivier Mark Gerald"})
        self.assertEqual("NAME", context.get_message_keyword())

    def test_get_message_second_word(self):
        context = Context(**{'message': "NAME Olivier Mark Gerald"})
        self.assertEqual("Olivier", context.get_message_second_word())
        
        context = Context(**{'message': "NAME Olivier"})
        self.assertEqual("Olivier", context.get_message_second_word())

    def test_get_data_from_notation(self):
        context = Context(**{'message': "NAME Olivier Mark Gerald"})

        try:
            context.get_data_from_notation('something')
        except MissingData:
            pass

        self.assertEqual(
            "NAME Olivier Mark Gerald",
            context.get_data_from_notation('message'))

        self.assertEqual(
            "NAME",
            context.get_data_from_notation('message', '1'))

        self.assertEqual(
            "Olivier",
            context.get_data_from_notation('message', '2'))

        self.assertEqual(
            None,
            context.get_data_from_notation('message', '10'))

        self.assertEqual(
            "Olivier Mark Gerald",
            context.get_data_from_notation('message', '2', 'end'))

        self.assertEqual(
            "Olivier Mark Gerald",
            context.get_data_from_notation('message', '2', '6'))

        self.assertEqual(
            None,
            context.get_data_from_notation('message', '6', '8'))

        self.assertEqual(
            "NAME",
            context.get_data_from_notation('message', '1', '1'))
