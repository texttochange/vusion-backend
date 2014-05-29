
from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.persist import Template


class TestTemplate(TestCase, ObjectMaker):
    
    def test_upgrade(self):
        template = Template(**{
            'name': 'my template',
            'type-template': 'open-question',
            'template': 'This is the template'
        })
        self.assertEqual(
            template['model-version'],
            Template.MODEL_VERSION)