from twisted.trial.unittest import TestCase

from vusion.persist import Export

from tests.utils import ObjectMaker


class TestExport(TestCase, ObjectMaker):

    def test_validation_ok(self):
        export = Export(**self.mkdoc_export())
        self.assertTrue(isinstance(export, Export))
