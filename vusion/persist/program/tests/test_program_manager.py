from twisted.trial.unittest import TestCase

from vusion.persist import ProgramManager
from vusion.tests.fixtures.program_fixture import ProgramFixture


class TestProgramManager(TestCase):

    def setUp(self):
        self.fixture = ProgramFixture()
        self.manager = ProgramManager(self.fixture.get_mysql())

    def tearDown(self):
        self.fixture.clearData()

    def test_get_running_programs(self):
        programs = self.manager.get_running()
        self.assertEqual('myprogramUrl', programs[0]['url'])
        self.assertEqual('myprogramUrl3', programs[1]['url'])
