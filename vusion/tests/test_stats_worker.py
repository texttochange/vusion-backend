from pymongo import MongoClient
from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase, WorkerHelper, MessageHelper

from tests.utils import MessageMaker, ObjectMaker
from vusion.tests.fixtures.program_fixture import ProgramFixture
from vusion import StatsWorker

class StatsWorkerTestCase(VumiTestCase, MessageMaker, ObjectMaker):

    base_config = {
        'application_name': 'stats',
        'mysql_user': 'cake_test',
        'mysql_password': 'password',
        'mysql_db': 'vusion_test',
        'mongodb_db_vusion': 'vusion_test',}

    @inlineCallbacks
    def setUp(self):
        self.program_fixture = ProgramFixture()
        self.application_name = self.base_config['application_name']
        self.worker_helper = self.add_helper(WorkerHelper())
        self.message_helper = self.add_helper(MessageHelper())
        self.worker = yield self.worker_helper.get_worker(
            StatsWorker, self.base_config)

    @inlineCallbacks
    def tearDown(self):
        yield self.worker_helper.cleanup()
        self.program_fixture.clearData()

    def dispatch_control(self, control):
        return self.worker_helper.dispatch_raw('.'.join([self.application_name, 'control']), control)

    def test_start_up(self):
        schedules = self.worker.get_schedules()
        self.assertEquals([], schedules.keys())

    @inlineCallbacks
    def test_add_program(self):
        self.program_fixture.initialize_properties(2)
        control = self.mkmsg_statsworker_control(
            action='add_stats',
            program_db='myprogramDb3')
        yield self.dispatch_control(control)

        schedules = self.worker.get_schedules()
        self.assertEquals(
            ['myprogramDb3'],
            schedules.keys())

    @inlineCallbacks
    def test_remove_program(self):
        control = self.mkmsg_statsworker_control(
            action='add_stats',
            program_db='myprogramDb')
        yield self.dispatch_control(control)

        control = self.mkmsg_statsworker_control(
            action='remove_stats',
            program_db='myprogramDb')
        yield self.dispatch_control(control)

        schedules = self.worker.get_schedules()
        self.assertEquals({}, schedules)
