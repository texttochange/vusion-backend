import sys
import traceback
import datetime

#import MySQLdb
from pymongo import MongoClient

from twisted.internet import reactor, task
from twisted.internet.defer import inlineCallbacks, maybeDeferred

from vumi import log
from vumi.worker import BaseWorker
from vumi.config import ConfigText, ConfigInt, ConfigFloat

from vusion.connectors import ReceiveStatsWorkerControlConnector
from vusion.persist import (
    ParticipantManager, HistoryManager, ShortcodeManager,
    ProgramSettingManager, ProgramManager)
from vusion.component import DialogueWorkerPropertyHelper


class StatsWorkerConfig(BaseWorker.CONFIG_CLASS):

    application_name = ConfigText(
        "The name this application instance will use to create its queues.",
        required=True, static=True)

    computation_hour=ConfigInt(
        "The hour at which the worker will perform the stat computation",
        default=1, static=True)

    mongodb_host = ConfigText(
        "The host machine address for mongodb",
        default='localhost', static=True)
    mongodb_port = ConfigInt(
        "The host machine port for mongodb",
        default=27017, static=True)
    mongodb_db_vusion = ConfigText(
        "The vusion database in mongo",
        default='vusion', static=True)

    mysql_host = ConfigText(
        "The host of the mysql instance.",
        default='127.0.0.1', static=True)
    mysql_port = ConfigInt(
        "The port of the mysql instance.",
        default=3306, static=True)
    mysql_user = ConfigText(
        "The user of the mysql instance.",
        required=True, static=True)
    mysql_password = ConfigText(
        "The password of the mysql instance.",
        required=True, static=True)
    mysql_db = ConfigText(
        "The db of the mysql instance.",
        default='vusion', static=True)


class StatsWorker(BaseWorker):

    CONFIG_CLASS = StatsWorkerConfig
    UNPAUSE_CONNECTORS = True

    def _validate_config(self):
        config = self.get_static_config()
        self.application_name = config.application_name
        self.validate_config()

    def setup_connectors(self):
        d = self.setup_connector(
            ReceiveStatsWorkerControlConnector,
            self.application_name)
    
        def cb(connector):
            connector.set_control_handler(self.dispatch_control)
            return connector

        return d.addCallback(cb)

    def setup_worker(self):
        d = maybeDeferred(self.setup_application)
        if self.UNPAUSE_CONNECTORS:
            d.addCallback(lambda r: self.unpause_connectors())
        return d

    def setup_application(self):
        config = self.get_static_config()
        log.debug('Starting Stats worker %s' % (config,))

        self.mongo = MongoClient(
            config.mongodb_host,
            config.mongodb_port,
            w=1)

        self.schedule_calls = {}

        self.shortcode_manager = ShortcodeManager(
            self.mongo[config.mongodb_db_vusion],
            'shortcodes')

        #self.mysql = MySQLdb.connect(
                    #host=config.mysql_host,
                    #port=config.mysql_port,
                    #user=config.mysql_user,
                    #passwd=config.mysql_password,
                    #db=config.mysql_db)
        #self.program_manager = ProgramManager(self.mysql)
        #programs = self.program_manager.get_running()
        #for program in programs:
            #self.schedule_stats(program['database'])

    def compute_stats(self, program_db_name):
        log.debug("compute stats for %s" % program_db_name)
        program_db = self.mongo[program_db_name]
        participant_manager = ParticipantManager(program_db, 'participants')
        participant_manager.aggregate_count_per_day()
        history_manager = HistoryManager(program_db, 'history')
        history_manager.aggregate_count_per_day()
        self.schedule_stats(program_db_name)

    def schedule_stats(self, program_db_name):
        log.debug("schedule stats for %s" % (program_db_name))
        program_db = self.mongo[program_db_name]
        until_next_1am = self.get_until_next_1am(program_db)
        if until_next_1am is None:
            return
        d = reactor.callLater(until_next_1am, self.compute_stats, program_db_name)
        self.schedule_calls[program_db_name] = d
        log.debug("scheduled stats for %s in %s" % (program_db_name, until_next_1am))

    def get_schedules(self):
        return self.schedule_calls

    def dispatch_control(self, msg):
        try:
            log.debug("Processing %r" % msg)
            if msg['message_type'] == 'add_stats':
                self.add_program(msg['program_db'])
            elif msg['message_type'] == 'remove_stats':
                self.remove_program(msg['program_db'])
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error = "Exception: %r" % traceback.format_exception(
                exc_type,
                exc_value,
                exc_traceback)
            log.error(error)

    def add_program(self, program_db):
        self.compute_stats(program_db)  #to remove for prod
        #self.schedule_stats(program_db)

    def remove_program(self, program_db):
        call = self.schedule_calls.pop(program_db, None)
        if not call is None:
            call.cancel()

    def get_until_next_1am(self, program_db):
        config = self.get_static_config()
        property_helper = DialogueWorkerPropertyHelper(
            ProgramSettingManager(program_db, 'program_settings'),
            self.shortcode_manager)
        property_helper.load()
        return property_helper.get_seconds_until(config.computation_hour)

    def teardown_worker(self):
        d = self.pause_connectors()
        d.addCallback(lambda r: self.teardown_application())
        return d

    def teardown_application(self):
        for program_db, schedule_call in self.schedule_calls.iteritems():
            if not schedule_call.cancelled:
                schedule_call.cancel()
        #self.mysql.close()
        self.mongo.close()
