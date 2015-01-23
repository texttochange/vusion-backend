import sys
import traceback
import csv

from pymongo import MongoClient

from twisted.internet.defer import inlineCallbacks, maybeDeferred

from vumi.config import ConfigText, ConfigInt
from vumi.worker import BaseWorker
from vumi import log

from vusion.connectors import ReceiveExportWorkerControlConnector
from vusion.message import ExportWorkerControl
from vusion.persist import ParticipantManager, HistoryManager


class ExportWorkerConfig(BaseWorker.CONFIG_CLASS):
    """Base config definition for applications.

    You should subclass this and add application-specific fields.
    """

    application_name = ConfigText(
        "The name this application instance will use to create its queues.",
        required=True, static=True)
    mongodb_host = ConfigText(
        "The host machine address for mongodb",
        default='localhost', static=True)
    mongodb_port = ConfigInt(
        "The host machine port for mongodb",
        default=27017, static=True)


class ExportWorker(BaseWorker):

    CONFIG_CLASS = ExportWorkerConfig
    UNPAUSE_CONNECTORS = True

    def _validate_config(self):
        config = self.get_static_config()
        self.application_name = config.application_name
        self.validate_config()
    
    def setup_connectors(self):
        d = self.setup_connector(
            ReceiveExportWorkerControlConnector,
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
        pass

    def teardown_worker(self):
        d = self.pause_connectors()
        d.addCallback(lambda r: self.teardown_application())
        return d

    def teardown_application(self):
        pass

    @inlineCallbacks
    def dispatch_control(self, msg):
        try:
            log.debug("Exporting %r" % msg)
            if msg['message_type'] == 'export_participants':
                yield self.export_participants(msg)
            elif msg['message_type'] == 'export_history':
                self.export_history(msg)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            log.error(
                "Exception: %r" %
                traceback.format_exception(exc_type, exc_value, exc_traceback))

    @inlineCallbacks
    def export_participants(self, msg):
        config = self.get_static_config()
        mongo = MongoClient(config.mongodb_host, config.mongodb_port)
        db = mongo[msg['database']]
        manager = ParticipantManager(db, msg['collection'])
        headers = ['phone', 'tags'] 
        label_headers = yield manager.get_labels(msg['conditions'])
        headers += label_headers
        with open(msg['file_full_name'], 'wb') as csvfile:
            csvfile.write("%s\n" % ','.join(headers))
            cursor = manager.get_participants(msg['conditions'])
            row_template = dict((header, "") for header in headers)
            for participant in cursor:
                row = row_template.copy()
                row['phone'] = '"%s"' % participant['phone']
                row['tags'] = '"%s"' % ','.join(participant['tags'])
                for label in participant['profile']:
                    if label['label'] not in headers:
                        raise Exception(
                            'One label %s of %s was missed on %s'% (
                                label['label'],
                                participant['phone'],
                                msg['conditions']))
                    row[label['label']] = '"%s"' % label['value']
                ordered_row = []
                for header in headers:
                    ordered_row.append(row[header])
                csvfile.write("%s\n" % ','.join(ordered_row))
            csvfile.close()

    def export_history(self, msg):
        config = self.get_static_config()
        mongo = MongoClient(config.mongodb_host, config.mongodb_port)
        db = mongo[msg['database']]
        manager = HistoryManager(db, msg['collection'], None, None)
        headers = ['participant-phone',
                   'message-direction',
                   'message-status',
                   'message-content',
                   'timestamp'] 
        with open(msg['file_full_name'], 'wb') as csvfile:
            csvfile.write("%s\n" % ','.join(headers))
            cursor = manager.get_historys(msg['conditions'])
            row_template = dict((header, "") for header in headers)
            for history in cursor:
                row = row_template.copy()
                row['participant-phone'] = '"%s"' % history['participant-phone']
                row['message-direction'] = '"%s"' % history['message-direction']
                row['message-status'] = '"%s"' % history['message-status']
                row['message-content'] = '"%s"' % history['message-content']
                row['timestamp'] = '"%s"' % history['timestamp']
                ordered_row = []
                for header in headers:
                    ordered_row.append(row[header])
                csvfile.write("%s\n" % ','.join(ordered_row))
            csvfile.close()
