import os
import sys
import traceback
import codecs

from pymongo import MongoClient

from twisted.internet.defer import inlineCallbacks, maybeDeferred, returnValue

from vumi.persist.txredis_manager import TxRedisManager
from vumi.config import ConfigText, ConfigInt, ConfigFloat
from vumi.worker import BaseWorker
from vumi import log

from vusion.connectors import ReceiveExportWorkerControlConnector
from vusion.message import ExportWorkerControl
from vusion.persist import (ParticipantManager, HistoryManager,
                            ScheduleManager, UnmatchableReplyManager,
                            ExportManager, DialogueManager)
from vusion.component import BasicLogger


class ExportWorkerConfig(BaseWorker.CONFIG_CLASS):
    """Base config definition for applications.

    You should subclass this and add application-specific fields.
    """

    application_name = ConfigText(
        "The name this application instance will use to create its queues.",
        required=True, static=True)
    database = ConfigText(
        "The database to retrieve the exports collection.",
        required=True, static=True)
    max_total_export_megabytes = ConfigFloat(
        "The maximum number of Mo which could be used for exported files.",
        default=1000, static=True)
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
        self.mongo = MongoClient(
            self.config['mongodb_host'],
            self.config['mongodb_port'])
        db = self.mongo[self.config['database']]
        self.exports = ExportManager(db, 'exports')
        self.exports.set_log_helper(BasicLogger())

    def teardown_worker(self):
        d = self.pause_connectors()
        d.addCallback(lambda r: self.teardown_application())
        return d

    def teardown_application(self):
        pass

    def get_file_size(self, file_full_name):
        return os.path.getsize(file_full_name)

    def megabytes2bytes(self, megabytes):
        return long(megabytes * 1024 * 1024)

    def has_reach_space_limit(self):
        limit = self.megabytes2bytes(self.config['max_total_export_megabytes'])
        return self.exports.has_export_space(limit)

    @inlineCallbacks
    def dispatch_control(self, msg):
        try:
            log.debug("Exporting %r" % msg)
            export = self.exports.get_export(msg['export_id'])
            log.debug("Export details %r" % export)
            if export is None:
                raise Exception('Cannot retrieve export %s' % msg['export_id'])
            if self.has_reach_space_limit():
                self.exports.no_space(export['_id'])
                return
            self.exports.processing(export['_id'])
            if export.is_participants_export():
                yield self.export_participants(export)
            elif export.is_history_export():
                self.export_history(export)
            elif export.is_unmatchable_reply_export():
                self.export_unmatchable_reply(export)
            self.exports.success(
                export['_id'], self.get_file_size(export['file-full-name']))
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error = "Exception: %r" % traceback.format_exception(
                exc_type,
                exc_value,
                exc_traceback)
            log.error(error)
            if hasattr(e, 'strerror'):
                failure_reason = e.strerror
            else:
                failure_reason = e.message
            self.exports.failed(msg['export_id'], failure_reason)

    @inlineCallbacks
    def replace_join(self, conditions, schedule_mgr):
        if not isinstance(conditions, dict) and not isinstance(conditions, list):
            returnValue(conditions)
        if isinstance(conditions, list):
            result_conditions = []
            for item in conditions:
                r = yield self.replace_join(item, schedule_mgr)
                result_conditions.append(r)
            returnValue(result_conditions)
        else:
            result_conditions = {} 
            for key, value in conditions.iteritems():
                if key == '$join':
                    c = yield schedule_mgr.get_unique_participant_phones()
                    result_conditions['$in'] = []
                    for item in c:
                        result_conditions['$in'].append(item['_id'])
                else:
                    if isinstance(value, dict) or isinstance(value, list):
                        result_conditions[key] = yield self.replace_join(value, schedule_mgr)
                    else:
                        result_conditions[key] = value
            returnValue(result_conditions)

    @inlineCallbacks
    def export_participants(self, export):
        db = self.mongo[export['database']]
        schedule_mgr = ScheduleManager(db, 'schedules')
        dialogue_mgr = DialogueManager(db, 'dialogues')
        conditions = yield self.replace_join(export['conditions'], schedule_mgr)
        log.debug("running conditions %r" % conditions) 
        participant_mgr = ParticipantManager(db, export['collection'])
        headers = ['phone', 'tags'] 
        label_headers = yield participant_mgr.get_labels(conditions)
        ordered_label_headers = dialogue_mgr.get_labels_order_from_dialogues(label_headers)
        headers += ordered_label_headers
        with codecs.open(export['file-full-name'], 'wb', 'utf-8') as csvfile:
            log.debug("header of the export %r" % headers)
            csvfile.write("%s\n" % ','.join(headers))
            sort = export.get_order_pymongo()
            cursor = participant_mgr.get_participants(conditions, sort)
            row_template = dict((header, "") for header in headers)
            for participant in cursor:
                if participant is None:
                    log.debug("Trying to export none participant!!");
                    continue
                row = row_template.copy()
                row['phone'] = '"%s"' % participant['phone']
                row['tags'] = '"%s"' % ','.join(participant['tags'])
                for label in participant['profile']:
                    if label['label'] not in headers:
                        raise Exception(
                            'One label %s of %s was missed on %s'% (
                                label['label'],
                                participant['phone'],
                                export['conditions']))
                    row[label['label']] = '"%s"' % label['value']
                ordered_row = []
                for header in headers:
                    ordered_row.append(row[header])
                csvfile.write("%s\n" % ','.join(ordered_row))

    def export_history(self, export):
        db = self.mongo[export['database']]
        manager = HistoryManager(db, export['collection'], None, None)
        headers = ['participant-phone',
                   'message-direction',
                   'message-status',
                   'message-content',
                   'timestamp'] 
        with codecs.open(export['file-full-name'], 'wb', 'utf-8') as csvfile:
            csvfile.write("%s\n" % ','.join(headers))
            sort = export.get_order_pymongo()
            cursor = manager.get_historys(export['conditions'], sort=sort)
            row_template = dict((header, "") for header in headers)
            for history in cursor:
                if history is None:
                    continue
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

    def export_unmatchable_reply(self, export):
        db = self.mongo[export['database']]
        manager = UnmatchableReplyManager(db, export['collection'])
        headers = ['from',
                   'to',
                   'message-content',
                   'timestamp']
        with codecs.open(export['file-full-name'], 'wb', 'utf-8') as csv_file:
            csv_file.write("%s\n" % ','.join(headers))
            sort = export.get_order_pymongo()
            cursor = manager.get_unmatchable_replys(
                export['conditions'], sort=sort)
            row_template = dict((header, "") for header in headers)
            for unmatchable in cursor:
                if unmatchable is None:
                    continue
                row = row_template.copy()
                row['from'] = '"%s"' % unmatchable['participant-phone']
                row['to'] = '"%s"' % unmatchable['to']
                row['message-content'] = '"%s"' % unmatchable['message-content']
                row['timestamp'] = '"%s"' % unmatchable['timestamp']
                ordered_row = []
                for header in headers:
                    ordered_row.append(row[header])
                csv_file.write("%s\n" % ','.join(ordered_row))
