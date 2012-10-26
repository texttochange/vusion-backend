"""Tests for vusion.persist.request."""

from bson import ObjectId

from twisted.trial.unittest import TestCase

from vusion.error import FailingModelUpgrade
from vusion.persist import WorkerConfig

from tests.utils import ObjectMaker


class TestWorkerConfig(TestCase, ObjectMaker):

    def test_process_fields(self):
        worker_config_unicode = {
            '_id': ObjectId(),
            'name': u'WorkerName',
            'class': u'vusion.TtcDialogueWorker',
            'config': {'database': u'something',
                       'id': 1}}
        worker_config = WorkerConfig(**worker_config_unicode)
        self.assertTrue(isinstance(worker_config['name'], str))
        self.assertTrue(isinstance(worker_config['config']['database'], str))
        self.assertTrue(isinstance(worker_config['_id'], ObjectId))
        self.assertEqual(worker_config['config']['id'], 1)

    def test_create_instance(self):
        worker_config = WorkerConfig()
        self.assertEqual(worker_config['name'], '')
        self.assertTrue(worker_config.get_version(), '2')

    def test_upgrade_form_1_to_2(self):
        worker_config_1 = {
            '_id': 'one Id',
            'name': 'WorkerName',
            'class': 'vusion.TtcGenericWorker',
            'config': {'database': 'something'}}

        worker = WorkerConfig(**worker_config_1)

        self.assertEqual(worker['class'], 'vusion.DialogueWorker')
        self.assertEqual(worker['_id'], 'one Id')

        worker_config_2 = {
            'name': 'WorkerName',
            'class': 'someclass',
            'config': {'database': 'something'}}

        worker = WorkerConfig(**worker_config_2)

        self.assertEqual(worker['class'], 'someclass')
