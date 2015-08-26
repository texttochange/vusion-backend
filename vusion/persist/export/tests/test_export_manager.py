from pymongo import MongoClient

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.persist import ExportManager, Export


class TestExportManager(TestCase, ObjectMaker):

    def setUp(self):
        self.database_name = 'test_vusion'
        c = MongoClient(w=1)
        db = c[self.database_name]
        self.manager = ExportManager(db, 'exports')
        self.clearData()

    def tearDown(self):
        self.clearData()

    def clearData(self):
        self.manager.drop()

    def test_failed(self):
        export = Export(**self.mkdoc_export())
        export_id = self.manager.save_object(export)

        self.manager.failed(export_id, 'something bad happend')

        export_saved = self.manager.get_export(export_id)
        self.assertEqual(export_saved['status'], 'failed')

    def test_success(self):
        export = Export(**self.mkdoc_export())
        export_id = self.manager.save_object(export)

        self.manager.success(export_id, 145L)

        export_saved = self.manager.get_export(export_id)
        self.assertEqual(export_saved['status'], 'success')

    def test_processing(self):
        export = Export(**self.mkdoc_export())
        export_id = self.manager.save_object(export)

        self.manager.processing(export_id)

        export_saved = self.manager.get_export(export_id)
        self.assertEqual(export_saved['status'], 'processing')

    def test_get_total_export_size(self):
        self.assertEqual(
            self.manager.get_total_export_size(), 0)

        export = Export(**self.mkdoc_export(
            status='failed', size=200L))
        self.manager.save_object(export)

        export = Export(**self.mkdoc_export(
            status='success', size=200L))
        self.manager.save_object(export)

        export = Export(**self.mkdoc_export(
            status='processing', size=200L))
        self.manager.save_object(export)

        export = Export(**self.mkdoc_export(
            status='no-space', size=200L))
        self.manager.save_object(export)

        self.assertEqual(
            self.manager.get_total_export_size(), 200L)