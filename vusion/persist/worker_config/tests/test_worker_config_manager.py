from pymongo import MongoClient

from twisted.trial.unittest import TestCase

from tests.utils import ObjectMaker

from vusion.persist import WorkerConfig, WorkerConfigManager


class TestWorkerConfigManager(TestCase, ObjectMaker):
    
    def setUp(self):
        self.database_name = 'test_vusion_db'
        c = MongoClient(w=1)
        db = c[self.database_name]
        self.manager = WorkerConfigManager(db, 'worker_configs')
        self.clearData()
    
    def tearDown(self):
        self.clearData()
   
    def clearData(self):
        self.manager.drop()

    def test_get_worker_configs(self):
        self.manager.save_document(self.mkobj_worker_config('worker1'))
        self.manager.save_document(self.mkobj_worker_config('worker2'))
        cursor = self.manager.get_worker_configs()
        self.assertEqual(2, cursor.count())
        worker1 = cursor.next()
        self.assertTrue(isinstance(worker1, WorkerConfig))
        worker2 = cursor.next()
        self.assertTrue(isinstance(worker2, WorkerConfig))
        

    def test_save_worker_config(self):
        worker1 = self.mkobj_worker_config('worker1')
        self.manager.save_document(worker1)
        
        worker1['config']['a key'] = 'a value'
        self.manager.save_worker_config(worker1)
        
        self.assertEqual(1, self.manager.count())
        worker1 = self.manager.find_one()
        self.assertTrue('a key' in worker1['config'])

    def test_remove_worker_config(self):
        worker1 = self.mkobj_worker_config('worker1')
        self.manager.save_document(worker1)
        worker2 = self.mkobj_worker_config('worker2')
        self.manager.save_document(worker2)
        
        self.manager.remove_worker_config('worker1')
        self.assertEqual(1, self.manager.count())
