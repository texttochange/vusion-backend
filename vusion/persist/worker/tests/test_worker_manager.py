from twisted.trial.unittest import TestCase

from vusion.persist import WorkerManager

import MySQLdb

class TestWorkerManager(TestCase):

    def setUp(self):
        query = """CREATE TABLE programs (name VARCHAR(20), url VARCHAR(20),""" + \
            """`database` VARCHAR(20), status VARCHAR(20));"""
        self.db = MySQLdb.connect(
            host='127.0.0.1',
            port=3306,
            user='cake_test',
            passwd="password",
            db="vusion_test")
        self.clearData()
        c = self.db.cursor()
        c.execute(query)
        c.close()

        self.manager = WorkerManager(self.db)

    def tearDown(self):
        self.clearData()

    def clearData(self):
        c = self.db.cursor()
        c.execute("""DROP TABLE IF EXISTS programs""")
        c.close()

    def test_get_running_workers(self):
        c = self.db.cursor()
        c.executemany(
            """INSERT INTO programs (name, url, `database`, status) """ + \
            """VALUES (%s, %s, %s, %s);""",
            [
            ('my program', 'myprogramUrl', 'myprogramDb', 'running'),
            ('my program 2', 'myprogramUrl2', 'myprogramDb2', 'archieved'),
            ('my program 3', 'myprogramUrl3', 'myprogramDb3', 'running'),
            ])
        self.db.commit()
        c.close()
        workers = self.manager.get_running_workers()
        self.assertEqual(['myprogramUrl', 'myprogramUrl3'], workers)
