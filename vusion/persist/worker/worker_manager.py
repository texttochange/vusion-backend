import MySQLdb


class WorkerManager():

    def __init__(self, db):
        self.db = db

    def close_connection(self):
        self.db.close()

    def get_running_workers(self):
        workers = []
        c = self.db.cursor()
        c.execute("""SELECT url FROM programs
                  WHERE status = 'running'""")
        for (url,) in c.fetchall():
            workers.append(url)
        return workers
