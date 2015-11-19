import MySQLdb

from program import Program


class ProgramManager():

    def __init__(self, db):
        self.db = db

    def close_connection(self):
        self.db.close()

    def get_running(self):
        programs = []
        c = self.db.cursor()
        c.execute("SELECT name, url, `database`, status FROM programs "
                  "WHERE status = 'running'")
        for (name, url, database, status) in c.fetchall():
            program = Program(**{
                'name': name,
                'url': url,
                'database': database,
                'status': status})
            programs.append(program)
        return programs
