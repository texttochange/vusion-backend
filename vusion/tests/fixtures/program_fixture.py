import MySQLdb
from pymongo import MongoClient

from vusion.persist import ProgramSettingManager, ShortcodeManager
from tests.utils import ObjectMaker

class ProgramFixture(object, ObjectMaker):

    config = {
        'mysql_host': '127.0.0.1',
        'mysql_port': 3306,
        'mysql_user': 'cake_test',
        'mysql_passwd': 'password',
        'mysql_db': 'vusion_test',
        'mongo_db_vusion': 'vusion_test',
        'mongo_db_programs': ['myprogramDb', 'myprogramDb2', 'myprogramDb3']}

    def __init__(self):
        self.mysql = MySQLdb.connect(
            host= self.config['mysql_host'],
            port= self.config['mysql_port'],
            user= self.config['mysql_user'],
            passwd= self.config['mysql_passwd'],
            db= self.config['mysql_db'])
        self.mongo = MongoClient(w=1)
        self.shortcode_mgr = ShortcodeManager(
            self.mongo[self.config['mongo_db_vusion']], 'shortcodes') 
        
        self.setting_mgrs = []
        for i in range(0, 3):
            self.setting_mgrs.append(ProgramSettingManager(
                self.mongo[self.config['mongo_db_programs'][i]], 'program_settings'))

        self.clearData()
        self.create_program_table()
        self.fill_program_data()
        self.initialize_properties(0)

    def __del__(self):
        self.mongo.close()
        self.mysql.close()

    def initialize_properties(self, index, program_settings=None, shortcode=None, register_keyword=False):
        if shortcode is None:
            shortcode = self.mkobj_shortcode('8181', '256')
        self.shortcode_mgr.save(shortcode)
        if program_settings is None:
            program_settings = self.mk_program_settings('256-8181')
        for program_setting in program_settings:
            self.setting_mgrs[index].save(program_setting)

    def get_mysql(self):
        return self.mysql;

    def clearData(self):
        self._run("""DROP TABLE IF EXISTS programs""")
        for mgr in self.setting_mgrs:
            mgr.drop()
        self.shortcode_mgr.drop()

    def _run(self, query):
        c = self.mysql.cursor()
        c.execute(query)
        self.mysql.commit()
        c.close()

    def create_program_table(self):
        query = """CREATE TABLE programs (name VARCHAR(20), url VARCHAR(20),""" + \
            """`database` VARCHAR(20), status VARCHAR(20));"""
        self._run(query)

    def fill_program_data(self):
        c = self.mysql.cursor()
        c.executemany(
            """INSERT INTO programs (name, url, `database`, status) """ + \
            """VALUES (%s, %s, %s, %s);""",
            [
            ('my program', 'myprogramUrl', self.config['mongo_db_programs'][0], 'running'),
            ('my program 2', 'myprogramUrl2', self.config['mongo_db_programs'][1], 'archieved'),
            ('my program 3', 'myprogramUrl3', self.config['mongo_db_programs'][2], 'running'),
            ])
        self.mysql.commit()
        c.close()


