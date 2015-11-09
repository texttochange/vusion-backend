from vusion.persist import ModelManager


class ProgramSettingManager(ModelManager):

    def __init__(self, db, collection_name, **kwargs):
        super(ProgramSettingManager, self).__init__(db, collection_name, **kwargs)

    def get_setting(self, name):
        pass