from vusion.persist import ModelManager


class TemplateManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(TemplateManager, self).__init__(db, collection_name, **kwargs)