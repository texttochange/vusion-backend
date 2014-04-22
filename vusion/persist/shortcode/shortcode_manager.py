from vusion.persist import ModelManager


class ShortcodeManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(ShortcodeManager, self).__init__(db, collection_name)
        self.collection.ensure_index('shortcode')