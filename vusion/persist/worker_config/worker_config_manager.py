from vusion.persist import ModelManager, WorkerConfig
from vusion.persist.cursor_instanciator import CursorInstanciator


class WorkerConfigManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(WorkerConfigManager, self).__init__(db, collection_name, **kwargs)

    def get_worker_configs(self):
        def log(exception, item):
            self.log("Exception %s while instanciating a WorkerConfig %r" % (exception, item))
        return CursorInstanciator(self.collection.find(), WorkerConfig, [log])

    def get_worker_config(self, name):
        try:
            return WorkerConfig(**self.collection.find_one({'name': name}))
        except:
            self.log("Worker Config for %s cannot be found." % name)
            return None

    def save_worker_config(self, worker_config):
        if worker_config.is_already_saved():
            return self.save_document(worker_config)
            # need a update in case it's a overwriting from the config file
        return self.update(
            {'name': worker_config['name']},
            {'$set': worker_config.get_as_dict()},
            True)

    def remove_worker_config(self, worker_name):
        self.remove({'name': worker_name})
