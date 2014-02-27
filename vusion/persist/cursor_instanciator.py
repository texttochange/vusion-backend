from vusion.error import VusionError

class CursorInstanciator(object):

    def __init__(self, cursor, instanciator_callback, failure_callbacks=[]):
        self.cursor = cursor
        self.instanciator_callback = instanciator_callback
        self.failure_callbacks = failure_callbacks

    def __iter__(self):
        return self

    def next(self):
        try:
            item = self.cursor.next()
            return self.instanciator_callback(**item)
        except VusionError as e:
            for failure_callback in self.failure_callbacks:
                failure_callback(e, item)

    def add_failure_callback(self, callback):
        self.failure_callbacks.append(callback)

    def __getattr__(self,attr):
        orig_attr = self.cursor.__getattribute__(attr)
        if callable(orig_attr):
            def hooked(*args, **kwargs):
                result = orig_attr(*args, **kwargs)
                # prevent wrapped_class from becoming unwrapped
                if result == self.cursor:
                    return self
                return result
            return hooked
        else:
            return orig_attr
