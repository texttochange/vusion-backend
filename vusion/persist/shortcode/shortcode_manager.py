import re

from vusion.persist import ModelManager
from shortcode import Shortcode


class ShortcodeManager(ModelManager):
    
    def __init__(self, db, collection_name, **kwargs):
        super(ShortcodeManager, self).__init__(db, collection_name)
        self.collection.ensure_index('shortcode')

    def get_shortcode(self, to_addr, from_addr):
        matching_code = None
        codes = self.collection.find({'shortcode': to_addr})
        if codes is None or codes.count() == 0:
            self.log_helper.err("Could not find shortcode for %s" % to_addr)
            return None
        elif codes.count() == 1:
            return Shortcode(**codes[0])
        else:
            for code in codes:
                regex = re.compile(('^\+%s' % code['international-prefix']))
                if re.match(regex, from_addr):
                    return Shortcode(**code)
        self.log_helper.err("Could not find shortcode for %s with %s " %
                            (to_addr, code['international-prefix']))
        return None
