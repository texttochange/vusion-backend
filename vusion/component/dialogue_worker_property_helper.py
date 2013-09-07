from datetime import datetime
from time import mktime
from pytz import all_timezones, utc, timezone

from math import ceil
from copy import deepcopy

from vusion.utils import (get_shortcode_value, is_shortcode_address,
                          get_shortcode_international_prefix, time_to_vusion_format)
from vusion.error import MissingCode, MissingLocalTime
from vusion.persist.shortcode import Shortcode


class DialogueWorkerPropertyHelper(object):
    
    def __init__(self, setting_collection, shortcode_collection):
        self._init_properties()
        self.setting_collection = setting_collection
        self.shortcode_collection = shortcode_collection

    def _init_properties(self):
        self.properties = {
            'shortcode':None,
            'timezone': None,
            'shortcode-international-prefix': None,
            'shortcode-max-character-per-sms': None,        
            'default-template-closed-question': None,
            'default-template-open-question': None,
            'default-template-unmatching-answer': None,
            'unmatching-answer-remove-reminder': 0, 
            'customized-id': None,
            'double-matching-answer-feedback': None,
            'double-optin-error-feedback': None,
            'request-and-feedback-prioritized': None,
            'credit-type': None,
            'credit-number': None,
            'credit-from-date': None,
            'credit-to-date': None,
	    'sms-forwarding-allowed': 'full',
	}
    
    def __getitem__(self, key):
        return self.properties[key]

    def __setitem__(self, key, value):
        self.properties[key] = value

    def __contains__(self, key):
        return key in self.properties

    def load(self, callback_rules={}):
        old_properties = deepcopy(self.properties)
        self._init_properties()
        settings = self.setting_collection.find()
        for setting in settings:
            self[setting['key']] = setting['value']
        shortcode_address = self.properties['shortcode']
        if shortcode_address is None:
            return False
        conditions = {'shortcode': get_shortcode_value(shortcode_address)}
        if is_shortcode_address(self['shortcode']):
            conditions.update({'international-prefix': get_shortcode_international_prefix(shortcode_address)})
        shortcode = self.shortcode_collection.find_one(conditions)
        if shortcode is None:
            self._init_properties()
            raise MissingCode("No code matching %r." % conditions)
        shortcode = Shortcode(**shortcode)
        self['shortcode-international-prefix'] = shortcode['international-prefix']
        self['shortcode-max-character-per-sms'] = shortcode['max-character-per-sms']
        for key, callback in callback_rules.iteritems():
            if self[key] != old_properties[key]:
                callback()

    def is_ready(self):
        if self['shortcode'] is None or self['timezone'] is None:
            return False
        return True

    def use_credits(self, message_content):
        if self['shortcode-max-character-per-sms'] is None:
            return 0
        if len(message_content) == 0:
            return 1
        return int(ceil(float(len(message_content)) / float(self['shortcode-max-character-per-sms'])))
    
    def get_local_time(self, date_format='datetime', time_delta=None):
	try:
	    local_time = datetime.utcnow().replace(tzinfo=utc).astimezone(
	        timezone(self['timezone'])).replace(tzinfo=None)
	except:
	    local_time = datetime.utcnow().replace(tzinfo=None)
	if time_delta is not None:
	    local_time = local_time + time_delta
	if (date_format=='datetime'):
	    return local_time
	elif (date_format=='vusion' or date_format=='iso'):
	    return time_to_vusion_format(local_time)
	elif (date_format=="timestamp"):
	    return long("%s%s" % (long(mktime(local_time.timetuple())),local_time.microsecond))	
	else:
	    raise Exception('Datetime format %s is not supported' % date_format)

    def is_sms_forwarding_allowed(self):
	if self['sms-forwarding-allowed'] == "full":
	    return True
	else:
	    return False
	
