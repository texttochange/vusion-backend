from datetime import datetime, timedelta, time
import pytz
from time import mktime
import iso8601
import re
from unidecode import unidecode

from vusion.const import PLUS_REGEX, ZEROS_REGEX


def get_default(kwargs, field, default_value):
    return kwargs[field] if field in kwargs else default_value


def get_local_code(from_addr):
    return (from_addr or '').split('-')[1]


def clean_keyword(keyword):
    if isinstance(keyword, str):
        keyword = keyword.decode('utf-8')
    return unidecode(keyword).lower()


def time_to_vusion_format(timestamp):
    return timestamp.strftime('%Y-%m-%dT%H:%M:%S')


def time_to_vusion_format_date(timestamp):
    return timestamp.strftime('%Y-%m-%d')


def get_now_timestamp():
    return time_to_vusion_format(datetime.now())


def time_from_vusion_format(date_time_str):
    return iso8601.parse_date(date_time_str).replace(tzinfo=None)


def date_from_vusion_format(date_time_str):
    return time_from_vusion_format(date_time_str).replace(hour=0, minute=0, second=0)


def get_local_time(timezone):
    if timezone is None or timezone in pytz.all_timezones:
        return datetime.utcnow()
    return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(
        pytz.timezone(timezone)).replace(tzinfo=None)


def get_local_time_as_timestamp(local_time):
    return long("%s%s" % (long(mktime(local_time.timetuple())),
                          local_time.microsecond))

##TODO rename is_prefixed_code_a_shortcode
def is_shortcode_address(address):
    if address is None:
        return False
    regex_NATIONAL_SHORTCODE = re.compile('^[0-9]+-[0-9]+$')
    if re.match(regex_NATIONAL_SHORTCODE, address):
        return True
    return False

##TODO rename is_prefixed_code_a_longcode
def is_longcode_address(address):
    regex_LONGCODE = re.compile('/^\+[0-9]+$/')
    if re.match(regex_LONGCODE, address):
        return True
    return False

##TODO rename from_prefixed_code_to_code
def get_shortcode_value(shortcode):
    if shortcode is None :
        return None
    if is_shortcode_address(shortcode):
        return shortcode.split('-')[1]
    return shortcode

##TODO rename from_prefixed_code_to_prefix
def get_shortcode_international_prefix(shortcode):
    if shortcode is None :
        return None
    if is_shortcode_address(shortcode):
        return shortcode.split('-')[0]
    return shortcode

##TODO move function in Shortcode model
def get_shortcode_address(shortcode):
    if shortcode['supported-internationally'] == 0:
        return ("%s-%s" % (shortcode['international-prefix'], shortcode['shortcode']))
    return shortcode['shortcode']


def get_offset_date_time(reference_time, days, at_time):
    sending_day = reference_time + timedelta(days=int(days))
    time_of_sending = at_time.split(':', 1)
    return datetime.combine(sending_day, time(int(time_of_sending[0]), int(time_of_sending[1])))


def split_keywords(keywords):
    return [k.lower() for k in (keywords or '').split(', ')]


def add_char_to_pattern(string, pattern):
    regex = re.compile('[a-zA-Z]')
    l = list(string)
    for index, char in enumerate(l):
        if regex.match(char):
            l[index] = "%%%s" % char
    return ''.join(l)


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def get_word(content, position=0, delimiter=' '):
    splits = (content or '').split(delimiter)
    if position < len(splits):
        return splits[position]
    return None


def get_words(content, start, end, delimiter=' '):
    start = start - 1
    splits = (content or '').split(delimiter)
    if start < len(splits):
        if end == 'end':
            return ' '.join(splits[start:])
        else:
            end = int(end)
            return ' '.join(splits[start:end])
    return None


def clean_phone(phone):
    if phone in [None,'']:
        return None
    if (re.match(ZEROS_REGEX, phone)):
        return re.sub(ZEROS_REGEX, "+", phone)
    if (not re.match(PLUS_REGEX, phone)):
        return '+%s' % phone
    return phone

def dynamic_content_notation_to_string(domain, keys):
    tmp = domain
    for key, value in sorted(keys.iteritems()):
        tmp = '%s.%s' % (tmp, value)
    return '[%s]' % tmp


#TODO remove DataLayerUtils in tests package
class DataLayerUtils:

    def __init__(self):
        self.collections = {}

    def setup_collections(self, names):
        for name in names:
            self.setup_collection(name)

    def setup_collection(self, name):
        if name in self.db.collection_names():
            self.collections[name] = self.db[name]
        else:
            self.collections[name] = self.db.create_collection(name)

    def drop_collections(self):
        for name, collection in self.collections.items():
            collection.drop()
