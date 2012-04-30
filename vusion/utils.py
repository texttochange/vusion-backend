from datetime import datetime
import pytz
import time
import iso8601


def time_to_vusion_format(timestamp):
    return timestamp.strftime('%Y-%m-%dT%H:%M:%S')


def time_from_vusion_format(date_time_str):
    return iso8601.parse_date(date_time_str).replace(tzinfo=None)


def get_local_time(timezone):
    if timezone is None or timezone in pytz.all_timezones:
        return datetime.utcnow()
    return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(
        pytz.timezone(timezone)).replace(tzinfo=None)


def get_local_time_as_timestamp(local_time):
    return long(time.mktime(local_time.timetuple()))
