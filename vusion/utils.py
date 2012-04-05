from datetime import datetime

def time_to_vusion_format(timestamp):
    return timestamp.strftime('%Y-%m-%dT%H:%M:%S')

def get_local_time(timezone):
    return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone(timezone)).replace(tzinfo=None)
