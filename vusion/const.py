import re


TAG_REGEX = re.compile('^[a-zA-Z0-9\s]+$')
LABEL_REGEX = re.compile('^[a-zA-Z0-9\s]+:[a-zA-Z0-9\s\.]+$')

PLUS_REGEX = re.compile("^\+")
ZEROS_REGEX = re.compile("^(0){1,2}")
