from unidecode import unidecode


def clean_keyword(keyword):
    if isinstance(keyword, str):
        keyword = keyword.decode('utf-8')
    return unidecode(keyword).lower()