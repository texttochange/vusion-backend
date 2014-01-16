from unidecode import unidecode


def clean_keyword(keyword):
    return unidecode(keyword).lower()