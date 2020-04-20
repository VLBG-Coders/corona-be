import re


def map_date(date):
    """Maps a date in the form of mm/dd/yy to conform datetime string.

    Returns False, if date cannot be mapped.
    """
    re_result = re.search(
        '(\d{1,2})/(\d{1,2})/(\d{2})', date)
    if re_result:
        month, day, year = re_result.groups()
        # attention: this will only work for 80 years! ;)
        return f"20{year}-{int(month):02}-{int(day):02}"
    return False