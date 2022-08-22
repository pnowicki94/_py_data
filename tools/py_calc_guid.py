import uuid


def calc_guid():
    return '{' + str(uuid.uuid4()).upper() + '}'
