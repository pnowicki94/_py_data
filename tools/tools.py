import os


class MyIterator:
    def __init__(self):
        self.idx = 1

    def __iter__(self):
        return self

    def __next__(self):
        result = self.idx
        self.idx += 1
        return result


my_iterator = MyIterator()


def try_except(func):
    def try_(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ValueError, ConnectionError) as err:
            print(func.__name__, err)
        except Exception as err:
            print('Inny error:', func.__name__, err)

    return try_


def write_log(dir_name, file_name, data):
    """

    :param dir_name: docelowy folder
    :param file_name: nazwa pliku
    :param data: dane do zapisania wpliku
    :return: None
    """
    with open(f'../{dir_name}' + os.sep + file_name, 'w', encoding="utf-8") as f:
        f.write(data)


def write_script(dir_name, file_name, data):
    """

    :param dir_name: docelowy folder
    :param file_name: nazwa pliku
    :param data: dane do zapisania wpliku
    :return:
    """
    if not os.path.exists(f'../{dir_name}'):
        os.makedirs(f'../{dir_name}')
    write_log(dir_name, file_name, data)


def generate_id(last_value):
    gen_id = MyIterator()
    if last_value:
        if type(last_value) is int:
            gen_id.idx = last_value + 1
        else:
            gen_id.idx = last_value
    else:
        gen_id.idx = 1
    return gen_id
