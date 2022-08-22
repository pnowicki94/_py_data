import json
import os

PARAM_FILE_DIRECTORY = '_py_data/config_db'  # ścieżka do folderu z plikami konfiguracyjnymi baz danych


def __database_config_file_directory(database):
    """

    :param database: nazwa pliku z konfiguracją połączenia do bazy danych
    :return: ścieżka do pliku z konfiguracja bazy danych
    """

    param_file_directory = os.getcwd().split('_py_data')[0] + PARAM_FILE_DIRECTORY

    files = [os.path.join(param_file_directory, file) for file in os.listdir(param_file_directory) if
             file == "{}.json".format(database)]
    try:
        return files[0]
    except IndexError:
        print("Brak pliku konfiguracyjnego")


def connection_parameters(database_name):
    """

    :param database_name: nazwa pliku z konfiguracją połączenia do bazy danych
    :return: słownik z atrybutami połączenia do bazy danych oraz z typem bazy danych
    """
    directory = __database_config_file_directory(database_name)
    with open(directory) as json_file:
        parameters = json.load(json_file)

    db_type = parameters['type']
    db_conn = parameters['params']

    return db_conn, db_type
