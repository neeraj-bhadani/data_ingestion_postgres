import os
import logging

def set_custom_logger():
    global LOGGER

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)
    module_name = __name__.replace('.', '_')
    log_path = f'logs/{module_name}_log.txt'

    log_dir = os.path.dirname(log_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.ERROR)

    fileHandler = logging.FileHandler(log_path, mode='w')
    fileHandler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s File_name: %(filename)s Function_name: %(funcName)s Line_no: %(lineno)d Message: %(message)s',
        datefmt='%d/%m/%Y %I:%M:%S %p'
    )
    consoleHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)

    LOGGER.addHandler(fileHandler)
    LOGGER.addHandler(consoleHandler)

    return LOGGER
