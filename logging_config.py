import logging


def setup_logging(filename='app.log', level=logging.INFO):
    """
    Настраивает логирование в файл и консоль.
    :param filename: имя файла для логов
    :param level: уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.FileHandler(filename, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info("Логирование настроено. Файл: %s", filename)
    return logger
