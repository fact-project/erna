import logging


def setup_logging(level, name='erna'):
    logger = logging.getLogger(name)

    level = logging.getLevelName(level)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logging.captureWarnings(True)
