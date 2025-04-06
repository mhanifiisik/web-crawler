from loguru import logger

class Logger:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            logger.add("log.log", rotation="10MB", level="INFO")
        return cls._instance

    def info(self, message, *args):
        if args:
            logger.info(message, *args)
        else:
            logger.info(message)

    def debug(self, message, *args):
        if args:
            logger.debug(message, *args)
        else:
            logger.debug(message)

    def warning(self, message, *args):
        if args:
            logger.warning(message, *args)
        else:
            logger.warning(message)

    def error(self, message, *args):
        if args:
            logger.error(message, *args)
        else:
            logger.error(message)

    def critical(self, message, *args):
        if args:
            logger.critical(message, *args)
        else:
            logger.critical(message)
