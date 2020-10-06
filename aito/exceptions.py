from logging import Logger


class BaseError(Exception):
    def __init__(self, message: str, logger: Logger):
        """

        :param message: the error message
        :type message: str
        :param logger: the logger that will be used to lock the eror and stack trace
        :type logger: Logger
        """
        super().__init__(message)
        self.message = message
        logger.error(message)
        logger.debug(self.__repr__(), stack_info=True)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.message})"

