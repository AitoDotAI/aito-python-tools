from logging import Logger
from typing import Union
import jsonschema


class BaseError(Exception):
    def __init__(self, message: str, logger: Logger):
        """

        :param message: the error message
        :type message: str
        """
        self.message = message

        super().__init__(self.message)
        logger.error(message)
        logger.debug(self.__repr__(), stack_info=True)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.message})"


class ValidationError(BaseError):
    """An error when validating a component

    """
    def __init__(self, component_name: str, error: Union[str, Exception], logger: Logger):
        """

        :param component_name: the name of the schema component
        :type component_name: str
        :param error: the original error
        :type error: Union[str, Exception]
        """
        self.component_name = component_name
        self.error = error
        if isinstance(error, jsonschema.exceptions.ValidationError):
            instance = ''.join([f'[{repr(index)}]' for index in error.absolute_path])
            error_msg = f"{error.message} on instance {instance}"
        else:
            error_msg = str(error)
        error_msg = f"[{self.component_name}] {error_msg}"

        super().__init__(error_msg, logger)
