import json
from abc import abstractmethod, ABC
from aito.exceptions import BaseError
from jsonschema import ValidationError, Draft7Validator, validate
import logging
from typing import Any, Dict


LOG = logging.getLogger('JsonFormat')


class JsonValidationError(BaseError):
    """An error when validating a component

    """
    def __init__(self, component_name: str, base_error: ValidationError):
        """

        :param component_name: the name of the schema component
        :type component_name: str
        :param base_error: the original error
        :type base_error: ValidationError
        """
        self.component_name = component_name
        self.error = base_error
        instance = ''.join([f'[{repr(index)}]' for index in base_error.absolute_path])
        error_msg = base_error.message + f' on instance {instance}' if instance else base_error.message
        error_msg = f"[{self.component_name}] {error_msg}"
        super().__init__(error_msg, LOG)


class JsonFormat(ABC):
    """Base class for class that can be converted from and to JSON

    """
    @classmethod
    @abstractmethod
    def json_schema(cls):
        """the JSON schema of the class

        :rtype: Dict
        """
        pass

    @classmethod
    def json_schema_validate(cls, obj: Any):
        """Validate an object with the class json_schema
        Returns the object if validation success, else raise :class:`.JsonValidationError`

        :param obj: the object to be validated
        :type obj: Any
        :return: the object if validation succeed
        :rtype: Any
        """
        try:
            validate(instance=obj, schema=cls.json_schema(), cls=Draft7Validator)
        except ValidationError as err:
            raise JsonValidationError(cls.__name__, err) from None
        return obj

    def json_schema_validate_with_schema(self, obj: Any, schema: Dict):
        """Validate an object with the given schema

        :param obj: the object to be validated
        :type obj: Any
        :param schema: the schema to be validate against
        :type schema: Dict
        :return: the object if validation succeed
        :rtype: Any
        """
        try:
            validate(instance=obj, schema=schema, cls=Draft7Validator)
        except ValidationError as err:
            raise JsonValidationError(self.__class__.__name__, err) from None
        return obj

    @abstractmethod
    def to_json_serializable(self):
        """convert the object to an object that can be serialized to a JSON formatted string
        """
        pass

    def to_json_string(self, **kwargs):
        """convert the object to a JSON string

        :param kwargs: the keyword arguments for json.dumps method
        :rtype: str
        """
        return json.dumps(self.to_json_serializable(), **kwargs)

    @classmethod
    @abstractmethod
    def from_deserialized_object(cls, obj: Any):
        """create a class object from a JSON deserialized object
        """
        pass

    @classmethod
    def from_json_string(cls, json_string: str, **kwargs):
        """create an class object from a JSON string

        :param json_string: the JSON string
        :type json_string: str
        :param kwargs: the keyword arguments for json.loads method
        """
        return json.loads(json_string, object_hook=cls.from_deserialized_object, **kwargs)

    def __str__(self):
        return self.to_json_string()
