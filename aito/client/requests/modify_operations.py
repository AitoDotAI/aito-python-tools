"""Helper classes for Aito Modify API operations.

These classes provide a fluent interface for building modify queries::

    from aito.client.requests import Insert, Update, Delete
    import aito.api as api

    # Insert a single entry
    api.modify(client, Insert("products", {"id": "1", "name": "Apple"}))

    # Insert multiple entries
    entries = [{"id": "1", "name": "Apple"}, {"id": "2", "name": "Banana"}]
    api.modify(client, Insert("products", entries))

    # Update entries matching a condition
    api.modify(client, Update("products").where({"id": "1"}).set({"name": "New Name"}))

    # Update with upsert (insert if not exists)
    api.modify(client, Update("products").where({"id": "1"}).set({"name": "Name"}).upsert())

    # Delete entries matching a condition
    api.modify(client, Delete("products", {"id": "1"}))

    # Multiple operations atomically
    ops = [Insert("products", {"id": "1", "name": "Apple"}),
           Update("products").where({"id": "2"}).set({"name": "Banana"}),
           Delete("products", {"id": "3"})]
    api.modify(client, ops)
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Union


class ModifyOperation(ABC):
    """Base class for modify operations."""

    @abstractmethod
    def to_query(self) -> Dict:
        """Convert the operation to a query dictionary."""
        pass


class Insert(ModifyOperation):
    """Insert operation for the Modify API.

    Example::

        # Insert single entry
        Insert("products", {"id": "1", "name": "Apple"})

        # Insert multiple entries
        Insert("products", [{"id": "1"}, {"id": "2"}])
    """

    def __init__(self, table: str, entries: Union[Dict, List[Dict]]):
        """
        :param table: The name of the table to insert into
        :type table: str
        :param entries: A single entry dict or a list of entry dicts to insert
        :type entries: Union[Dict, List[Dict]]
        """
        self.table = table
        self.entries = entries

    def to_query(self) -> Dict:
        return {
            "into": self.table,
            "insert": self.entries
        }


class Update(ModifyOperation):
    """Update operation for the Modify API.

    Uses a fluent interface::

        Update("products").where({"id": "1"}).set({"name": "New Name"})

        # With upsert (insert if not exists)
        Update("products").where({"id": "1"}).set({"name": "Name"}).upsert()
    """

    def __init__(self, table: str):
        """
        :param table: The name of the table to update
        :type table: str
        """
        self._table = table
        self._where: Dict = None
        self._set: Dict = None
        self._upsert: bool = False

    def where(self, condition: Dict) -> 'Update':
        """Set the condition for which entries to update.

        :param condition: The condition to match entries
        :type condition: Dict
        :return: self for chaining
        :rtype: Update
        """
        self._where = condition
        return self

    def set(self, fields: Dict) -> 'Update':
        """Set the fields to update.

        :param fields: The fields and values to set
        :type fields: Dict
        :return: self for chaining
        :rtype: Update
        """
        self._set = fields
        return self

    def upsert(self, enable: bool = True) -> 'Update':
        """Enable upsert mode (insert if not exists).

        :param enable: Whether to enable upsert, defaults to True
        :type enable: bool
        :return: self for chaining
        :rtype: Update
        """
        self._upsert = enable
        return self

    def to_query(self) -> Dict:
        if self._where is None:
            raise ValueError("Update operation requires a where() condition")
        if self._set is None:
            raise ValueError("Update operation requires set() fields")

        query = {
            "update": self._table,
            "where": self._where,
            "set": self._set
        }
        if self._upsert:
            query["upsert"] = True
        return query


class Delete(ModifyOperation):
    """Delete operation for the Modify API.

    Example::

        Delete("products", {"id": "1"})
    """

    def __init__(self, table: str, condition: Dict):
        """
        :param table: The name of the table to delete from
        :type table: str
        :param condition: The condition to match entries to delete
        :type condition: Dict
        """
        self.table = table
        self.condition = condition

    def to_query(self) -> Dict:
        return {
            "from": self.table,
            "delete": self.condition
        }
