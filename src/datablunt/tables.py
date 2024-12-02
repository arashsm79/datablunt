from sqlalchemy import (
    create_engine,
    ForeignKeyConstraint
)
from sqlalchemy.sql.sqltypes import LargeBinary, Time, Uuid, String, Float, Boolean, Integer, DateTime, Date, Interval, Numeric, Enum
from datetime import datetime, date, timedelta, time
from decimal import Decimal
import uuid
import ipaddress
from pathlib import Path
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import mapped_column, registry, DeclarativeMeta
import sqlalchemy.orm
from sqlalchemy import select, except_, column

from typing import Generic, TypeVar, Any, get_origin, get_args, Type
from types import NoneType
from tqdm import tqdm

import pandas as pd


T = TypeVar('T')
mapper_registry = registry()

class Primary(Generic[T]):
    pass

def convert_python_type(python_type: Any) -> Any:
    if issubclass(python_type, Enum):
        return Enum(python_type)
    if issubclass(
        python_type,
        (
            str,
            ipaddress.IPv4Address,
            ipaddress.IPv4Network,
            ipaddress.IPv6Address,
            ipaddress.IPv6Network,
            Path
        ),
    ):
        return String
    if issubclass(python_type, float):
        return Float
    if issubclass(python_type, bool):
        return Boolean
    if issubclass(python_type, int):
        return Integer
    if issubclass(python_type, datetime):
        return DateTime
    if issubclass(python_type, date):
        return Date
    if issubclass(python_type, timedelta):
        return Interval
    if issubclass(python_type, time):
        return Time
    if issubclass(python_type, bytes):
        return LargeBinary
    if issubclass(python_type, Decimal):
        return Numeric
    if issubclass(python_type, uuid.UUID):
        return Uuid
    raise ValueError(f"{python_type} has no matching SQLAlchemy type")

class DataBluntMetaClass(DeclarativeMeta):

    def __new__(cls, name, bases, dct, parents=[], **kwargs: Any):
        # Set __table__ name for the database if __abstract__ is not set or False
        if not dct.get("__abstract__", False):
            dct["__tablename__"] =  name.lower()

        # If parents are specified, process them
        dct['_db_parents'] = parents
        for parent in parents:
            # Store all composite foreign key columns
            composite_fk_columns = []
            parent_pk_columns = inspect(parent).primary_key
            for pk in parent_pk_columns:
                # Add foreign key columns to the child class
                fk_column = mapped_column(
                    pk.name,
                    pk.type,
                    primary_key=True,
                )
                dct[pk.name] = fk_column
                composite_fk_columns.append(pk.name)

            if "__table_args__" not in dct:
                dct["__table_args__"] = ()
            table_args = list(dct["__table_args__"])
            table_args.append(
                ForeignKeyConstraint(
                    composite_fk_columns,
                    [f"{parent.__name__.lower()}.{fk_column}" for fk_column in composite_fk_columns],
                    ondelete="RESTRICT",
                    onupdate="CASCADE"
                )
            )
            dct["__table_args__"] = tuple(table_args)

            # TODO: See if it makes sense to add some relationships to the child class

        # Go through attribute annotations of the class and add
        # corresponding database mapped columns to the class
        for attr_name, attr_type in dct.get("__annotations__", {}).items():
            column_kwargs = {}
            attr_type_args = list(get_args(attr_type))
            if get_origin(attr_type) is Primary:
                column_kwargs["primary_key"] = True
                attr_type = attr_type_args[0]
            if NoneType in attr_type_args:
                column_kwargs["nullable"] = True
                attr_type_args.remove(NoneType)
                attr_type = next(iter(attr_type_args))

            dct[attr_name] = mapped_column(attr_name, convert_python_type(attr_type), **column_kwargs)

        # Remove all the type annotations from the class attributes
        dct.pop("__annotations__", None)
        # Call SQLAlchemy's __new__ method to create the actual non-meta class
        new_cls = super().__new__(cls, name, bases, dct)

        return new_cls

class DataBluntTable(metaclass=DataBluntMetaClass):
    __abstract__ = True
    registry = mapper_registry
    metadata = mapper_registry.metadata
    __init__ = mapper_registry.constructor

    @classmethod
    def valid_keys(cls, keys):
        valid_columns = {column.key for column in inspect(cls).mapper.column_attrs}
        return {key: value for key, value in keys.items() if key in valid_columns}

engine = create_engine("sqlite:////home/arashsm79/playground/datablunt/datablunltdb.sql", enable_from_linting=False)
DataBluntTable.metadata.create_all(engine)
session = sqlalchemy.orm.Session(engine)

class Manual(DataBluntTable):
    __abstract__ = True

class Computed(DataBluntTable):
    __abstract__ = True

    @classmethod
    def populate(cls, keys=None, make_kwargs=None):

        parent_classes: list[Type[DataBluntTable]] = cls._db_parents
        parent_keys = []

        for parent in parent_classes:
            if issubclass(parent, DataBluntTable):
                parent_keys += list(inspect(parent).primary_key)
        parent_keys = [column(key.name) for key in parent_keys]
        keys_query = except_(select(*parent_keys).select_from(*parent_classes), select(*parent_keys).select_from(cls))
        keys = [k._asdict() for k in session.execute(keys_query).all()]
        nkeys = len(keys)
        success_count = 0
        if nkeys:
            for key in tqdm(keys, desc=cls.__class__.__name__):
                try:
                    cls.make(cls, key, **(make_kwargs or {}))
                    success_count += 1
                except Exception as e:
                    raise e

        session.commit()
        return f'success_count: {success_count}'