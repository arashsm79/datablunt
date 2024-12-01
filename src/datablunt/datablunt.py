from sqlalchemy import (
    ForeignKey,
    create_engine,
    PrimaryKeyConstraint, 
)
from sqlalchemy.sql.sqltypes import LargeBinary, Time, Uuid, String, Float, Boolean, Integer, DateTime, Date, Interval, Numeric, Enum
from datetime import datetime, date, timedelta, time
from decimal import Decimal
import uuid
import ipaddress
from pathlib import Path
from sqlalchemy.orm import relationship
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import mapped_column, registry, DeclarativeBase, DeclarativeMeta
import sqlalchemy.orm

from typing import Generic, TypeVar, Any, get_origin, get_args
from types import NoneType


T = TypeVar('T')

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

    # Replicate SQLAlchemy
    def __setattr__(cls, name: str, value: Any) -> None:
        DeclarativeMeta.__setattr__(cls, name, value)

    def __delattr__(cls, name: str) -> None:
        DeclarativeMeta.__delattr__(cls, name)

    def __new__(cls, name, bases, dct, parents=None, **kwargs: Any):

        # Set __table__ name for the database if __abstract__ is not set or False
        if not dct.get("__abstract__", False):
            dct["__tablename__"] =  name.lower()

        # If parents are specified, process them
        if parents:
            # Store all composite primary key columns
            composite_pk_columns = []
            for parent in parents:
                parent_pk_columns = inspect(parent).primary_key
                for pk in parent_pk_columns:
                    # Add foreign key columns to the child class
                    fk_column = mapped_column(
                        pk.name,
                        pk.type,
                        ForeignKey(f"{parent.__tablename__}.{pk.name}"),
                    )
                    dct[pk.name] = fk_column
                    composite_pk_columns.append(pk.name)

                # Add relationships to the child class
                relationship_name = parent.__name__.lower()
                dct[relationship_name] = relationship(parent)

            # Define composite primary key constraint
            if composite_pk_columns:
                dct["__table_args__"] = (
                    PrimaryKeyConstraint(*composite_pk_columns),
                )

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

            column = mapped_column(attr_name, convert_python_type(attr_type), **column_kwargs)
            dct[attr_name] = column

        # Remove all the type annotations from the class attributes
        dct.pop("__annotations__", None)
        # Call SQLAlchemy's __new__ method to create the actual non-meta class
        if 'Video' in name:
            pass

        new_cls = super().__new__(cls, name, bases, dct)

        return new_cls


class DataBluntTable(DeclarativeBase, metaclass=DataBluntMetaClass):
    __abstract__ = True

class Manual(DataBluntTable):
    __abstract__ = True

class Computed(DataBluntTable):
    __abstract__ = True


class Video(Manual):
    video_id: Primary[int]
    name: Primary[str]
    path: str | None

class Recording(Manual):
    recording_id: Primary[int]
    date: Primary[str]
    duration: float

class Subject(Manual):
    subject_id: Primary[int]
    name: str

class Session(Manual, parents = [Recording, Subject]):
    session_id: Primary[int]
    date: str

class Pose(Computed, parents = [Session]):
    frame: int
    
    def make():
        pass

if __name__ == "__main__":
    engine = create_engine("sqlite:///datablunltdb.sql")
    DataBluntTable.metadata.create_all(engine)

    with sqlalchemy.orm.Session(engine) as session:
        # Create a new video instance
        new_video = Video(video_id=1, name="Sample Video", path="/path/to/video")

        # Add the new video to the session and commit
        session.add(new_video)
        session.commit()

        # Query the video to verify it was added
        added_video = session.query(Video).filter_by(video_id=1).first()
        print(f"Added video: {added_video.name}, {added_video.path}")