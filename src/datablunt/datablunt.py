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

    def __new__(cls, name, bases, dct, parents=None, **kwargs: Any):
        # Set __table__ name for the database if __abstract__ is not set or False
        if not dct.get("__abstract__", False):
            dct["__tablename__"] =  name.lower()

        # Store all composite primary key columns
        composite_pk_columns = []
        # If parents are specified, process them
        if parents:
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

                # TODO: Add relationships to the child class
                # relationship_name = parent.__name__.lower()
                # dct[relationship_name] = relationship(parent)

        # Go through attribute annotations of the class and add
        # corresponding database mapped columns to the class
        for attr_name, attr_type in dct.get("__annotations__", {}).items():
            column_kwargs = {}
            attr_type_args = list(get_args(attr_type))
            if get_origin(attr_type) is Primary:
                attr_type = attr_type_args[0]
                composite_pk_columns.append(attr_name)
            if NoneType in attr_type_args:
                column_kwargs["nullable"] = True
                attr_type_args.remove(NoneType)
                attr_type = next(iter(attr_type_args))

            column = mapped_column(attr_name, convert_python_type(attr_type), **column_kwargs)
            dct[attr_name] = column

        # Define composite primary key constraint
        if composite_pk_columns:
            dct["__table_args__"] = (
                PrimaryKeyConstraint(*composite_pk_columns),
            )

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
    recording_date: Primary[str]
    duration: float

class Subject(Manual):
    subject_id: Primary[int]
    name: str

class Session(Manual, parents = [Recording, Subject]):
    session_id: Primary[int]
    session_date: str

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
        session.add(new_video)

        # Create a new recording instance
        new_recording = Recording(recording_id=1, recording_date="2023-10-01", duration=120.0)
        session.add(new_recording)

        # Create a new subject instance
        new_subject = Subject(subject_id=1, name="John Doe")
        session.add(new_subject)

        # Create a new session instance
        new_session = Session(session_id=1, session_date="2023-10-02", recording_id=1, recording_date="2023-10-01", subject_id=1)
        session.add(new_session)

        # Commit all the new instances
        session.commit()

        # Query the video to verify it was added
        added_video = session.query(Video).filter_by(video_id=1).first()
        print(f"Added video: {added_video.name}, {added_video.path}")

        # Query the recording to verify it was added
        added_recording = session.query(Recording).filter_by(recording_id=1).first()
        print(f"Added recording: {added_recording.recording_date}, {added_recording.duration}")

        # Query the subject to verify it was added
        added_subject = session.query(Subject).filter_by(subject_id=1).first()
        print(f"Added subject: {added_subject.name}")

        # Query the session to verify it was added
        added_session = session.query(Session).filter_by(session_id=1).first()
        print(f"Added session: {added_session.session_date}, Recording ID: {added_session.recording_id}, Subject ID: {added_session.subject_id}")