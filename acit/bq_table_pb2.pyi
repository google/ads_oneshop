from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

BIGQUERY_OPTS_FIELD_NUMBER: _ClassVar[int]
DESCRIPTOR: _descriptor.FileDescriptor
bigquery_opts: _descriptor.FieldDescriptor

class BigQueryMessageOptions(_message.Message):
    __slots__ = ["extra_fields", "table_name", "use_json_names"]
    EXTRA_FIELDS_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    USE_JSON_NAMES_FIELD_NUMBER: _ClassVar[int]
    extra_fields: _containers.RepeatedScalarFieldContainer[str]
    table_name: str
    use_json_names: bool
    def __init__(self, table_name: _Optional[str] = ..., use_json_names: bool = ..., extra_fields: _Optional[_Iterable[str]] = ...) -> None: ...
