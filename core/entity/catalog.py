from pydantic import BaseModel
from typing import Union, List
from uuid import UUID

SCHEMA_PUBLIC = 'public'
COLUMN_NAME = 'column_name'

FOREIGN_TABLE_SCHEMA = 'foreign_table_schema'
FOREIGN_TABLE_NAME = 'foreign_table_name'
FOREIGN_COLUMN_NAME = 'foreign_column_name'

EMPTY_STRING = ''


class GetTableColumnsRequest(BaseModel):
    schema_name: str = 'public'
    table_name: str
    complete_attribute: bool = False


class GetTableAttributesRequest(BaseModel):
    schema_name: str = 'public'
    table_name: str
    complete_attribute: bool = False
    with_data: bool = False
    page: int = 1
    page_size: int = 10
    keyword: str = ""


class Condition(BaseModel):
    column_name: str
    operator: str
    value: Union[List[Union[str, int, float, bool, UUID]], str, int, float, bool, UUID]
    will_be_processed: bool = True


class Query(BaseModel):
    and_: Union[List[Condition], 'Query'] = []
    or_: Union[List[Condition], 'Query'] = []


class GetTableDataRequest(BaseModel):
    schema_name: str = 'public'
    table_name: str
    page: int = 1
    page_size: int = 10
    keyword: str = ""
    custom_column: str = ""
    id: Union[str, int, float, bool, UUID] = None
    ids: List[Union[str, int, float, bool, UUID]] = []
    is_composite_primary_key: bool = False
    primary_key_name: List[str] = []
    query: List[Union[Query, Condition]] = []


class CreateTableRecordRequest(BaseModel):
    schema_name: str = 'public'
    table_name: str
    data: dict
    return_id: bool = True
    primary_key_column: str = None
