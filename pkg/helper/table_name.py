from core.entity.catalog import SCHEMA_PUBLIC
from typing import Tuple, Union


def normalize_schema_and_table_name(schema_name: Union[str, None], table_name: str) -> Tuple[str, str]:
    if '.' in table_name and schema_name is None:
        schema_name, table_name = table_name.split('.')
    else:
        schema_name = SCHEMA_PUBLIC
        if schema_name is not None:
            schema_name = schema_name

    return schema_name, table_name
