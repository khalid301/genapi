from abc import ABC, abstractmethod
from core.entity.catalog import Condition, Query, GetTableColumnsRequest, EMPTY_STRING
from typing import Union

class QueryParser(ABC):
    def __init__(self) -> None:
        super().__init__()

    def value_is_number_or_bool(self, value) -> bool:
        return type(value) == int or type(value) == float or type(value) == bool

    def audition_insert_column(self, columns: list[dict], data: dict) -> dict:
        columns_map = {}
        for column in columns:
            columns_map[column['column_name']] = True

        for key in list(data.keys()):
            if key not in columns_map:
                del data[key]

        return data

    def audition_filter_columns(self, columns: list[str], query: list[Union[Query, Condition]]) -> list[str]:
        for query_item in query:
            for condition_or_query_and in query_item.and_:
                if isinstance(condition_or_query_and, Condition):
                    if condition_or_query_and.column_name not in columns:
                        condition_or_query_and.will_be_processed = False

            for condition_or_query_or in query_item.or_:
                if isinstance(condition_or_query_or, Condition):
                    if condition_or_query_or.column_name not in columns:
                        condition_or_query_or.will_be_processed = False

        where_clauses = [self.build_query(query) for query in query]

        # remove where_clauses member if empty
        where_clauses = [clause for clause in where_clauses if clause]

        return where_clauses

    def build_condition(self, condition: Condition) -> str:
        if not condition.will_be_processed:
            return EMPTY_STRING

        if condition.operator.lower() == 'IN'.lower():
            if self.value_is_number_or_bool(condition.value):
                return f"{condition.column_name} {condition.operator} ({', '.join([str(v) for v in condition.value])})"
            else:
                in_string = ', '.join([f"'{v}'" for v in condition.value])
                return f"{condition.column_name} {condition.operator} ({in_string})"
        else:
            if self.value_is_number_or_bool(condition.value):
                return f"{condition.column_name} {condition.operator} {condition.value}"
            else:
                return f"{condition.column_name} {condition.operator} '{condition.value}'"

    def build_query(self, query: Query) -> str:
        parts = []

        for condition_or_query in query.and_:
            if isinstance(condition_or_query, Condition):
                query_str = self.build_condition(condition_or_query)

                if len(query_str) > 1:
                    parts.append(self.build_condition(condition_or_query))
            else:
                query_str = self.build_query(condition_or_query)

                if len(query_str) > 1:
                    parts.append(query_str)

        and_query = ' AND '.join(parts)

        parts = []

        for condition_or_query in query.or_:
            if isinstance(condition_or_query, Condition):
                query_str = self.build_condition(condition_or_query)

                if len(query_str) > 1:
                    parts.append(query_str)
            else:
                query_str = self.build_query(condition_or_query)

                if len(query_str) > 1:
                    parts.append(query_str)

        or_query = ' OR '.join(parts)

        if and_query and or_query and len(and_query) > 1 and len(or_query) > 1:
            return f"({and_query}) AND ({or_query})"
        elif and_query:
            if len(and_query) > 1:
                return f"({and_query})"
            else:
                return EMPTY_STRING
        else:
            if len(or_query) > 1:
                return f"({or_query})"
            else:
                return EMPTY_STRING
