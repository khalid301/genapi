from abc import ABC
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from pkg.conn.database import engine
from core.entity.catalog import GetTableColumnsRequest, GetTableDataRequest, GetTableAttributesRequest, CreateTableRecordRequest, Condition
from core.repository.catalog_repository.query_parser import QueryParser


class CatalogRepository(ABC):
    def __init__(self) -> None:
        super().__init__()

        self.QueryParser = QueryParser()

        _session = sessionmaker(bind=engine)
        self.session = _session()

    async def get_primary_key(self, req: GetTableAttributesRequest):
        if '.' in req.table_name:
            schema_name, table_name = req.table_name.split('.')
        else:
            schema_name = req.schema_name
            table_name = req.table_name

        query = text(f'''
            SELECT               
              pg_attribute.attname, 
              format_type(pg_attribute.atttypid, pg_attribute.atttypmod) 
            FROM pg_index, pg_class, pg_attribute, pg_namespace 
            WHERE 
              pg_class.oid = '{schema_name}.{table_name}'::regclass AND 
              indrelid = pg_class.oid AND 
              pg_class.relnamespace = pg_namespace.oid AND 
              pg_attribute.attrelid = pg_class.oid AND 
              pg_attribute.attnum = any(pg_index.indkey)
             AND indisprimary
        ''')
        result_proxy = self.session.execute(query)

        # Get the column names from the ResultProxy
        column_names = result_proxy.keys()

        # Fetch all rows as a list of tuples
        rows = result_proxy.fetchall()

        # Convert each tuple to a dictionary using the column names as keys
        return [dict(zip(column_names, row)) for row in rows]

    async def get_table_columns(self, req: GetTableColumnsRequest):
        select_column = "ordinal_position, column_name, is_nullable, data_type"
        if req.complete_attribute:
            select_column = "*"

        query = text(f'''SELECT {select_column} 
                        FROM information_schema.columns 
                        WHERE table_name = :table_name 
                        ORDER BY ordinal_position''')
        result_proxy = self.session.execute(query, {'table_name': req.table_name})

        # Get the column names from the ResultProxy
        column_names = result_proxy.keys()

        # Fetch all rows as a list of tuples
        rows = result_proxy.fetchall()

        # Convert each tuple to a dictionary using the column names as keys
        return [dict(zip(column_names, row)) for row in rows]

    async def get_table_foreign_keys(self, req: GetTableColumnsRequest):
        if '.' in req.table_name:
            schema_name, table_name = req.table_name.split('.')
        else:
            schema_name = req.schema_name
            table_name = req.table_name

        query = text(f'''SELECT
                            tc.table_schema, 
                            tc.constraint_name, 
                            tc.table_name, 
                            kcu.column_name, 
                            ccu.table_schema AS foreign_table_schema,
                            ccu.table_name AS foreign_table_name,
                            ccu.column_name AS foreign_column_name 
                        FROM information_schema.table_constraints AS tc 
                        JOIN information_schema.key_column_usage AS kcu
                            ON tc.constraint_name = kcu.constraint_name
                            AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.constraint_column_usage AS ccu
                            ON ccu.constraint_name = tc.constraint_name
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                            AND tc.table_schema=:schema_name
                            AND tc.table_name=:table_name''')
        result_proxy = self.session.execute(query, {'schema_name': schema_name, 'table_name': table_name})

        # Get the column names from the ResultProxy
        column_names = result_proxy.keys()

        # Fetch all rows as a list of tuples
        rows = result_proxy.fetchall()

        print("rows", rows)

        # Convert each tuple to a dictionary using the column names as keys
        return [dict(zip(column_names, row)) for row in rows]

    async def get_table_data(self, req: GetTableDataRequest):
        try:
            if '.' in req.table_name:
                schema_name, table_name = req.table_name.split('.')
            else:
                schema_name = req.schema_name
                table_name = req.table_name

            query = f'''SELECT * FROM {schema_name}.{table_name}'''

            # generate query based on req.Query
            if req.query:
                # first we need to check if column in query exist in table, if not, we will ignore it
                req_attr = GetTableColumnsRequest(
                    schema_name=schema_name,
                    table_name=table_name
                )
                columns = await self.get_table_columns(req_attr)

                # get column names
                column_names = [column['column_name'] for column in columns]

                # filter query based on column names
                where_clauses = self.QueryParser.audition_filter_columns(column_names, req.query)

                # check if where_clauses is not empty
                if where_clauses:
                    query = query + ' WHERE ' + ' AND '.join(where_clauses)

            # implement pagination
            if req.page and req.page_size:
                # append limit and offset
                query = query + f' LIMIT {req.page_size} OFFSET {(req.page - 1) * req.page_size}'

            result_proxy = self.session.execute(text(query))

            # Get the column names from the ResultProxy
            column_names = result_proxy.keys()

            # Fetch all rows as a list of tuples
            rows = result_proxy.fetchall()

            # Convert each tuple to a dictionary using the column names as keys
            return [dict(zip(column_names, row)) for row in rows]
        except SQLAlchemyError as e:
            # Return the error message as a response
            return {'error': str(e)}

    async def get_table_data_by_id(self, req: GetTableDataRequest):
        if '.' in req.table_name:
            schema_name, table_name = req.table_name.split('.')
        else:
            schema_name = req.schema_name
            table_name = req.table_name

        # return error if id is not provided
        if req.id is None and req.is_composite_primary_key is False:
            return {'error': 'ID value is required'}

        if len(req.ids) == 0 and req.is_composite_primary_key is True:
            return {'error': 'IDs value is required'}

        # get primary key name
        primary_key_names = req.primary_key_name

        # Prepare the WHERE clause for the query
        if req.is_composite_primary_key:
            # For composite keys, create a list of conditions and join them with AND
            conditions = [f"{key} = :{key}" for key in primary_key_names]
            where_clause = " AND ".join(conditions)
        else:
            # For single keys, just use the key name
            if req.custom_column is not None:
                primary_key_names = [req.custom_column]

            where_clause = f"{primary_key_names[0]} = :id"

        query = text(f'''SELECT * 
                        FROM {schema_name}.{table_name} 
                        WHERE {where_clause}
                        LIMIT 1''')

        # Execute the query
        if req.is_composite_primary_key:
            # For composite keys, pass the ids dictionary directly
            result_proxy = self.session.execute(query, req.ids)
        else:
            # For single keys, pass a dictionary with the id
            result_proxy = self.session.execute(query, {'id': req.id})

        # Get the column names from the ResultProxy
        column_names = result_proxy.keys()

        # get the first row
        row = result_proxy.fetchone()

        # Convert each tuple to a dictionary using the column names as keys
        return dict(zip(column_names, row))

    async def get_table_data_by_ids(self, req: GetTableDataRequest):
        try:
            if '.' in req.table_name:
                schema_name, table_name = req.table_name.split('.')
            else:
                schema_name = req.schema_name
                table_name = req.table_name

            # return error if ids is not provided
            if not req.ids:
                return {'error': 'IDs value is required'}

            # get primary key first
            req_attr = GetTableAttributesRequest(
                schema_name=schema_name,
                table_name=table_name
            )
            primary_key = await self.get_primary_key(req_attr)

            # get primary key name
            primary_key_name = primary_key[0]['attname']
            if req.custom_column:
                primary_key_name = req.custom_column

            query = text(f'''SELECT * 
                            FROM {schema_name}.{table_name} 
                            WHERE {primary_key_name} IN {tuple(req.ids)}''')
            result_proxy = self.session.execute(query)

            # Get the column names from the ResultProxy
            column_names = result_proxy.keys()

            # Fetch all rows as a list of tuples
            rows = result_proxy.fetchall()

            # Convert each tuple to a dictionary using the column names as keys
            return [dict(zip(column_names, row)) for row in rows]

        except SQLAlchemyError as e:
            # Return the error message as a response
            return {'error': str(e)}

    async def create_table_record(self, req: CreateTableRecordRequest):
        try:
            req_attr = GetTableColumnsRequest(
                schema_name=req.schema_name,
                table_name=req.table_name
            )
            columns = await self.get_table_columns(req_attr)

            req.data = self.QueryParser.audition_insert_column(columns, req.data)

            # Prepare the SQL INSERT statement
            columns = ', '.join(req.data.keys())

            placeholders = ', '.join(':' + key for key in req.data.keys())
            query = text(f'''INSERT INTO {req.schema_name}.{req.table_name} ({columns})
                                 VALUES ({placeholders}) RETURNING *''')

            # Execute the query
            result_proxy = self.session.execute(query, req.data)

            # commit session
            self.session.commit()

            # Get the column names from the ResultProxy
            column_names = result_proxy.keys()

            # Fetch the inserted row
            row = result_proxy.fetchone()

            # Convert the row to a dictionary using the column names as keys
            inserted_record = dict(zip(column_names, row))

            # If return_id is True, return only the id of the inserted record
            if req.return_id:
                return inserted_record[req.primary_key_column]

            # Otherwise, return the entire inserted record
            return inserted_record

        except SQLAlchemyError as e:
            # Return the error message as a response
            return {'error': str(e)}

