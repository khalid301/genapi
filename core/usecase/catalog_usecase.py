from core.repository.catalog_repository.implement import CatalogRepository
from core.entity.catalog import GetTableColumnsRequest, GetTableAttributesRequest, GetTableDataRequest, CreateTableRecordRequest, COLUMN_NAME, FOREIGN_TABLE_SCHEMA, FOREIGN_TABLE_NAME, FOREIGN_COLUMN_NAME


class CatalogUseCase:
    def __init__(self) -> None:
        super().__init__()
        self.CatalogRepo = CatalogRepository()

    async def get_primary_key(self, req: GetTableAttributesRequest) -> list[str]:
        return await self.CatalogRepo.get_primary_key(req)

    async def get_table_columns(self, req: GetTableColumnsRequest) -> list[str]:
        return await self.CatalogRepo.get_table_columns(req)

    async def get_table_data_by_ids(self, req: GetTableDataRequest):
        records = await self.CatalogRepo.get_table_data(req)

        req_foreign = GetTableColumnsRequest(
            schema_name=req.schema_name,
            table_name=req.table_name
        )
        foreign_keys = await self.CatalogRepo.get_table_foreign_keys(req_foreign)

        for foreign_key in foreign_keys:
            foreign_ids = []
            for record in records:
                # check if the value is text or others
                if isinstance(record[foreign_key[COLUMN_NAME]], str):
                    k = record[foreign_key[COLUMN_NAME]].strip()
                else:
                    k = record[foreign_key[COLUMN_NAME]]

                foreign_ids.append(k)

            # combine schema name and table name
            table_name = f"{foreign_key[FOREIGN_TABLE_SCHEMA]}.{foreign_key[FOREIGN_TABLE_NAME]}"
            req_foreign = GetTableDataRequest(
                table_name=table_name,
                ids=foreign_ids,
                custom_column=foreign_key[FOREIGN_COLUMN_NAME]
            )
            # call get_table_data_by_ids recursively, but prevent infinite loop, but allow the same table_name
            foreign_records = await self.CatalogRepo.get_table_data_by_ids(req_foreign)

            # create dict foreign_records with id as key
            foreign_records_dict = {}
            for foreign_record in foreign_records:
                foreign_records_dict[foreign_record[foreign_key[FOREIGN_COLUMN_NAME]]] = foreign_record

            # assign foreign_records to records
            for record in records:
                record[foreign_key[FOREIGN_TABLE_NAME]] = foreign_records_dict.get(record[foreign_key[COLUMN_NAME]], {})

        return records

    async def get_table_data(self, req: GetTableDataRequest):
        records = await self.CatalogRepo.get_table_data(req)

        # check if records return error
        if 'error' in records:
            return records

        req_foreign = GetTableColumnsRequest(
            schema_name=req.schema_name,
            table_name=req.table_name
        )
        foreign_keys = await self.CatalogRepo.get_table_foreign_keys(req_foreign)

        for foreign_key in foreign_keys:
            foreign_ids = []
            for record in records:
                # check if the value is text or others
                if isinstance(record[foreign_key[COLUMN_NAME]], str):
                    k = record[foreign_key[COLUMN_NAME]].strip()
                else:
                    k = record[foreign_key[COLUMN_NAME]]

                # detect if k exist and not None
                if k:
                    foreign_ids.append(k)

            # combine schema name and table name
            print("foreign_ids", foreign_ids)
            table_name = f"{foreign_key[FOREIGN_TABLE_SCHEMA]}.{foreign_key[FOREIGN_TABLE_NAME]}"
            req_foreign = GetTableDataRequest(
                table_name=table_name,
                ids=foreign_ids,
                custom_column=foreign_key[FOREIGN_COLUMN_NAME]
            )
            foreign_records = await self.CatalogRepo.get_table_data_by_ids(req_foreign)

            # create dict foreign_records with id as key
            foreign_records_dict = {}
            for foreign_record in foreign_records:
                if isinstance(foreign_record, dict):
                    key = foreign_record[foreign_key[FOREIGN_COLUMN_NAME]]
                    foreign_records_dict[key] = foreign_record
                    foreign_record_attr = foreign_key[FOREIGN_COLUMN_NAME]
                    key = foreign_record[foreign_record_attr]
                    foreign_records_dict[key] = foreign_record

            # assign foreign_records to records
            for record in records:
                record[foreign_key[FOREIGN_TABLE_NAME]] = foreign_records_dict.get(record[foreign_key[COLUMN_NAME]], {})

        return records

    async def get_table_data_by_id(self, req: GetTableDataRequest):
        if '.' in req.table_name and req.schema_name is None:
            schema_name, table_name = req.table_name.split('.')
        else:
            schema_name = req.schema_name
            table_name = req.table_name

        # get primary key first
        req_attr = GetTableAttributesRequest(
            schema_name=schema_name,
            table_name=table_name
        )
        primary_key = await self.get_primary_key(req_attr)

        # we need to add handler if primary key is composite
        if len(primary_key) > 1:
            req.is_composite_primary_key = True

        primary_key_list = []
        for key in primary_key:
            primary_key_list.append(key['attname'])

        req.primary_key_name = primary_key_list

        record = await self.CatalogRepo.get_table_data_by_id(req)

        return record

    async def get_table_attributes(self, req: GetTableAttributesRequest):

        req_columns = GetTableColumnsRequest(
            table_name=req.table_name,
            complete_attribute=req.complete_attribute
        )

        # return as dictionary
        table_attr = {
            'table_name': req.table_name,
            'columns': await self.CatalogRepo.get_table_columns(req_columns),
            'primary_key': await self.CatalogRepo.get_primary_key(req),
            'foreign_keys': await self.CatalogRepo.get_table_foreign_keys(req_columns)
        }

        if req.with_data:
            req_data = GetTableDataRequest(
                table_name=req.table_name,
                page=req.page,
                page_size=req.page_size,
                keyword=req.keyword
            )
            table_attr['data'] = await self.CatalogRepo.get_table_data(req_data)

        return table_attr

    async def create_table_record(self, req: CreateTableRecordRequest):
        # first, get the primary key
        req_attr = GetTableAttributesRequest(
            schema_name=req.schema_name,
            table_name=req.table_name
        )
        primary_key = await self.get_primary_key(req_attr)

        primary_key_list = []
        for key in primary_key:
            primary_key_list.append(key['attname'])

        if len(primary_key) == 0:
            return {'error': 'Primary key is required'}

        # check mandatory column
        req_column = GetTableColumnsRequest(
            schema_name=req.schema_name,
            table_name=req.table_name
        )
        columns = await self.get_table_columns(req_column)

        mandatory_columns = [column[COLUMN_NAME] for column in columns if column['is_nullable'] == 'NO']

        # check if mandatory column exists in the data
        for mandatory_column in mandatory_columns:
            if mandatory_column not in req.data and mandatory_column not in primary_key_list:
                return {'error': f'{mandatory_column} is mandatory'}

        result = await self.CatalogRepo.create_table_record(req)

        return result
