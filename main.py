import uvicorn
from fastapi import FastAPI
from config.config import GUNICORN_CONFIG
from pkg.conn.database import database
from core.usecase.catalog_usecase import CatalogUseCase

from core.entity.catalog import GetTableColumnsRequest, GetTableAttributesRequest, GetTableDataRequest, CreateTableRecordRequest, SCHEMA_PUBLIC
from pkg.helper.table_name import normalize_schema_and_table_name


def init_app() -> FastAPI:
    _app = FastAPI()
    return _app


def init_db(_app: FastAPI):
    @_app.on_event("startup")
    async def startup():
        await database.connect()

    @_app.on_event("shutdown")
    async def shutdown():
        await database.disconnect()


app = init_app()
init_db(app)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/primary-key/{table_name}")
async def get_primary_key(table_name: str):
    schema_name, table_name = normalize_schema_and_table_name(None, table_name)

    req = GetTableAttributesRequest(
        schema_name=schema_name,
        table_name=table_name
    )
    primary_key = await CatalogUseCase().get_primary_key(req)

    return primary_key


@app.get("/columns/{table_name}")
async def get_table_columns(table_name: str, complete_attribute: bool = False):
    schema_name, table_name = normalize_schema_and_table_name(None, table_name)

    req = GetTableColumnsRequest(
        schema_name=schema_name,
        table_name=table_name,
        complete_attribute=complete_attribute
    )
    columns = await CatalogUseCase().get_table_columns(req)

    return columns


@app.get("/attributes/{table_name}")
async def get_table_attributes(table_name: str, complete_attribute: bool = False, with_data: bool = False, page: int = 1, page_size: int = 10, keyword: str = ""):
    schema_name, table_name = normalize_schema_and_table_name(None, table_name)

    req = GetTableAttributesRequest(
        schema_name=schema_name,
        table_name=table_name,
        complete_attribute=complete_attribute,
        with_data=with_data,
        page=page,
        page_size=page_size,
        keyword=keyword
    )
    attributes = await CatalogUseCase().get_table_attributes(req)

    return attributes


@app.post("/data/table")
async def get_table_data(req: GetTableDataRequest):
    schema_name, table_name = normalize_schema_and_table_name(req.schema_name, req.table_name)
    req.table_name = table_name
    req.schema_name = schema_name

    data = await CatalogUseCase().get_table_data(req)
    return data


@app.post("/data/table/id")
async def get_table_data_by_id(req: GetTableDataRequest):
    schema_name, table_name = normalize_schema_and_table_name(req.schema_name, req.table_name)
    req.table_name = table_name
    req.schema_name = schema_name

    data = await CatalogUseCase().get_table_data_by_id(req)
    return data


@app.put("/data/table")
async def create_table_record(req: CreateTableRecordRequest):
    schema_name, table_name = normalize_schema_and_table_name(req.schema_name, req.table_name)
    req.table_name = table_name
    req.schema_name = schema_name

    data = await CatalogUseCase().create_table_record(req)
    return data


@app.patch("/data/table")
async def update_table_record(req: CreateTableRecordRequest):
    schema_name, table_name = normalize_schema_and_table_name(req.schema_name, req.table_name)
    req.table_name = table_name
    req.schema_name = schema_name

    data = await CatalogUseCase().update_table_record(req)
    return data

if __name__ == '__main__':
    uvicorn.run("main:core", host=GUNICORN_CONFIG.HOST, port=GUNICORN_CONFIG.PORT, reload=True)
