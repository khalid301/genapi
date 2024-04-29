#!/bin/bash

export GENAPI_DB_HOST=103.41.206.233
export GENAPI_DB_USERNAME=postgres
export GENAPI_DB_PASSWORD=r00t_db
export GENAPI_DB_NAME=crm

uvicorn main:app --reload
