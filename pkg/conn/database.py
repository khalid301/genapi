from databases import Database
from sqlalchemy import create_engine, MetaData
from config.config import GUNICORN_CONFIG

DATABASE_URL = f"{GUNICORN_CONFIG.DB_DIALECT}://{GUNICORN_CONFIG.DB_USERNAME}:{GUNICORN_CONFIG.DB_PASSWORD}@{GUNICORN_CONFIG.DB_HOST}:{GUNICORN_CONFIG.DB_PORT}/{GUNICORN_CONFIG.DB_NAME}"

engine = create_engine(DATABASE_URL, echo=True)
metadata = MetaData()
database = Database(DATABASE_URL)
