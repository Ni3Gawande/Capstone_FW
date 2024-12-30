import pytest
from sqlalchemy import create_engine
from Configuration.config import *

@pytest.fixture()
def connect_sqlserverdb_engine():
    engine = create_engine(
        f'mssql+pyodbc://{SQLSERVER_HOST}/{SQLSERVER_DATABASE}?driver={SQLSERVER_DRIVER}&trusted_connection={SQLSERVER_TRUSTED_CONNECTION}')

    sqlserver_engine = engine.connect()
    yield sqlserver_engine
    sqlserver_engine.close()

@pytest.fixture()
def connect_mysqldb_engine():
    engine = create_engine(
        f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

    mysql_engine=engine.connect()
    yield mysql_engine
    mysql_engine.close()
