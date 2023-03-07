
import os
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from airflow.utils.orm_event_handlers import setup_event_handlers
from airflow.utils.log.secrets_masker import mask_secret
from sqlalchemy_utils import database_exists, create_database

load_dotenv(find_dotenv())


SQL_ALCHEMY_CONN = str(os.getenv("DATAFACTORY_SQL_ALCHEMY_CONN"))

pool_size = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_POOL_SIZE", 5)
max_overflow = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_MAX_OVERFLOW",10)
pool_recycle = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_POOL_RECYCLE", 1800)
pool_pre_ping = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_POOL_PRE_PING", True)
encoding = os.getenv("AIRFLOW__CORE__SQL_ENGINE_ENCODING", "utf-8")

engine = create_engine(SQL_ALCHEMY_CONN, pool_size=int(pool_size), max_overflow=int(max_overflow),
                       pool_recycle=int(pool_recycle), pool_pre_ping=pool_pre_ping, encoding=encoding)

if not database_exists(engine.url):
    create_database(engine.url)
    conn = engine.connect()
    conn.execute("CREATE EXTENSION postgis")
    conn.close()

mask_secret(engine.url.password)
setup_event_handlers(engine)

Session = scoped_session(
    sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
        expire_on_commit=False,
    )
)
