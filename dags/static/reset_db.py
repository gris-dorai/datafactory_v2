from airflow.models import DagRun, Variable

from models.datafactory import FileProcessStatus, Providers, MarketFacts, Definition, \
    ProviderVariables, StandardVariables, UnitOfMeasurement, Language, Statistics, Dataset, Validations

from airflow.models.base import Base
from sqlalchemy import Table, delete
from airflow import settings
from services.db_service import Session, engine

session = settings.Session()
df_session = Session()

def recreate_db():
    """
    delete all data from airflow dagrun table and recreate datafactory tables
    """
    get_table = Table('dag_run', Base.metadata, autoload=True, autoload_with=settings.engine,schema='public')
    delete_data = delete(get_table)
    session.execute(delete_data)
    session.commit()

    #datafactory tables to delete and recreate
    to_delete_and_recreate = [
        FileProcessStatus.__table__,
        Providers.__table__,
        MarketFacts.__table__,
        Definition.__table__,
        ProviderVariables.__table__,
        StandardVariables.__table__,
        UnitOfMeasurement.__table__,
        Language.__table__,
        Statistics.__table__,
        Dataset.__table__,
        Validations.__table__,
    ]
    #drop list of datafactory tables provided
    Base.metadata.drop_all(bind=engine, tables=to_delete_and_recreate)
    #create list of datafactory tables provided
    Base.metadata.create_all(bind=engine, tables=to_delete_and_recreate)

if __name__ == "__main__":
    recreate_db()