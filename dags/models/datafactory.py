import uuid
#from airflow.models.base import Base
from sqlalchemy import exc, func, schema
from airflow.utils.log.logging_mixin import LoggingMixin
from geoalchemy2 import Geometry
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy import Column, DateTime, String, Text, Boolean, Integer, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert
from airflow import settings
from airflow.models import DagRun

#create schema if not exist
from services.db_service import engine, Session

if not engine.dialect.has_schema(engine, "datafactory"):
    engine.execute(schema.CreateSchema("datafactory"))

conn = engine.connect()
conn.execute("CREATE EXTENSION postgis")    
    
Base = declarative_base()

df_session = Session()

class Dataset(Base):
    """ Describes the dataset table """

    __tablename__ = "dataset"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    dataset_id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(String(75), nullable=False)
    region = Column(String(50), nullable=False)
    crop = Column(String(50), nullable=False)
    year = Column(Integer, nullable=False)
    data_product_unique_id = Column(String(75), nullable=True)
    feature_geom = Column(Geometry('MULTIPOLYGON'), nullable=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())

    def insert(self):
        """ Creates a new dataset """
        df_session.add(self)
        try:
            df_session.commit()
        except exc.IntegrityError as e:
            # After error MUST rollback session avoid session flush errors
            df_session.rollback()
            raise RuntimeError(
                "Could not insert new dataset. reason: {}".format(e))

    def get_any_dataset_id(self):
        """ Returns a any dataset for Testing"""
        get_dataset = df_session.query((Dataset)).first()
        if get_dataset:
            return get_dataset.dataset_id
        return None


def definition_code(context):
    uuid_gen = uuid.uuid5(uuid.NAMESPACE_X500, context.get_current_parameters()['code'])
    return uuid_gen


class Definition(Base):
    """ Describes the definition table """

    __tablename__ = "definition"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    definition_id = Column(
        UUID(as_uuid=True), primary_key=True, default=definition_code)
    name = Column(String)
    code = Column(String, unique=True)
    uom = Column(String)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())



def market_facts_code(context):
    crop = context.get_current_parameters().get('crop','')
    country_code = context.get_current_parameters().get('adminlevel1_code','')
    definition = context.get_current_parameters().get('definition_id','')
    grid_id = context.get_current_parameters().get('grid_id','')
    year = context.get_current_parameters().get('year', '')
    product_dimension = context.get_current_parameters().get('product_dimension', '')
    uuid_gen = uuid.uuid5(uuid.NAMESPACE_X500,
                          str(f"{crop}_{country_code}_{definition}_{grid_id}_{year}_{product_dimension}"))
    return uuid_gen

class MarketFacts(Base):
    """ Describes the MarketFacts table """

    __tablename__ = "market_facts"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    market_facts_id = Column(
        UUID(as_uuid=True), primary_key=True, default=market_facts_code)
    grid_id = Column(String(75), nullable=True)
    country = Column(String(75))
    adminlevel1_code = Column(String(75))
    adminlevel2_code = Column(String(75))
    crop = Column(String(75))
    value = Column(JSONB, default="'{}'::JSON")
    definition_id = Column(String(75))
    product_dimension = Column(JSONB, default="'{}'::JSON", nullable=True)
    file_process_job_id = Column(String(75))
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())

    def bulk_insert(self, records_to_insert):
        """ bulk insert market_facts data """

        df_session.bulk_insert_mappings(MarketFacts, records_to_insert)
        try:
            # Execute the query
            df_session.commit()
        except exc.IntegrityError as e:
            # After error MUST rollback session avoid session flush errors
            df_session.rollback()
            raise RuntimeError(
                "Could not bulk insert MarketFacts. reason: {}".format(e))

    def get_by_country(self, country_code, crop):
        """ exclude country by crop and adminlevel1_code"""

        result = df_session.query(MarketFacts).filter(
            MarketFacts.adminlevel1_code == str(country_code)). \
            filter(MarketFacts.crop == str(crop)).order_by(MarketFacts.created_at.desc()).first()
        return result


class Statistics(Base):
    """ Describes the statistics table """

    __tablename__ = "statistics"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    statistics_id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(String(75), nullable=False)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey('datafactory.dataset.dataset_id'), nullable=True)
    grid_id = Column(String(75), nullable=False)
    summary = Column(JSONB, default="'{}'::JSON", nullable=True)
    temporal_dimension = Column(JSONB, default="'{}'::JSON", nullable=True)
    spatial_dimension = Column(JSONB, default="'{}'::JSON", nullable=True)
    product_dimension = Column(JSONB, default="'{}'::JSON", nullable=True)
    grid_process_id = Column(String(75), nullable=False)
    status = Column(String(75), nullable=False)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())

    def insert(self):
        """ Creates a new statistics """

        df_session.add(self)
        try:
            df_session.commit()
        except exc.IntegrityError as e:
            # After error MUST rollback session avoid session flush errors
            df_session.rollback()
            raise RuntimeError(
                "Could not insert new statistics. reason: {}".format(e))


def provider_hash_func(context):
    name = context.get_current_parameters()['name']
    uuid_gen = uuid.uuid5(uuid.NAMESPACE_X500, str(f"{name}"))
    return uuid_gen


class Providers(Base):
    """ Describes the provider table """

    __tablename__ = "data_provider"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    provider_id = Column(
        UUID(as_uuid=True), primary_key=True, default=provider_hash_func)
    name = Column(String, unique=True)
    url_prifix = Column(String)
    concurrency = Column(Integer)
    limit = Column(Integer)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())

    def bulk_insert(self, records_to_insert):
        """ bulk insert providers data """

        df_session.bulk_insert_mappings(Providers, records_to_insert)
        try:
            # Execute the query
            df_session.commit()
        except exc.IntegrityError as e:
            # After error MUST rollback session avoid session flush errors
            df_session.rollback()
            raise RuntimeError(
                "Could not bulk insert providers. reason: {}".format(e))

    def get_all_providers(self):
        """ Returns all providers """

        result = df_session.query(Providers).all()
        return result

    def get_provider(self, name):
        """ Returns provider """

        result = df_session.query(Providers).filter_by(
                name=name
            ).one_or_none()
        return result.url_prifix


class Validations(Base):
    """ Describes the validation table """

    __tablename__ = "data_validation"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    validation_id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    grid_id = Column(String(75), nullable=False)
    job_id = Column(String(75), nullable=False)
    grid_process_id = Column(String(75), nullable=True)
    file_loc = Column(String(250), nullable=True)
    file_loc_efs = Column(String(250), nullable=True)
    statistics_id = Column(UUID(as_uuid=True), ForeignKey('datafactory.statistics.statistics_id'))
    fields = Column(JSONB, default="'{}'::JSON")
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())

    def insert(self):
        """ Creates a new validation """

        df_session.add(self)
        try:
            df_session.commit()
        except exc.IntegrityError as e:
            # After error MUST rollback session avoid session flush errors
            df_session.rollback()
            raise RuntimeError(
                "Could not insert new validation. reason: {}".format(e))

    def get_all_validation(self):
        """ Returns all validation """

        result = df_session.query(Validations).all()
        return result


def providervariable_hash_func(context):
    name = context.get_current_parameters()['name']
    code = context.get_current_parameters()['code']
    provider_id = context.get_current_parameters()['provider_id']
    uuid_gen = uuid.uuid5(uuid.NAMESPACE_X500, str(f"{name}_{code}_{provider_id}"))
    return uuid_gen


class ProviderVariables(Base):
    """ Describes the provider variable table """

    __tablename__ = "provider_variables"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    provider_variable_id = Column(
        UUID(as_uuid=True), primary_key=True, default=providervariable_hash_func)
    name = Column(String(75))
    code = Column(String(75))
    stat_key = Column(String(75))
    json_path = Column(String(75))
    column_name = Column(String(75))
    data_type = Column(String(75))
    description = Column(Text)
    uom = Column(String(75))
    uom_language_code = Column(String(75))
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())
    post_coefficient_definition_id = Column(UUID(as_uuid=True), ForeignKey('datafactory.definition.definition_id'))
    provider_id = Column(UUID(as_uuid=True), ForeignKey('datafactory.data_provider.provider_id'))
    standard_variable_id = Column(UUID(as_uuid=True), ForeignKey('datafactory.standard_variables.standard_variable_id'))


def standard_variable_hash_func(context):
    name = context.get_current_parameters()['name']
    code = context.get_current_parameters()['code']
    uuid_gen = uuid.uuid5(uuid.NAMESPACE_X500, str(f"{name}_{code}"))
    return uuid_gen


class StandardVariables(Base):
    """ Describes the standard variable table """

    __tablename__ = "standard_variables"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    standard_variable_id = Column(
        UUID(as_uuid=True), primary_key=True, default=standard_variable_hash_func)
    name = Column(String(75))
    code = Column(String(75), unique=True)
    description = Column(Text)
    uom = Column(String(75))
    uom_language_code = Column(String(75))
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())


class UnitOfMeasurement(Base):
    """ Describes the unit of measurement table """

    __tablename__ = "unit_of_measurement"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    unit_of_measurement_id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(75))
    code = Column(String(75))
    scale = Column(String(75))
    offset = Column(String(75))
    dimension = Column(String(75))
    uom_is_standard = Column(Boolean)
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())


class Language(Base):
    """ Describes the language table """

    __tablename__ = "language"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    language_id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(75))
    language_code = Column(String(2))
    label = Column(String(75))
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())
    unit_of_measurement_id = Column(UUID(as_uuid=True), ForeignKey('datafactory.unit_of_measurement.unit_of_measurement_id'))


class FileProcessStatus(Base):
    """ Describes the file process status table """

    __tablename__ = "file_process_status"
    __table_args__ = {'schema': 'datafactory','extend_existing': True}

    file_process_status_id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    file_name = Column(String(150))
    target_table = Column(String(75))
    status = Column(String(75))
    job_id = Column(String(75))
    file_created = Column(String(75))
    note = Column(Text,nullable=True)
    processed_on = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now())



Dataset.__table__.create(engine, checkfirst=True)
Definition.__table__.create(engine, checkfirst=True)
MarketFacts.__table__.create(engine, checkfirst=True)
Statistics.__table__.create(engine, checkfirst=True)
Providers.__table__.create(engine, checkfirst=True)
Validations.__table__.create(engine, checkfirst=True)
UnitOfMeasurement.__table__.create(engine, checkfirst=True)
Language.__table__.create(engine, checkfirst=True)
StandardVariables.__table__.create(engine, checkfirst=True)
ProviderVariables.__table__.create(engine, checkfirst=True)
FileProcessStatus.__table__.create(engine, checkfirst=True)

# create index if not indexed
index = settings.engine.dialect.get_indexes(table_name='dag_run', schema='public', connection=settings.engine.connect())
indexes_list = list(map(lambda d: d['name'], index))
if not 'idx_dag_run_run_id' in indexes_list:
    dag_run_id_index = Index('idx_dag_run_run_id', DagRun.run_id)
    dag_run_id_index.create(bind=settings.engine)

market_facts_index = engine.dialect.get_indexes(table_name='market_facts', schema='datafactory',
                                                         connection=engine.connect())
market_facts_indexes_list = list(map(lambda m: m['name'], market_facts_index))
if not 'idx_crop_and_country_code' in market_facts_indexes_list:
    crop_and_country_index = Index('idx_crop_and_country_code', MarketFacts.definition_id, MarketFacts.crop,
                               MarketFacts.adminlevel1_code)
    crop_and_country_index.create(bind=engine)
if not 'idx_crop_and_grid_id' in market_facts_indexes_list:
    crop_and_grid_index = Index('idx_crop_and_grid_id', MarketFacts.definition_id, MarketFacts.crop,
                               MarketFacts.grid_id)
    crop_and_grid_index.create(bind=engine)
