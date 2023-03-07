# -*- coding: utf-8 -*-
"""
GridGeom DB service to manage grid_geom operations on DB
"""

from services.db_service import Session
import pandas as pd
from airflow import settings

df_session = Session()

class GridGeomDbServices:
    """
    Performs grid_geom operations on db
    """

    def get_global_refrence_grids(self, feature, year) -> str:
        """
        Get Global refrence grids from passed feature with the column geometry
        """

        # get the SQL to run
        sql_query = self._get_sql_query(feature)
        # Perform the operation on the database
        try:
            # Execute the query
            total_number_of_grids = df_session.execute(sql_query).fetchall()
            sql_response = df_session.execute(sql_query)
            # If the query returned any rows, process these
            if sql_response.returns_rows:
                return sql_response.mappings().all(),len(total_number_of_grids)
            return None, None
        except Exception as ex:
            raise RuntimeError(f"Error running SQL query: {ex}")

    def _get_sql_query(self, feature):
        """ Generate the SQL and return """
        return f"""
            SELECT t.grid_id, t.hactares, t.longitude, t.latitude,t.country, t.country_code, t.region,
                ST_ASGEOJSON(t.geometry) as geom
            FROM datafactory.grid_geom t
            WHERE ST_GeomFromEWKT(
                    'SRID=4326;{feature}')
                    && t.geometry and _ST_Intersects(
                        ST_GeomFromEWKT(
                            'SRID=4326;{feature}'),
                            ST_Centroid(t.geometry))
        """

