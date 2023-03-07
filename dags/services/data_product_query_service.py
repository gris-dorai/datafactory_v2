from services.json_unpack_service import UnPackObj


def create_query(dimension_dict, dimension_name):

    dimension_query = ""
    column_data_types = {'year': 'integer', 'season': 'double precision','growth_stage':'character varying',
                         'code':'numeric','region':'character varying','irrigation':'character varying','watermgmt':'character varying',
                         'from_date':'character varying','to_date':'character varying','start_date':'character varying',
                         'end_date':'character varying','country':'character varying','crop':'character varying'}
    for index,(k, v) in enumerate(UnPackObj().extract_obj(dimension_dict).items()):
        data_type = column_data_types.get(k.lower(), 'text')
        query = f"""
                (s.{dimension_name} -> {index} ->> 'value')::{data_type} as {k},"""
        dimension_query = dimension_query + query
    return dimension_query


class DataProductSchema:
    def __init__(self, provider_variables,temporal_dimension,spatial_dimension,product_dimension, data_product_unique_id,
                 job_id, dataset_id):
        self.provider_variables = provider_variables
        self.temporal_dimension = temporal_dimension
        self.spatial_dimension = spatial_dimension
        self.product_dimension = product_dimension
        self.data_product_unique_id = data_product_unique_id
        self.job_id = job_id
        self.dataset_id = dataset_id

    def create_variables_query(self):
        variables_query = ""
        index = 0
        for provider_variable in self.provider_variables:
            var_query = f"""
                (s.summary -> {index} ->> 'value')::{provider_variable.data_type} as {provider_variable.column_name},"""
            variables_query = variables_query + var_query
            index += 1
        return variables_query

    def temporal_dimension_query(self):
        create_temporal_dimension = create_query(self.temporal_dimension, 'temporal_dimension')
        return create_temporal_dimension

    def create_query_template(self):

        spatial_dimension_query = create_query(self.spatial_dimension,'spatial_dimension')
        product_dimension_query = create_query(self.product_dimension,'product_dimension')

        data_product_unique_id_query = f"and d.data_product_unique_id = '{self.data_product_unique_id}'"
        query_template = f"""
                SELECT s.statistics_id as row_id,
                s.grid_id,
                {self.create_variables_query()}
                {self.temporal_dimension_query()}
                {spatial_dimension_query}
                {product_dimension_query}
                g.geometry as geom, d.feature_geom as dataset_geom
                FROM datafactory.statistics s
                JOIN datafactory.grid_geom g ON s.grid_id = g.grid_id::text
                JOIN datafactory.dataset d ON s.dataset_id = d.dataset_id::uuid
                WHERE s.job_id = '{self.job_id}' and d.dataset_id = '{self.dataset_id}'
                {data_product_unique_id_query if self.data_product_unique_id else ""}
                ORDER BY s.created_at DESC
                """
        return query_template


class CropFactPhysiologySchema(DataProductSchema):

    def create_variables_query(self):
        variables_query = ""
        for provider_variable in self.provider_variables:
            var_query = f"""
                (s.summary -> {self.provider_variables.index(provider_variable)} ->> 'value')::{provider_variable.data_type} as {provider_variable.column_name},"""
            variables_query = variables_query + var_query
        return variables_query

    def temporal_dimension_query(self):
        self.temporal_dimension.extend([{"type":"START_DATE","value":""},{"type":"END_DATE","value":""},
                                        {"type":"END_YEAR","value":""}])
        create_temporal_dimension = create_query(self.temporal_dimension,'temporal_dimension')
        return create_temporal_dimension

    def end_year_index(self):
        end_year_i = next((i for i, item in enumerate(self.temporal_dimension) if item["type"] == "END_YEAR"), None)
        return end_year_i

    def create_query_template(self):

        spatial_dimension_query = create_query(self.spatial_dimension,'spatial_dimension')
        product_dimension_query = create_query(self.product_dimension,'product_dimension')

        data_product_unique_id_query = f"and d.data_product_unique_id = '{self.data_product_unique_id}'"
        query_template = f"""
                SELECT s.statistics_id as row_id,
                s.grid_id,
                {self.create_variables_query()}
                {self.temporal_dimension_query()}
                {spatial_dimension_query}
                {product_dimension_query}
                g.geometry as geom, d.feature_geom as dataset_geom,
                
                (
				SELECT NULLIF(temporal_dimension -> {self.end_year_index()} ->> 'value', '')::integer AS end_year
				FROM datafactory.statistics
				WHERE job_id = '{self.job_id}'
				  AND dataset_id = '{self.dataset_id}'
				  AND NULLIF(temporal_dimension -> {self.end_year_index()} ->> 'value', '')::integer = (
					SELECT MAX(NULLIF(temporal_dimension -> {self.end_year_index()} ->> 'value', '')::integer)
					FROM datafactory.statistics
					WHERE job_id = '{self.job_id}'
					  AND dataset_id = '{self.dataset_id}'
					  AND NULLIF(temporal_dimension -> {self.end_year_index()} ->> 'value', '') != ''
				  )
				LIMIT 1
				)
                
                FROM datafactory.statistics s
                JOIN datafactory.grid_geom g ON s.grid_id = g.grid_id::text
                JOIN datafactory.dataset d ON s.dataset_id = d.dataset_id::uuid
                WHERE s.job_id = '{self.job_id}' and d.dataset_id = '{self.dataset_id}'
                {data_product_unique_id_query if self.data_product_unique_id else ""}
                ORDER BY s.created_at DESC
                """
        return query_template


PROVIDERSCHEMALOOKUP = {
    'fbm': DataProductSchema,
    'cropfact-environmentalclassification':DataProductSchema,
    'cropfact-cropphysiology': CropFactPhysiologySchema,
    'gris-wcs': DataProductSchema,
    'satsure-sparta': DataProductSchema,
    'test-provider': DataProductSchema
}
