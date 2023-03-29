import json
import os
import tempfile

import requests
from airflow import AirflowException
from airflow.exceptions import AirflowFailException
from geojson import Feature, FeatureCollection
from models.datafactory import Providers
from dotenv import load_dotenv, find_dotenv
import pandas as pd
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import shutil
from shapely.geometry import shape
import urllib3
from services.json_unpack_service import ObjectExtract, ExtractValue
from pydantic import BaseModel, ValidationError, validator, root_validator
from typing import Any, Optional
from datetime import datetime, timedelta
from services.db_service import engine

load_dotenv(find_dotenv())

AUTHKEY = str(os.getenv("AUTHKEY", None))
CROPFACT_AUTHKEY = str(os.getenv("CROPFACT_AUTHKEY",None))
DEFINITION_ID = str(os.getenv("EXCLUDE_DEFINITION_ID", ""))

class UnPackObj:
    """
    Unpack Json Object
    """
    def extract_obj(self, list_obj):
        obj_unpacked = {}
        if list_obj:
            for obj in list_obj:
                key = obj['type']
                obj_unpacked[key] = obj['value']
            return obj_unpacked
        return obj_unpacked


class ProviderParams(BaseModel):
    providerName: str = None
    wait_time: int = 0


class ValidateTemplate(BaseModel):
    name: str
    provider: str
    data_product_unique_id: str = None
    provider_params: ProviderParams
    trigger_fme : bool = False
    fme_params: dict = {}
    context_code : str = None
    temporal_dimension : list
    measure_dimension : list
    product_dimension : list
    spatial_dimension : list

    @validator('temporal_dimension')
    def temporal_dimension_validation(cls, v):
        year = UnPackObj().extract_obj(v).get('YEAR',None)
        if not year:
            raise ValueError('must contain a year')
        return v

    @validator('product_dimension')
    def product_dimension_validation(cls, v, values):
        crop = UnPackObj().extract_obj(v).get('CROP', None)
        irrigation_type = UnPackObj().extract_obj(v).get('WATERMGMT', None)
        if not crop or not irrigation_type:
            raise ValueError('must contain a crop and irrigation_type')
        return v

    @validator('spatial_dimension')
    def spatial_dimension_validation(cls, v):
        region = UnPackObj().extract_obj(v).get('REGION', None)
        if not region:
            raise ValueError('must contain a region')
        return v

    @root_validator
    def trigger_fme_validation(cls, values):
        if values.get('trigger_fme', None) and not values.get('fme_params',None):
            raise ValueError("FME input parameters Required")
        v = values.get('fme_params',None)
        if values.get('trigger_fme',None) and not v.get('api_url', None) or not v.get('destination_db', None) or not \
                v.get('destination_table', None) or not v.get('publishing_api_params', None):
            raise ValueError("FME input parameters missing")
        return values



def json_extract(obj):
    """Recursively fetch values from nested JSON."""
    data_list = {}
    keydict = {"keyname":""}

    def extract(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    keydict['keyname'] = k
                    extract(v)
                else:
                    data_list[f"{keydict['keyname']}.{k}"] = v

        elif isinstance(obj, list):
            for item in obj:
                extract(item)
        return data_list

    values = extract(obj)
    return values


class FBMProvider:
    """
    Method for FBM Provider
    Parameters:
    provider: provider name
    """
    def __init__(self, provider):

        # Fetch provider url
        AUTHKEY = os.getenv("AUTHKEY", None)
        URL_PREFIX = Providers().get_provider(provider.lower())
        self.PROVIDER_API = f"{URL_PREFIX}?authkey={AUTHKEY}"

    def validate_template(self, template):
        ValidateTemplate(**template)

    def send_api_request(self, payload):
        """ Function to get the API response
        Parameters:
        payload: provider API body to send request
        Returns:
        dict: returns API response
        """
        headers = {'Content-Type': 'application/json'}

        try:
            response = requests.post(
                self.PROVIDER_API,
                headers=headers,
                data=payload,
            )
        except Exception as e:
            # raise exception with error message
            raise AirflowException(f"api failure: {e}")

        if response.status_code != 200:
            raise AirflowFailException(f"api failure, response : {response}")

        response = response.json()

        if response['boundaries_count'] == 0:
            # Return False to set the no data for grid
            return False
        elif response['message'] == 'Boundary List Not Found':
            # Return False to set the no data for grid
            return False
        elif response['data'] == None:
            raise AirflowFailException(f"api response data is None : {response}")
        else:
            return response

    def convert_response(self, api_response, temporal_dimension, spatial_dimension,product_dimension,dataset_list):
        """
        Convert API response to Features collection List
        Parameters:
        api_response: provider API response received
        temporal_dimension: temporal dimension data provided in the config
        spatial_dimension: spatial dimension data provided in the config
        product_dimension: product dimension data provided in the config
        Returns:
        list: returns list of features collections
        """
        features = []
        properties = {}
        if api_response:
            for data in api_response['data']:
                feature_properties = json_extract(data)
                data['boundaryData'].update(precision=15)
                feature = Feature(geometry=data['boundaryData'], properties=feature_properties)
                features.append(feature)
        properties.update(temporal_dimension=temporal_dimension, spatial_dimension=spatial_dimension,
                          product_dimension=product_dimension, dataset_list=dataset_list)
        feature_collections = FeatureCollection(features,properties=properties)
        return [feature_collections]

    def get_api_response(self, process_job_id, provider_params,temporal_dimension,product_dimension,spatial_dimension,
                         geometry, dataset_list, validate):
        """
        Fetch API response and return response in Standardized format
        Parameters:
        provider_params: provider parameters provided in the config
        temporal_dimension: temporal dimension data provided in the config
        spatial_dimension: spatial dimension data provided in the config
        product_dimension: product dimension data provided in the config
        geometry: 10X10 KM grid from global reference mart
        Returns:
        dict: returns api response
        """
        payload = json.dumps({
            "providerName": provider_params.get('providerName',None),
            "cropName": UnPackObj().extract_obj(product_dimension).get('CROP',None),
            "cropYear": UnPackObj().extract_obj(temporal_dimension).get('YEAR',None),
            "inputGeometry": json.loads(geometry['geom'])
        })
        print(payload)
        # get the fbm api response
        api_response = self.send_api_request(payload)
        standardize_response = self.convert_response(api_response, temporal_dimension, spatial_dimension,product_dimension,dataset_list)
        return standardize_response


class CropFactProvider:
    """
    Method for CropFact Environmental Types Provider
    """
    def __init__(self, provider):
        # Fetch provider url
        self.provider_url = Providers().get_provider(provider.lower())

    def validate_template(self, template):
        ValidateTemplate(**template)

    def api_payload(self,temporal_dimension,product_dimension,spatial_dimension, geometry):
        """
        Provide formatted payload or Body to request Cropfact API
        Parameters:
        temporal_dimension: temporal dimension data provided in the config
        spatial_dimension: spatial dimension data provided in the config
        product_dimension: product dimension data provided in the config
        geometry: 10X10 KM grid from global reference mart
        Returns:
        json: returns json object
        """
        temporal_data = UnPackObj().extract_obj(temporal_dimension)
        spatial_data = UnPackObj().extract_obj(spatial_dimension)
        product_data = UnPackObj().extract_obj(product_dimension)

        planting_year = temporal_data.get('Planting',None)
        harvest_year = temporal_data.get('Harvest',None)
        year = temporal_data.get('YEAR', None)
        if not planting_year or harvest_year:
            planting_year = harvest_year = year

        if product_data.get('WATERMGMT',None) == "IRRIGATED":
            irrigation = "Presence"
        else:
            irrigation = "Absence"
        json_body = {
            "scenario": {
                "croppingArea": {
                    "country": spatial_data.get('COUNTRY','GLOBAL'),
                    "scale": "0.1-degree",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            geometry['longitude'],
                            geometry['latitude']
                        ]
                    }
                },
                "genotype": {
                    "crop": product_data.get('CROP',"")
                },
                "management": {
                    "season": temporal_data.get('SEASON',None),
                    "operations": [
                        {
                            "name": "Planting",
                            "timestamp": str(planting_year) if planting_year is not None else None
                        },
                        {
                            "name": "Harvest",
                            "timestamp": str(harvest_year) if harvest_year is not None else None
                        },
                        {
                            "name": "Irrigation",
                            "status": irrigation,
                        }
                    ]
                }
            }
        }
        return json_body

    def get_api_response(self, process_job_id,provider_params, temporal_dimension,product_dimension,spatial_dimension,
                         geometry, dataset_list, validate):
        """
        Fetch API response and return response in Standardized format
        Parameters:
        provider_params: provider parameters provided in the config
        temporal_dimension: temporal dimension data provided in the config
        spatial_dimension: spatial dimension data provided in the config
        product_dimension: product dimension data provided in the config
        geometry: 10X10 KM grid from global reference mart
        Returns:
        dict: returns api response
        """
        api_response = []
        product_dimensions = []
        dataset_filtered_list = []
        irrigation_types = list(filter(lambda i: i['type'] == 'WATERMGMT', product_dimension))[0].get('value', None)
        if isinstance(irrigation_types, list):
            irrigation_types = irrigation_types
        else:
            irrigation_types = [irrigation_types]

        sql = f"select grid_id, product_dimension from datafactory.market_facts where " \
              f"definition_id = '{DEFINITION_ID}' and grid_id = '{geometry.get('grid_id', None)}'"
        market_facts_obj = pd.read_sql(sql, engine)
        market_facts_json = market_facts_obj.to_json(orient="records")
        market_facts_data = json.loads(market_facts_json)
        for fact in market_facts_data:
            irrigation_type_to_rmv = fact['product_dimension'].get('value', None)
            if irrigation_type_to_rmv in irrigation_types:
                irrigation_types.remove(irrigation_type_to_rmv)
        for irrigation_type in irrigation_types:
            product_dimension_obj = list(filter(lambda i: i['type'] != 'WATERMGMT', product_dimension))
            product_dimension_obj.extend([{"type": "WATERMGMT", "value": irrigation_type}])
            product_dimensions.append(product_dimension_obj)
            api_response.append(self.send_api_request(self.api_payload(temporal_dimension, product_dimension_obj,
                                                                       spatial_dimension, geometry)))
            # filter dataset list and return to process irrigated or non-irrigated
            dataset_generic = list(filter(lambda i: i['type'] == 'generic', dataset_list))
            if dataset_generic[0].get('dataset_id', None):
                dataset_filtered_list.append(dataset_list)
            else:
                dataset_filtered_obj = list(filter(lambda i: i['type'] == irrigation_type.lower(), dataset_list))
                dataset_filtered_obj[0].update(type='generic')
                dataset_filtered_list.append(dataset_filtered_obj)

        standardize_response = self.convert_response(api_response, temporal_dimension, spatial_dimension,
                                                     product_dimensions,dataset_filtered_list)
        return standardize_response

    def send_api_request(self, json_body):
        """ Function to get the API response
        Parameters:
        json_body: json body which will be sent as payload to api
        Returns:
        dict: returns api response
        """
        headers = {'x-api-key': CROPFACT_AUTHKEY, 'Content-Type': 'application/json'}

        try:
            response = requests.post(url=self.provider_url,headers=headers,json=json_body)
        except Exception as e:
            # raise exception with error message
            raise AirflowException(f"api failure: {e}")

        if response.status_code != 200:
            raise AirflowFailException(f"api failure, response : {response.json()}")
        elif response.json().get('errorCode',None) == 400:
            return False
        response = response.json()
        return response

    def convert_response(self, api_response, temporal_dimension, spatial_dimension,product_dimensions,dataset_filtered_list):
        """
        Convert API response to Features collection List
        Parameters:
        api_response: response received from the API
        temporal_dimension: temporal dimension data provided in the config
        spatial_dimension: spatial dimension data provided in the config
        product_dimension: product dimension data provided in the config
        Returns:
        list: returns list of feature collections
        """
        feature_collections=[]
        for response, product_dimension, dataset_list in zip(api_response, product_dimensions,dataset_filtered_list):
            features = []
            properties = {}
            if response:
                feature_properties = self.json_unpack(response)
                feature = Feature(geometry=feature_properties.pop('geometry'), properties=feature_properties)
                features.append(feature)
            properties.update(temporal_dimension=temporal_dimension, spatial_dimension=spatial_dimension,
                              product_dimension=product_dimension, dataset_list = dataset_list)
            feature_collections.append(FeatureCollection(features, properties=properties))
        return feature_collections

    def json_unpack(self, response_obj):
        """Recursively fetch values from nested JSON.
        Parameters:
        response_obj: response obj received from the API
        Returns:
        list: returns list of values
        """
        properties = {}
        temp_dict = {"key_name": ""}

        def parse_env_types(key_name, objs):
            for obj in objs:
                if key_name == "hierarchicalNeighborhoods":
                    temp_dict['key_name'] = obj.get('name',None).lower()
                for k, v in obj.items():
                    if isinstance(v, list):
                        if k == "classes":
                            sorted_list = sorted(v, key=lambda d: d.get('probability',None))
                            parse_env_types(k, [sorted_list][-1])
                    else:
                        properties[f"{temp_dict['key_name']}_{k}"] = v

        def extract(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k == "hierarchicalNeighborhoods":
                        if isinstance(v, list):
                            parse_env_types(k, v)
                    elif k == "geometry":
                        properties['geometry'] = v
                    elif isinstance(v, (dict, list)):
                        extract(v)

            elif isinstance(obj, list):
                for item in obj:
                    extract(item)
            return properties

        values = extract(response_obj)
        return values


class CropFactPhysiologyProvider:
    """
    Method for Crop Physiology Provider
    """
    def __init__(self, provider):
        self.geom = {}
        self.nested_json = []
        # Fetch provider url
        self.provider_url = Providers().get_provider(provider.lower())

    def validate_template(self, template):
        ValidateTemplate(**template)

    def api_payload(self,temporal_dimension,product_dimension,spatial_dimension, geometry):
        """
        Provide formatted payload or Body to request Cropfact API
        Parameters:
        temporal_dimension: temporal dimension data provided in the config
        spatial_dimension: spatial dimension data provided in the config
        product_dimension: product dimension data provided in the config
        geometry: 10X10 KM grid from global reference mart
        Returns:
        json: returns json object
        """
        temporal_data = UnPackObj().extract_obj(temporal_dimension)
        spatial_data = UnPackObj().extract_obj(spatial_dimension)
        product_data = UnPackObj().extract_obj(product_dimension)
        planting_year = temporal_data.get('Planting', None)
        harvest_year = temporal_data.get('Harvest', None)
        year = temporal_data.get('YEAR', None)
        if not planting_year or harvest_year:
            planting_year = harvest_year = year

        if product_data.get('WATERMGMT',None) == "IRRIGATED":
            irrigation = "Presence"
        else:
            irrigation = "Absence"
        json_body = {
            "scenario": {
                "croppingArea": {
                    "country": spatial_data.get('COUNTRY','GLOBAL'),
                    "scale": "0.1-degree",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            geometry['longitude'],
                            geometry['latitude']
                        ]
                    }
                },
                "genotype": {
                    "crop": product_data.get('CROP',"")
                },
                "management": {
                    "season": temporal_data.get('SEASON',None),
                    "operations": [
                        {
                            "name": "Planting",
                            "timestamp": str(planting_year) if planting_year is not None else None
                        },
                        {
                            "name": "Harvest",
                            "timestamp": str(harvest_year) if harvest_year is not None else None
                        },
                        {
                            "name": "Irrigation",
                            "status": irrigation,
                        }
                    ]
                }
            }
        }
        return json_body

    def get_api_response(self,process_job_id, provider_params, temporal_dimension,product_dimension,spatial_dimension,
                         geometry,dataset_list, validate):
        """
        Fetch API response and return response in Standardized format
        Parameters:
        provider_params: provider parameters provided in the config
        temporal_dimension: temporal dimension data provided in the config
        spatial_dimension: spatial dimension data provided in the config
        product_dimension: product dimension data provided in the
        geometry: 10X10 KM grid from global reference mart
        Returns:
        dict: returns api response
        """
        api_response = []
        dataset_filtered_list = []
        product_dimensions = []
        irrigation_types = list(filter(lambda i: i['type'] == 'WATERMGMT', product_dimension))[0].get('value',None)
        if isinstance(irrigation_types,list):
            irrigation_types = irrigation_types
        else:
            irrigation_types = [irrigation_types]
        sql = f"select grid_id, product_dimension from datafactory.market_facts where " \
              f"definition_id = '{DEFINITION_ID}' and grid_id = '{geometry.get('grid_id', None)}'"
        market_facts_obj = pd.read_sql(sql, engine)
        market_facts_json = market_facts_obj.to_json(orient="records")
        market_facts_data = json.loads(market_facts_json)
        for fact in market_facts_data:
            irrigation_type_to_rmv = fact['product_dimension'].get('value', None)
            if irrigation_type_to_rmv in irrigation_types:
                irrigation_types.remove(irrigation_type_to_rmv)
        for irrigation_type in irrigation_types:
            product_dimension_obj = list(filter(lambda i: i['type'] != 'WATERMGMT', product_dimension))
            product_dimension_obj.extend([{"type": "WATERMGMT", "value": irrigation_type}])
            product_dimensions.append(product_dimension_obj)
            api_response.append(self.send_api_request(self.api_payload(temporal_dimension, product_dimension_obj,
                                                                       spatial_dimension, geometry)))
            # filter dataset list and return to process irrigated or non-irrigated
            dataset_generic = list(filter(lambda i: i['type'] == 'generic', dataset_list))
            if dataset_generic[0].get('dataset_id', None):
                dataset_filtered_list.append(dataset_list)
            else:
                dataset_filtered_obj = list(filter(lambda i: i['type'] == irrigation_type.lower(), dataset_list))
                dataset_filtered_obj[0].update(type='generic')
                dataset_filtered_list.append(dataset_filtered_obj)

        standardize_response = self.convert_response(api_response, temporal_dimension, spatial_dimension,
                                                     product_dimensions,dataset_filtered_list)
        return standardize_response

    def send_api_request(self, json_body):
        """ Function to get the API response
        Parameters:
        json_body: json_body to send an request to API
        Returns:
        dict: returns api response
        """
        headers = {'x-api-key': CROPFACT_AUTHKEY, 'Content-Type': 'application/json'}

        try:
            response = requests.post(url=self.provider_url,headers=headers,json=json_body)
        except Exception as e:
            # raise exception with error message
            raise AirflowException(f"api failure: {e}")

        if response.status_code != 200:
            raise AirflowFailException(f"api failure, response : {response.json()}")
        elif response.json().get('errorCode',None) == 400:
            return False
        elif response.json().get('error',None):
            return False
        response = response.json()
        return response

    def convert_response(self, api_response, temporal_dimension, spatial_dimension, product_dimensions,
                         dataset_filtered_list):
        """
        convert api response to features collection
        Parameters:
        api_response: response received from the API
        temporal_dimension: temporal dimension data provided in the config
        spatial_dimension: spatial dimension data provided in the config
        product_dimension: product dimension data provided in the config
        Returns:
        list: returns list of feature collections
        """
        feature_collections = []
        for response, product_dimension, dataset_list in zip(api_response, product_dimensions, dataset_filtered_list):
            if not response:
                properties = {}
                temporal_dimension_data = []
                temporal_dimension_data.extend(list(filter(lambda i: i['type'] != 'GROWTH_STAGE', temporal_dimension)))
                temporal_dimension_data.extend([{"type": "GROWTH_STAGE", "value": ""},{"type": "START_DATE", "value": ""},
                                                {"type": "END_DATE", "value": ""},{"type": "END_YEAR", "value": ""}])
                properties.update(temporal_dimension=temporal_dimension_data, spatial_dimension=spatial_dimension,
                                  product_dimension=product_dimension, dataset_list = dataset_list)
                feature_collections.append(FeatureCollection([],properties=properties))
            else:
                variables_obj = self.json_unpack(response)
                df = pd.json_normalize(variables_obj)
                extract_variables_code = self.json_extract(response, 'code')
                variables_df = pd.json_normalize(extract_variables_code)
                df['growth_stage'].fillna("", inplace=True)
                growth_stages = df.growth_stage.unique()

                for gs in growth_stages:
                    properties = {}
                    filter_df = df[df['growth_stage'] == gs]
                    temporal_dimension_data = []
                    temporal_dimension_data.extend(list(filter(lambda i: i['type'] != 'GROWTH_STAGE', temporal_dimension)))
                    temporal_dimension_data.extend([{"type":"GROWTH_STAGE","value":filter_df['growth_stage'].values[0]}])

                    split_growth_stages = filter_df['growth_stage'].values[0].split("-")
                    find_growth_stage = variables_df.loc[(variables_df['name'].isin(split_growth_stages))]
                    try:
                        start_date = \
                        find_growth_stage[find_growth_stage['name'] == split_growth_stages[0]]['timestamp'].values[0]
                    except IndexError:
                        if split_growth_stages[1] == 'Planting':
                            if split_growth_stages[0] == '30DBP':
                                dbp = timedelta(days=30)
                            else:
                                raise ValueError(f"Unknown days before planting variable: {split_growth_stages[0]}")
                            planting_timestamp = find_growth_stage[find_growth_stage['name'] == 'Planting']['timestamp'].values[0]
                            begin_date = datetime.strptime(planting_timestamp, "%Y-%m-%d")
                            days_before_planting = (begin_date - dbp)
                            start_date = str(days_before_planting.strftime("%Y-%m-%d"))
                        else:
                            start_date = split_growth_stages[0]
                    try:
                        end_date = \
                        find_growth_stage[find_growth_stage['name'] == split_growth_stages[1]]['timestamp'].values[0]
                    except IndexError:
                        end_date = start_date
                    end_year = int(end_date.split("-")[0])
                    temporal_dimension_data.extend([{"type": "START_DATE", "value": start_date},
                                                    {"type": "END_DATE", "value": end_date},
                                                    {"type": "END_YEAR", "value": end_year}])

                    feature_properties = json.loads(filter_df[filter_df.columns.difference(["growth_stage", "geometry.type",
                                                                                            "geometry.coordinates"])].sum().to_json())
                    properties.update(temporal_dimension=temporal_dimension_data, spatial_dimension=spatial_dimension,
                                      product_dimension=product_dimension,dataset_list = dataset_list)
                    geometry = {'type': filter_df['geometry.type'].values[0],
                                'coordinates': filter_df['geometry.coordinates'].values[0]}
                    feature = Feature(geometry=geometry, properties=feature_properties)
                    feature_collections.append(FeatureCollection([feature],properties=properties))
        return feature_collections

    def json_unpack(self, response_obj):
        """
        unpack api response, extract required data with mapping growth stages and return nested json
        Parameters:
        response_obj: response received from the API
        Returns:
        json: returns nested json object
        """
        variable_names = self.json_extract(response_obj, 'code')
        df = pd.json_normalize(variable_names)

        for i, row in df.iterrows():
            row.fillna("", inplace=True)
            data = {}
            name = row['name']
            code = row['code']
            values = row['value']
            growth_stages = row['timestamp']

            if isinstance(growth_stages, list) or isinstance(values, list):
                if not values:
                    values = [None]
                for value, growth_stage in zip(values, growth_stages):
                    data = {}
                    data[f"{name}_{code}"] = value
                    data['growth_stage'] = growth_stage
                    data.update(self.geom)
                    self.nested_json.append(data)
        return self.nested_json

    def json_extract(self,obj, key):
        """Recursively fetch values from nested JSON."""
        arr = []

        def extract(obj, arr, key):
            """Recursively search for values of key in JSON tree."""
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k == "geometry":
                        self.geom.update(geometry=v)
                    if isinstance(v, (dict, list)):
                        extract(v, arr, key)
                    elif k == key:
                        arr.append(obj)

            elif isinstance(obj, list):
                for item in obj:
                    extract(item, arr, key)
            return arr

        values = extract(obj, arr, key)
        return values


class GRISWCSProvider:

    def __init__(self, provider):
        self.GRIS_AUTHKEY = str(os.getenv("GRIS_AUTHKEY",None))
        self.RASTER_DIR = str(os.getenv("RASTER_DIR",None))
        self.FILE_PATH = str(os.getenv("FILE_PATH", None))
        self.provider_url = Providers().get_provider(provider.lower())

    def validate_template(self, template):
        ValidateTemplate(**template)
        crop_code = UnPackObj().extract_obj(template['product_dimension']).get('CODE', None)
        if not crop_code:
            raise ValueError("Crop Code is Required")

    def download_file(self, run_id, geometry, provider_params):
        coverage_id = provider_params.get('coverage_id',None)
        poly_geom = shape(json.loads(geometry.get('geom')))
        poly_bounds = poly_geom.bounds
        longitude = f'Long("{poly_bounds[0]}","{poly_bounds[2]}")'
        latitude = f'Lat("{poly_bounds[1]}","{poly_bounds[3]}")'
        download_url = f'{self.provider_url}service=WCS&version=2.0.1&request=GetCoverage&subset={longitude}&subset={latitude}&' \
                       f'coverageId=gris:{coverage_id}&authkey={self.GRIS_AUTHKEY}&format=geotiff&subsettingcrs=http://www.opengis.net/def/crs/EPSG/0/4326'
        #path = os.path.join(tempfile.gettempdir(), self.RASTER_DIR)
        path = os.path.join(self.FILE_PATH, self.RASTER_DIR)
        if not os.path.exists(path):
            os.makedirs(path)
        self.file_name = f'{path}/{run_id}.tiff'
        file_download = requests.get(download_url, stream=True)
        with open(self.file_name, 'wb') as out_file:
            shutil.copyfileobj(file_download.raw, out_file)

        if file_download.status_code == 200 and file_download.headers['Content-Type'] == 'geotiff':
            self.raster_reprojection(geometry)
            del file_download
            return True
        elif file_download.status_code == 200 and file_download.headers['Content-Type'] == 'application/xml; charset=utf-8':
            del file_download
            return False
        else:
            raise ValueError("Unable to download file")

    def coordinate_system(self, geometry):
        longitude = geometry.get('longitude', None)
        latitude = geometry.get('latitude', None)
        params = {
            'authkey': self.GRIS_AUTHKEY,
            'service': 'wfs',
            'version': '1.1.0',
            'request': 'GetFeature',
            'outputFormat': 'json',
            'typeName': 'gris:World_UTM_Grid_epsg',
            'cql_filter': f'INTERSECTS(geometry, POINT({latitude} {longitude}))',
            'maxFeatures': '1'
        }
        response = requests.get(params=params, url='https://gris.syngentadigitalapps.com/geoserver/ows')
        coordinate_system = ExtractValue().nested_json_value_extract(response.json(),'epsg')
        return coordinate_system


    def raster_reprojection(self,geometry):
        coordinate_sys = self.coordinate_system(geometry)
        if coordinate_sys:
            dst_crs = coordinate_sys[0]
        else:
            raise ValueError("Couldn't set coordinate system")
        with rasterio.open(self.file_name) as src:
            transform, width, height = calculate_default_transform(src.crs, dst_crs, src.width, src.height, *src.bounds)
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': dst_crs,
                'transform': transform,
                'width': width,
                'height': height
            })
            with rasterio.open(self.file_name, 'w', **kwargs) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest)

    def raster_data_filter(self, raster_data_obj, raster_filter_code):
        px_values = self.raster_data(raster_data_obj)
        pixel_x, pixel_y = raster_data_obj.res
        pixel_counts = px_values.count(int(raster_filter_code))
        value = (pixel_x * pixel_y * pixel_counts)
        return value

    def raster_data(self, raster_data_obj):
        raster_array = raster_data_obj.read()
        px_values = raster_array.flatten().tolist()
        return px_values

    def get_api_response(self, run_id,provider_params,temporal_dimension,product_dimension, spatial_dimension,
                         geometry, dataset_list, validate):

        download_file = self.download_file(run_id, geometry, provider_params)
        if download_file:
            raster_data_obj = rasterio.open(self.file_name)
        else:
            raster_data_obj = None
        if not validate and os.path.exists(self.file_name):
            os.remove(self.file_name)
        standardize_data = self.convert_response(temporal_dimension, spatial_dimension,product_dimension, geometry,
                                                 raster_data_obj, dataset_list)
        return standardize_data

    def convert_response(self,temporal_dimension, spatial_dimension,product_dimension, geometry,raster_data_obj,
                         dataset_list):

        properties = {}
        raster_filter_code = ObjectExtract().extract_value(product_dimension, "CODE").get('value', None)
        if raster_data_obj:
            raster_data = self.raster_data(raster_data_obj)
            raster_data_filtered = self.raster_data_filter(raster_data_obj, raster_filter_code)
        else:
            raster_data = None
            raster_data_filtered = None
        feature = Feature(geometry=shape(json.loads(geometry.get('geom'))),
                          properties={'raster_data':raster_data,'raster_data_filtered':raster_data_filtered})
        properties.update(temporal_dimension=temporal_dimension, spatial_dimension=spatial_dimension,
                          product_dimension=product_dimension, dataset_list=dataset_list)
        feature_collections = FeatureCollection([feature],properties=properties)
        return [feature_collections]


class SatsureProvider:

    def __init__(self, provider):
        self.SPARTA_AUTHKEY = str(os.getenv("SPARTA_AUTHKEY", None))
        self.RASTER_DIR = str(os.getenv("RASTER_DIR", None))
        self.FILE_PATH = str(os.getenv("FILE_PATH", None))
        self.PROVIDER_API = Providers().get_provider(provider.lower())

    def validate_template(self, template):
        ValidateTemplate(**template)
        crop_code = UnPackObj().extract_obj(template.get('product_dimension', None)).get('CODE', None)
        if not crop_code:
            raise ValueError("Crop Code is Required")

    def request_data(self, payload):
        """ Function to request_data from provider
        Parameters:
        payload: provider API body to send request
        Returns:
        dict: returns API response
        """
        request_data_url = f'{self.PROVIDER_API}request-data?api_key={self.SPARTA_AUTHKEY}'
        try:
            response = requests.post(
                request_data_url,
                json=payload,
            )
        except Exception as e:
            # raise exception with error message
            raise AirflowException(f"api failure: {e}")

        if response.status_code == 400:
            error_msg = response.json().get('message',None).get('message',None)
            if error_msg == "Area of interest doesn't fall under the servicable areas":
                return 'No Data'

        if response.status_code != 200:
            raise AirflowFailException(f"api failure, response : {response.content}")

        response = response.json()
        request_id = response.get('data', None).get('request_id', None)
        if request_id:
            download_data = self.download_data(request_id)
            return download_data
        else:
            raise AirflowFailException(f"api failure, response : {response.content}")

    def download_data(self, request_id):
        download_data_url = f'{self.PROVIDER_API}download-data/{request_id}?api_key={self.SPARTA_AUTHKEY}'
        try:
            response = requests.get(
                download_data_url,
            )
        except Exception as e:
            # raise exception with error message
            raise AirflowException(f"api failure: {e}")

        if response.status_code != 200:
            raise AirflowFailException(f"api failure, response : {response.content}")
        if response.json().get('data', None).get('crops_data', None).get('code', None) == 400:
            raise AirflowFailException(f"api failure, response : {response.content}")

        response = response.json()
        url = ExtractValue().nested_json_value_extract(response,'url')
        return url

    def api_payload(self, temporal_dimension,product_dimension, geometry):
        """
        Provide formatted payload or Body to request Satsure API
        Parameters:
        temporal_dimension: temporal dimension data provided in the config
        geometry: 10X10 KM grid from global reference mart
        Returns:
        json: returns json object
        """
        temporal_data = UnPackObj().extract_obj(temporal_dimension)
        product_dimension = UnPackObj().extract_obj(product_dimension)

        from_date = str(temporal_data.get('FROM_DATE', ''))
        to_date = str(temporal_data.get('TO_DATE', ''))

        if product_dimension.get('CROP', None):
            crop = [str(product_dimension.get('CROP', None))]
        else:
            crop = []
        geom = json.loads(geometry.get('geom'))
        json_body = {
            "crops": crop,
            "aoi": {
                "type": geom['type'],
                "coordinates" : geom['coordinates']
            },
            "from_date": from_date,
            "to_date": to_date
        }
        return json_body

    def download_file(self, run_id, geometry, provider_params,temporal_dimension, product_dimension):
        download_url = self.request_data(self.api_payload(temporal_dimension,product_dimension, geometry))
        if download_url == "No Data":
            self.file_name = 'None'
            return  "No Data"
        #path = os.path.join(tempfile.gettempdir(), self.RASTER_DIR)
        path = os.path.join(self.FILE_PATH, self.RASTER_DIR)
        if not os.path.exists(path):
            os.makedirs(path)
        self.file_name = f'{path}/{run_id}.tiff'
        http = urllib3.PoolManager()
        with http.request('GET', download_url[0], preload_content=False) as resp, \
                open(self.file_name, 'wb') as out_file:
            resp_obj = resp
            shutil.copyfileobj(resp, out_file)

        if resp_obj.status == 200:
            return True
        else:
            raise ValueError(f"Error Downloading file")

    def raster_data_filter(self, raster_data_obj, raster_filter_code):
        px_values = self.raster_data(raster_data_obj)
        pixel_x, pixel_y = raster_data_obj.res
        pixel_counts = px_values.count(int(raster_filter_code))
        value = (pixel_x * pixel_y * pixel_counts)
        return value

    def raster_data(self, raster_data_obj):
        raster_array = raster_data_obj.read()
        px_values = raster_array.flatten().tolist()
        return px_values

    def get_api_response(self, run_id, provider_params, temporal_dimension, product_dimension, spatial_dimension,
                         geometry, dataset_list, validate):

        download_file = self.download_file(run_id, geometry, provider_params,temporal_dimension, product_dimension)
        if download_file and download_file != 'No Data':
            raster_data_obj = rasterio.open(self.file_name)
        else:
            raster_data_obj = None
        if not validate and os.path.exists(self.file_name):
            os.remove(self.file_name)
        standardize_data = self.convert_response(temporal_dimension, spatial_dimension, product_dimension, geometry,
                                                 raster_data_obj, dataset_list)
        return standardize_data

    def convert_response(self, temporal_dimension, spatial_dimension, product_dimension, geometry, raster_data_obj,
                         dataset_list):

        properties = {}
        raster_filter_code = ObjectExtract().extract_value(product_dimension, "CODE").get('value', 1)
        if raster_data_obj:
            raster_data = self.raster_data(raster_data_obj)
            raster_data_filtered = self.raster_data_filter(raster_data_obj, raster_filter_code)
        else:
            raster_data = None
            raster_data_filtered = None
        feature = Feature(geometry=shape(json.loads(geometry.get('geom'))),
                          properties={'raster_data': raster_data, 'raster_data_filtered': raster_data_filtered})
        properties.update(temporal_dimension=temporal_dimension, spatial_dimension=spatial_dimension,
                          product_dimension=product_dimension, dataset_list=dataset_list)
        feature_collections = FeatureCollection([feature], properties=properties)
        return [feature_collections]


class TestProvider:
    """
    Method for Tes Provider
    Parameters:
    provider: provider name
    """
    def __init__(self, provider):
        self.provider = str(provider)

    def validate_template(self, template):
        ValidateTemplate(**template)

    def convert_response(self,temporal_dimension, spatial_dimension,product_dimension,dataset_list,geometry):
        """
        Convert API response to Features collection List
        Parameters:
        api_response: provider API response received
        temporal_dimension: temporal dimension data provided in the config
        spatial_dimension: spatial dimension data provided in the config
        product_dimension: product dimension data provided in the config
        Returns:
        list: returns list of features collections
        """
        properties = {}

        feature = Feature(geometry=shape(json.loads(geometry.get('geom'))),
                          properties={'test_data': 1.0,})
        properties.update(temporal_dimension=temporal_dimension, spatial_dimension=spatial_dimension,
                          product_dimension=product_dimension, dataset_list=dataset_list)
        feature_collections = FeatureCollection([feature], properties=properties)
        return [feature_collections]

    def get_api_response(self, process_job_id, provider_params,temporal_dimension,product_dimension,spatial_dimension,
                         geometry, dataset_list, validate):
        """
        Fetch API response and return response in Standardized format
        Parameters:
        provider_params: provider parameters provided in the config
        temporal_dimension: temporal dimension data provided in the config
        spatial_dimension: spatial dimension data provided in the config
        product_dimension: product dimension data provided in the config
        geometry: 10X10 KM grid from global reference mart
        Returns:
        dict: returns api response
        """
        standardize_response = self.convert_response(temporal_dimension, spatial_dimension,
                                                     product_dimension,dataset_list, geometry)
        return standardize_response


PROVIDERLOOKUP = {
    'fbm': FBMProvider,
    'cropfact-environmentalclassification':CropFactProvider,
    'cropfact-cropphysiology': CropFactPhysiologyProvider,
    'gris-wcs': GRISWCSProvider,
    'satsure-sparta': SatsureProvider,
    'test-provider': TestProvider
}
