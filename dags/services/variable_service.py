from statistics import StatisticsError

from services import statistics_service
from models.datafactory import ProviderVariables, StandardVariables, Language, UnitOfMeasurement
from services.db_service import Session
import rasterio

df_session = Session()


class UnPackObj:
    """
    unpack json object
    """
    def extract_obj(self, list_obj):
        obj_unpacked = {}
        for obj in list_obj:
            key = obj['type']
            obj_unpacked[key] = obj['value']
        return obj_unpacked


class ParseJsonObj:
    """
    Parse Nested Json Object
    Parameters:
    response_obj: response received from the API
    json_path: path which is used to find the key value in the nested json object
    context_code: The code provided to check and validate value
    """
    def __init__(self, response_data, json_path, context_code):
        self.arr = []
        self.response_data = response_data
        self.json_path = json_path
        self.context_code = context_code

    def json_extract(self, obj, key):
        """Recursively fetch values from nested JSON."""

        data_list = []

        def extract(obj, data_list, key):
            """Recursively search for values of key in JSON tree."""
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k == key:
                        if k == 'contextItems.value' and not obj.get('contextItems.code',None) == self.context_code:
                            pass
                        else:
                            data_list.append(v)
                    elif isinstance(v, (dict, list)):
                        extract(v, data_list, key)

            elif isinstance(obj, list):
                for item in obj:
                    extract(item, data_list, key)
            return data_list

        values = extract(obj, data_list, key)
        self.arr.clear()
        self.arr.extend(data_list)
        return values

    def process_json(self):
        #nested_json = self.json_path.split('.')
        #for path in nested_json:

        path = self.json_path
        if not self.arr:
            self.json_extract(self.response_data, path)
        elif isinstance(self.arr, (dict, list)):
            self.json_extract(self.arr, path)
        return self.arr


class VariableCalculator:
    """
    Method to calculate variable values
    parameter:
    providervaribable_id: provider variable id to find the provider
    """
    def __init__(self, providervaribable_id):

        self.prov_variable = providervaribable_id
        self.providervariable_obj = df_session.query(ProviderVariables).\
            filter(ProviderVariables.provider_variable_id == self.prov_variable).first()
        self.standardvariable_obj = df_session.query(StandardVariables).\
            filter(StandardVariables.standard_variable_id == self.providervariable_obj.standard_variable_id).first()
        self.input_values = []
        self.summary = []
        self.provider_variable_id = self.providervariable_obj.provider_variable_id
        self.stat_func = statistics_service.statistics_lookup(self.providervariable_obj.stat_key)

    def parse_json_obj(self, response_obj,context_code):
        """
        parse json object
        Parameters:
        response_obj: response received from the API
        context_code: code used to validate the value
        Returns:
        json: returns nested json object
        """
        json_path = self.providervariable_obj.json_path
        parse_nested_obj = ParseJsonObj(response_obj, json_path, context_code).process_json()
        return parse_nested_obj

    def convert_uom(self, target_value):
        """
        unit of measurement conversion
        Parameters:
        target_value: value which needs to be converted to its unit of measurement
        Returns:
        float: returns converted value
        """
        get_uom = self.providervariable_obj.uom
        if not get_uom:
            return target_value
        get_uom_lang_code = self.providervariable_obj.uom_language_code

        uom_lang_obj = df_session.query(Language).filter(Language.language_code == get_uom_lang_code,
                                                      Language.name == get_uom).first()
        uom_obj = df_session.query(UnitOfMeasurement).filter(UnitOfMeasurement.unit_of_measurement_id ==
                                                          uom_lang_obj.unit_of_measurement_id).first()

        baseoffset = uom_obj.offset
        scale = uom_obj.scale
        return (target_value * float(scale)) + int(baseoffset)

    def load_value(self, response_obj, context_code):
        """
        parse and load variable values
        Parameters:
        response_obj: response object which received from API
        context_code: code which is used to validate the value
        """
        raw_values = self.parse_json_obj(response_obj, context_code)
        for r_value in raw_values:
            if isinstance(r_value, list):
                self.input_values.extend(r_value)
            elif r_value:
                self.input_values.append(self.convert_uom(r_value))

    def calculate_variable(self,total_grid_area):
        """
        calculate variable using returned statistics method
        Parameters:
        total_grid_area: total grid area to be calculated
        Returns:
        obj: returns provider_variable_id object
        float: returns calculated output value
        """
        if not self.stat_func:
            self.value = self.input_values[0] if self.input_values else None
            return self.provider_variable_id, self.value
        elif self.stat_func in [statistics_service.percent, statistics_service.raster_area,
                                statistics_service.raster_percentage]:
            self.value = self.stat_func(self.input_values, total_grid_area)
        else:
            try:
                self.value = self.stat_func(self.input_values)
            except StatisticsError:
                self.value = None

        return self.provider_variable_id, self.value

