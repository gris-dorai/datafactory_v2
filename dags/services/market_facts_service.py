import json
import os
import pandas as pd
from services.db_service import engine
from dotenv import load_dotenv
from services.json_unpack_service import UnPackObj

file_path = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(file_path, ".env"))


def _get_marketfact_exclusion(crop, spatial_hiearchy_type, spatial_hiearchy_value):
    exclude_definition_id = str(os.getenv("EXCLUDE_DEFINITION_ID", ""))
    if crop:
        crop_filter = f"and crop = '{crop}'"
    else:
        crop_filter = ""
    sql = f"select grid_id, product_dimension from datafactory.market_facts where definition_id = '{exclude_definition_id}' " \
           f"{crop_filter} and {spatial_hiearchy_type} = '{spatial_hiearchy_value}'"
    return pd.read_sql(sql, engine)


def _filter_productdimension(market_facts, product_dimension):
    market_facts_json = market_facts.to_json(orient="records")
    market_facts_data = json.loads(market_facts_json)
    market_facts_irrigation_types = []
    irrigation_types = []
    for fact in market_facts_data:
        if not isinstance(fact['product_dimension'], str):
            for dim in product_dimension:
                if dim['type'] in fact['product_dimension'].values():
                    prod_dim = UnPackObj().extract_obj(product_dimension).get(dim['type'], None)
                    if isinstance(prod_dim, list):
                        irrigation_types.extend(prod_dim)
                    else:
                        irrigation_types.append(prod_dim)
                    market_facts_irrigation_types.append(fact['product_dimension'].get('value',None))
        else:
            return True

    if set(irrigation_types) == set(market_facts_irrigation_types):
        return True


def exclude(gridcell, crop, product_dimension):
    # Check adminlevel1
    market_facts_level_1 = _get_marketfact_exclusion(crop, "adminlevel1_code", gridcell['country_code'])
    if not market_facts_level_1.empty and _filter_productdimension(market_facts_level_1, product_dimension):
        return True

    # Check grid_id
    market_facts_level_2 = _get_marketfact_exclusion(None, "grid_id", gridcell['grid_id'])
    if not market_facts_level_2.empty and _filter_productdimension(market_facts_level_2, product_dimension):
        return True
    else:
        return False