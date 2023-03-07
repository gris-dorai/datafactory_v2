# -*- coding: utf-8 -*-
"""
config has information of the Providers and Variables
"""

template_geojson = {
    "name": "TESTING_NA",
    "provider": "fbm",
    "provider_params": {
        "providerName": "cropio"
    },
    "crop": "Soya bean",
    "region": "NA",
    "year": "2019",
    "validate": False,
    "checkIrrigated": False,
    "context_code": "A_CROPIO_PRODUCTIVITY_EST",
    "geometry": {
        "type": "MultiPolygon",
        "coordinates": [[
            [
                [
                    -94.85,
                    43.05
                ],
                [
                    -94.85,
                    43.15
                ],
                [
                    -94.75,
                    43.15
                ],
                [
                    -94.75,
                    43.05
                ],
                [
                    -94.85,
                    43.05
                ]
            ]
        ]]
    }
}


'''
#Template which can be used to fail secondary dag

template_geojson = {
    "name": "TESTING_NA",
    "provider": "fbm",
    "provider_params": {
        "providerName": "cropio"
    },
    "crop": "Soya bean",
    "region": "NA",
    "year": "2019",
    "validate": false,
    "checkIrrigated": false,
    "task_state": "fail",
    "geometry": {
        "type": "MultiPolygon",
        "coordinates": [[
            [
                [
                    -94.85,
                    43.05
                ],
                [
                    -94.85,
                    43.15
                ],
                [
                    -94.75,
                    43.15
                ],
                [
                    -94.75,
                    43.05
                ],
                [
                    -94.85,
                    43.05
                ]
            ]
        ]]
    }
}
'''

sample_cropfact_template = {
    "name": "TESTING_ENV_CLASSIFICATION",
    "provider": "cropfact-environmentalclassification",
    "provider_params": {
        "providerName": "cropfact-environmentalclassification"
    },

    "validate": False,
    "context_code": "A_CROPIO_PRODUCTIVITY_EST",

    "temporal_dimension": [{
        "YEAR": "2022",
        "SEASON":1,
    }
    ],

    "measure_dimension":[
        "LEVELZEROCODE",
        "LEVELONECODE",
        "LEVELTWOCODE",
        "LEVELZEROPROBABILITY",
        "LEVELONEPROBABILITY",
        "LEVELTWOPROBABILITY",
    ],

    "product_dimension":[{
            "CROP": "Corn Grain",
            "WATERMGMT":"non-irrigated"
        }
    ],

    "spatial_dimension":[{
        "REGION": "NA",
        "COUNTRY":"GLOBAL",
        "GEOMETRY": {
            "type": "MultiPolygon",
            "coordinates": [[
                [
                    [
                        -94.85,
                        43.05
                    ],
                    [
                        -94.85,
                        43.15
                    ],
                    [
                        -94.75,
                        43.15
                    ],
                    [
                        -94.75,
                        43.05
                    ],
                    [
                        -94.85,
                        43.05
                    ]
                ]
            ]]
        },
    }
]
}

# sample template to run Batch Jobs
sample_batch_jobs_template = {
    'start_year': 2018,
    'end_year': 2020,
    'template': {
        "name": "FBM-CROPIO-SOYABEAN-2019-NA-1-GRID-TEST",
        "provider": "FBM",
        "data_product_unique_id": "4a4e2f6c-a6ef-5213-ae99-fe9d865de7b7",
        "provider_params": {
            "providerName": "CROPIO"
        },
        "validate": False,
        "context_code": "A_CROPIO_PRODUCTIVITY_EST",
        "temporal_dimension": [
            {
                "type": "YEAR",
                "value": 2019
            }
        ],
        "measure_dimension": [
            "LANDUSE",
            "LANDUSEPCT",
            "FIELDSIZEMED",
            "FIELDSIZESD",
            "YIELDESTIMATE"
        ],
        "product_dimension": [
            {
                "type": "CROP",
                "value": "Soya bean"
            },
            {
                "type": "WATERMGMT",
                "value": "NON-IRRIGATED"
            }
        ],
        "spatial_dimension": [
            {
                "type": "REGION",
                "value": "NA",
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [[
                        [
                            [
                                -94.85,
                                43.05
                            ],
                            [
                                -94.85,
                                43.15
                            ],
                            [
                                -94.75,
                                43.15
                            ],
                            [
                                -94.75,
                                43.05
                            ],
                            [
                                -94.85,
                                43.05
                            ]
                        ]
                    ]]
                }
            },
            {
                "type": "COUNTRY",
                "value": "US"
            }
        ]
    }
}

# sample template for Cropio/FBM Provider
sample_fbm_template = {
    "name": "FBM-CROPIO-SOYABEAN-2019-NA-100GRID-TEST",
    "provider": "FBM",
    "data_product_unique_id": "04342abd-3d69-550a-ac78-04ea846648b9",
    "provider_params": {
        "providerName": "CROPIO",
        "wait_time": 60,  # in seconds
    },
    "validate": False,
    "context_code": "A_CROPIO_PRODUCTIVITY_EST",
    "temporal_dimension": [
        {
            "type": "YEAR",
            "value": 2019
        }
    ],
    "measure_dimension": [
        "LANDUSE",
        "LANDUSEPCT",
        "FIELDSIZEMED",
        "FIELDSIZESD",
        "YIELDESTIMATE"
    ],
    "product_dimension": [
        {
            "type": "CROP",
            "value": "Soya bean"
        },
        {
            "type": "WATERMGMT",
            "value": "NON-IRRIGATED"
        }
    ],
    "spatial_dimension": [
        {
            "type": "REGION",
            "value": "NA",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            -95.35,
                            42.65
                        ],
                        [
                            -94.35,
                            42.65
                        ],
                        [
                            -94.35,
                            43.55
                        ],
                        [
                            -95.35,
                            43.55
                        ],
                        [
                            -95.35,
                            42.65
                        ]
                    ]
                ]
            }
        },
        {
            "type": "COUNTRY",
            "value": ""
        }
    ]
}

# sample template for cropio/FBM provider of 1 grid
sample_template = {
    "name": "FBM-CROPIO-SOYABEAN-2019-NA-1-GRID-TEST",
    "provider": "FBM",
    #"data_product_unique_id": "4a4e2f6c-a6ef-5213-ae99-fe9d865de7b7",
    "data_product_unique_id": None,
    "provider_params": {
        "providerName": "CROPIO",
        "wait_time": 60,  # in seconds
    },
    "trigger_fme": False,
    "fme_params":{
        "api_url":"http://deawilgisp009.syngentaaws.org:8080/fmejobsubmitter/Datafactory/df_gris_fbm_provider.fmw?",
        "destination_db":"SyngentaDev",
        "destination_table": "datafactory_statistics",
        "publishing_api_params":{"api-key": "3077e95c-b5be-4894-bbb4-5dbca5548bf2", "table": "nafta_acresplanted_corngrain_2021p",
                       "sql": "select * from public.nafta_acresplanted_corngrain_2021p", "title": "corngrain_acresplanted_2021",
                       "name": "corngrain_acresplanted_2021", "filename": "corngrain_acresplanted_2021.shp", "geom_col": "geom",
                       "geom_type": "polygon", "geom_epsg": "4326", "tags": "test", "organization": "Syngenta",
                       "license_id": "CC-BY-4.0", "description": "test_12", "notes": "test_12", "maintainer": "shreyas",
                       "maintainer_email": "shreyas.udupa@syngenta.com", "frequency": "Not Updated", "portfolio": ["Corn"],
                       "crop": ["Grain Corn"], "author": "Shreyas", "author_email": "shreyas.udupa@syngenta.com",
                       "support": "Best Effort", "version": "2.0", "product_group": ["Crop Enhancement"],
                       "data_standard": "pep", "language": "English", "temporal_coverage": "2021", "country": "AMERICAN SAMOA",
                       "dimension": "Grower", "data_quality": "Fair", "source": "usda, statcan", "license_title": "",
                       "private": False, "spatial": "", "groups": [], "last_updated_at": "2022-11-28"}
    },
    "validate": False,
    "context_code": "A_CROPIO_PRODUCTIVITY_EST",
    "temporal_dimension": [
        {
            "type": "YEAR",
            "value": 2019
        }
    ],
    "measure_dimension": [
        "LANDUSE",
        "LANDUSEPCT",
        "FIELDSIZEMED",
        "FIELDSIZESD",
        "YIELDESTIMATE"
    ],
    "product_dimension": [
        {
            "type": "CROP",
            "value": "Soya bean"
        },
        {
            "type": "WATERMGMT",
            "value": "NON-IRRIGATED"
        }
    ],
    "spatial_dimension": [
        {
            "type": "REGION",
            "value": "NA",
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [[
                [
                    [
                        -94.85,
                        43.05
                    ],
                    [
                        -94.85,
                        43.15
                    ],
                    [
                        -94.75,
                        43.15
                    ],
                    [
                        -94.75,
                        43.05
                    ],
                    [
                        -94.85,
                        43.05
                    ]
                ]
            ]]
            }
        },
        {
            "type": "COUNTRY",
            "value": ""
        }
    ]
}

# sample template for Crop Physiology Provider
sample_cropfact_physiology_template = {
    "name": "CROPFACT-PHYSIOLOGY-CORN-2019-EAME-100GRID-TEST",
    "provider": "cropfact-cropphysiology",
    "data_product_unique_id": "2a240224-c02b-578f-8efa-4088c1fb9d3a",
    "provider_params": {
        "providerName": "cropfact-cropphysiology",
        "wait_time": 60,  # in seconds
    },
    "validate": False,
    "temporal_dimension": [
        {
            "type": "YEAR",
            "value": 2019
        },
        {
            "type": "Planting",
            "value": 2019
        },
        {
            "type": "Harvest",
            "value": 2019
        },
        {
            "type": "SEASON",
            "value": 1
        },
        {
            "type": "GROWTH_STAGE",
            "value": "growth_stage"
        }
    ],
    "measure_dimension": [],
    "product_dimension": [
        {
            "type": "CROP",
            "value": "Corn Grain"
        },
        {
            "type": "WATERMGMT",
            "value": ["IRRIGATED","NON-IRRIGATED"]
        }
    ],
    "spatial_dimension": [
        {
            "type": "REGION",
            "value": "EAME",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            9.5,
                            48.15
                        ],
                        [
                            9.5,
                            48.15
                        ],
                        [
                            9.5,
                            49.5
                        ],
                        [
                            10.5,
                            49.5
                        ],
                        [
                            10.5,
                            48.15
                        ]
                    ]
                ]
            }
        },
        {
            "type": "COUNTRY",
            "value": "Germany"
        }
    ]
}

sample_gris_wcs_template = {
    "name": "GRIS-WCS-corn-2010-NA-1-GRID-TEST",
    "provider": "GRIS-WCS",
    "data_product_unique_id": "517e8742-96ba-5dd7-a8fd-ea9e9c928b7c",
    "provider_params": {
        "providerName": "GRIS-WCS",
      	"coverage_id": "Agriculture-Boundary-CRD-2010-NA-CropScape-USDA-v2022"
    },
    "validate": False,
    "temporal_dimension": [
        {
            "type": "YEAR",
            "value": 2010
        }
    ],
    "measure_dimension": [],
    "product_dimension": [
        {
            "type": "CROP",
            "value": "corn"
        },
        {
            "type": "WATERMGMT",
            "value": "NON-IRRIGATED"
        },
        {
            "type": "CODE",
            "value": 1
        }
    ],
    "spatial_dimension": [
        {
            "type": "REGION",
            "value": "NA",
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [[
                [
                    [
                        -94.85,
                        43.05
                    ],
                    [
                        -94.85,
                        43.15
                    ],
                    [
                        -94.75,
                        43.15
                    ],
                    [
                        -94.75,
                        43.05
                    ],
                    [
                        -94.85,
                        43.05
                    ]
                ]
            ]]
            }
        },
        {
            "type": "COUNTRY",
            "value": "US"
        }
    ]
}

sample_satsure_sparta_template = {
    "name": "Satsure-sparta-1-GRID-TEST",
    "provider": "SATSURE-SPARTA",
    "data_product_unique_id": "617e8742-96ba-5dd7-a8fd-ea9e9c928b7c",
    "provider_params": {
        "providerName": "SATSURE-SPARTA",
    },
    "validate": False,
    "temporal_dimension": [
        {
            "type": "YEAR",
            "value": 2022
        },
        {
            "type": "FROM_DATE",
            "value": "20220701"
        },
        {
            "type": "TO_DATE",
            "value": "20220801"
        }
    ],
    "measure_dimension": [],
    "product_dimension": [
        {
            "type": "CROP",
            "value": "corn"
        },
        {
            "type": "WATERMGMT",
            "value": "NON-IRRIGATED"
        },
        {
            "type": "CODE",
            "value": 1
        }
    ],
    "spatial_dimension": [
        {
            "type": "REGION",
            "value": "IN",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[75.65,17.35],[75.65,17.45],[75.75,17.45],[75.75,17.35],[75.65,17.35]]]
            }
        },
        {
            "type": "COUNTRY",
            "value": "IN"
        }
    ]
}
