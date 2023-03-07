import json
import math

from haversine import haversine, Unit
from shapely import wkt
from shapely.geometry import Point, shape, MultiPolygon, mapping

from services.area_calc_service import area


class FieldProduction:
    """
    Field Yield Production
    Parameters:
    geometry: 10X10 KM grid from global reference mart
    context_code: provider context code to check in API response
    """
    def __init__(self, geometry, context_code):

        get_wkt = shape(json.loads((geometry['geom'])))
        geom_wkt = get_wkt.wkt
        _geom = wkt.loads(geom_wkt)
        if _geom.geom_type == 'Polygon':
            geom_wkt = (MultiPolygon([_geom])).wkt
        g1 = wkt.loads(geom_wkt)
        g2 = mapping(g1)
        self.grid_geom = shape(json.loads(json.dumps(g2)))
        self.geometry = geometry
        self.context_code = context_code

    def find_quotient(self, field_obj):
        """
        Get area and calculate the perimeter of field object
        Parameters:
        field_obj: calculation to be done for provided field object
        Returns:
        float: returns calculated quotient value
        """
        perimeter = 0
        area_meters = area(self.geometry['geom'])

        for coordinate in field_obj['geometry']['coordinates']:
            for ring in coordinate:
                for first, second in zip(ring, ring[1:]):
                    # reverse Coordinates
                    point_a = [first[1], first[0]]
                    point_b = [second[1], second[0]]
                    currentEdgeLength = haversine(point_a, point_b, unit=Unit.METERS)
                    perimeter += currentEdgeLength

        field_obj['perimeter'] = perimeter

        # calculate isoperimetric quotient
        quotient = (4 * math.pi * area_meters) / (field_obj['perimeter'] * field_obj['perimeter'])
        return quotient

    def get_yield_production(self,field_obj):
        """
        get yeild production of provided field object
        Parameters:
        field_obj: calculation to be done for provided field object
        Returns:
        float: returns calculated quotient value
        boolean: returns True if yield production else returns False
        """
        data_centroid = Point((shape(field_obj['geometry'])).centroid)
        contains = self.grid_geom.contains(data_centroid)
        touches = self.grid_geom.touches(data_centroid)
        if contains or touches:
            quotient = self.find_quotient(field_obj)
            yeild_production = None
            if field_obj['properties']['contextItems.code'] == str(self.context_code):
                yeild_production = field_obj['properties']['contextItems.value']
            return yeild_production,quotient
        else:
            yeild_production = None
            quotient = 0.0
            return yeild_production,quotient