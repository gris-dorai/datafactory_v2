
import statistics


def percent(x, y):
    """
    calculate percentage
    """

    try:
        input_values = sum(x)
        value = input_values * (100 / y)
    except:
        value = 0.0
    return value


def statistics_lookup(stat_method):
    """
    statistics method lookup
    """
    if not stat_method:
        return None
    statistics_methods = {'sum': sum, 'min': min, 'max': max, "percentage": percent, "raster_area": raster_area,
                          "raster_percentage": raster_percentage}
    get_stat_method = statistics_methods.get(stat_method,None)
    if not get_stat_method:
        get_method = getattr(statistics, stat_method)
    else:
        get_method = get_stat_method
    return get_method


def raster_area(input_value, total_grid_area):
    area = sum(input_value) * 0.0001 # in hectares
    return area


def raster_percentage(input_value,total_grid_area):
    area = raster_area(input_value, total_grid_area)
    area_percentage = percent([area], total_grid_area)
    return area_percentage