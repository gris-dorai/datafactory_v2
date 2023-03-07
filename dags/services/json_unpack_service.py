
class ObjectExtract:
    def extract_value(self,obj, key_value):
        """Recursively fetch values from nested JSON."""
        arr = {}

        def extract(obj, arr, key_value):
            """Recursively search for values of key in JSON tree."""
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k == key_value:
                        arr.update(value=v)
                    elif isinstance(v, (dict, list)):
                        extract(v, arr, key_value)
                    elif k == "type" and v == key_value:
                        arr.update(value=obj['value'])

            elif isinstance(obj, list):
                for item in obj:
                    extract(item, arr, key_value)
            return arr

        values = extract(obj, arr, key_value)
        return values


class UnPackObj:
    """
    Unpack Json Object
    """
    def extract_obj(self,list_obj):
        obj_unpacked= {}
        for obj in list_obj:
            key = obj['type']
            obj_unpacked[key] = obj['value']
        return obj_unpacked


class ExtractValue:

    def nested_json_value_extract(self, obj, key):
        """Recursively fetch values from nested JSON."""
        arr = []
        def extract(obj, arr, key):
            """Recursively search for values of key in JSON tree."""
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        extract(v, arr, key)
                    elif k == key:
                        arr.append(v)
            elif isinstance(obj, list):
                for item in obj:
                    extract(item, arr, key)
            return arr

        values = extract(obj, arr, key)
        return values