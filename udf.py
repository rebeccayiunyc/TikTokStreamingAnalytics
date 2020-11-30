import json

def string_to_json(string_value):
    #b_to_string = byte_value.decode("utf-8")
    string_to_dict = eval(string_value)
    dict_to_json = json.dumps(string_to_dict)
    return dict_to_json

def anomalyDetector():
    #insert function here
    #abs(value - mean) >= 2.5 * standard deviation
    pass

