import os
from json import load

# Function for recursively flattening json into underscore separated keys 
def flatten_json(y):
    out = {}

    def flatten(x, name=''):

        # If the nested key-value pair is of dict type
        if type(x) is dict: 
            for a in x:
                flatten(x[a], name + a + '_')

        # If the nested key-value pair is of list type 
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else: 
            out[name[:-1]] = x
    
    flatten(y)
    return out


# Merge all objects given into a single object
def merge(obj, json_blob):
    if json_blob:
        for item in json_blob.items():
            setattr(obj, item[0], item[1])

    return obj


# Load the gcp.config file and merge the contents into the context object
def load_config(context):
    config = None
    
    with open(f'{os.getcwd()}/gcp.config', 'r') as config_file:
        config = flatten_json(load(config_file))

    context = merge(context, config)