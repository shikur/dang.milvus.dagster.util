from pymilvus import Collection, connections, utility
import yaml

def connect_to_milvus(host='localhost', port='19530'):
    try:
        connections.connect(alias="default", host="localhost", port=19530)
        print("Connection to Milvus server succeeded.")
        return connections
    except Exception as e:
       print(f"Connection failed: {e}")


def create_collection_if_not_exists(collection_name, fields):
    if not utility.has_collection(collection_name):
        collection = Collection(name=collection_name, schema=fields)
        return collection
    return Collection(name=collection_name)

def insert_data_into_milvus(collection_name, data):
    collection = Collection(name=collection_name)
    result = collection.insert(data)
    return result.primary_keys


# Function to load Milvus configuration
def load_milvus_config(config_path):
    with open(config_path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)