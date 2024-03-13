import resource

from pymilvus import Collection, CollectionSchema, DataType, FieldSchema, connections, utility
from dagster import schedule
from dagster_docker import docker_executor
from dagster import Field, String, op, job, graph,  repository , resource, Int
import pandas as pd
from transformers import AutoTokenizer, AutoModel
import torch
from milvus_utils import connect_to_milvus, create_collection_if_not_exists,  insert_data_into_milvus



@resource({
    "read_dir": Field(String),
    "write_dir": Field(String),
    "filename": Field(String),
    "host": Field(String),
    "port": Field(Int),
    "collection_name": Field(String),
    "milvus_config_path": Field(String),
    "writefilename": Field(String),
    "readfilename": Field(String),
    "sourcetype":Field(String),
    "tokenizer": context.resource_config["tokenizer"],
    "modeltype":context.resource_config["sourcetype"], 

})
def conversations_file_dirs_resource(context):
    return {
        "write_dir": context.resource_config["write_dir"],
        "read_dir": context.resource_config["read_dir"],
        "readfilename": context.resource_config["readfilename"],
        "writefilename": context.resource_config["writefilename"],
        "host": context.resource_config["host"] ,
        "port": context.resource_config["port"] ,                
        "collection_name": context.resource_config["collection_name"],
        "milvus_config_path":  context.resource_config["milvus_config_path"],
        "sourcetype":context.resource_config["sourcetype"],
        "tokenizer": context.resource_config["tokenizer"],
        "modeltype":context.resource_config["modeltype"], 
    }
tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

def vectorize_text(text, tokenizer, model):
    inputs = tokenizer(text, padding=True, truncation=True, max_length=512, return_tensors="pt")
    with torch.no_grad():
        outputs = model(**inputs)
    embeddings = outputs.last_hidden_state.mean(1).numpy()  # Average pooling
    return embeddings.flatten()

@op(required_resource_keys={"conversations_file_dirs_resource"})
def conversation_read_csv(context) -> pd.DataFrame:
    dirsource = f"{context.resources.conversations_file_dirs_resource['read_dir']}";
    context.log.info(dirsource)
    filenamesource = f"{context.resources.conversations_file_dirs_resource['readfilename']}"
    fullpath = dirsource + filenamesource
    context.log.info(fullpath)
    df = pd.read_csv(fullpath)
    return df

@op
def conversation_transform_data(context, df: pd.DataFrame) -> pd.DataFrame:
    context.log.info("Transforming data")
    context.log.info(df.head(10))
    # return df.head(100)
    return df.head(100)

# @op(config_schema={"milvus_config_path": Field(String), "csv_file_path": Field(String), "collection_name": Field(String)})
@op(required_resource_keys={"conversations_file_dirs_resource"})
def insert_into_milvus_conversation(context, df: pd.DataFrame) -> pd.DataFrame:
    # Vectorize "Answers"
    df['Answer_Vector'] = df['Answers'].apply(lambda x: vectorize_text(x, tokenizer, model))

    # Connect to Milvus
    # connecttmilvus =connect_to_milvus(context.resources.conversations_file_dirs_resource['host'], context.resources.conversations_file_dirs_resource['port'])
    milvus_host = "milvus-standalone"
    milvus_port = "19530"
  
    connections.connect(host=milvus_host, port=milvus_port, timeout=120)
    collection_name = context.resources.conversations_file_dirs_resource['collection_name']
    index_params = {
    "index_type": "IVF_FLAT",  # Example index type
    "params": {"nlist": 1024},  # Example parameter, adjust based on your data and requirements
    "metric_type": "L2"  # Example metric, choose "L2" or "IP" based on your use case
}

    if not utility.has_collection(collection_name):
        id_field = FieldSchema(name="Question_ID", dtype=DataType.INT64, is_primary=True, auto_id=False)
        Answer_field = FieldSchema(name="Answers", dtype=DataType.VARCHAR, max_length=2000, description="foreign id of vector in a different database")
        vector_field = FieldSchema(name="Answer_Vector", dtype=DataType.FLOAT_VECTOR, dim=384)
        schema = CollectionSchema(fields=[id_field, Answer_field, vector_field], description="Question and Answers")
        create_collection_if_not_exists(collection_name, schema)

    collection = Collection(name=collection_name)
    collection.create_index(field_name="Answer_Vector", index_params=index_params)

    # Prepare data for insertion
    question_ids = df['Question_ID'].tolist()
    df['Answers']= df['Answers'].str.slice(stop=550)
    Answer= df['Answers'].tolist()
    vectors = df['Answer_Vector'].tolist()
    
    # Insert data into Milvus
    datauploaded = collection.insert([question_ids, Answer, vectors])
    context.log.info(f"Inserted data into Milvus with IDs: {datauploaded.primary_keys}")

    # utility.flush([collection_name])
    collection.load()
    
    return df
  

@op(required_resource_keys={"conversations_file_dirs_resource"})
def conversation_save_csv(context, df: pd.DataFrame):
    dirsource = f"{context.resources.file_dirs['write_file_dir']}";
    filenamesource = f"{context.resources.file_dirs['writefilename']}"
    fullpath = dirsource + filenamesource
    context.log.info("Transforming data")
    context.log.info(df.head(10))
    context.log.info(fullpath)
    df.to_csv(fullpath, index=False)
    


@job(resource_defs={"conversations_file_dirs_resource": conversations_file_dirs_resource})
def process_csv_pipeline():
    conversation_save_csv( insert_into_milvus_conversation(conversation_transform_data(conversation_read_csv())))
  

