import gzip
import jsonlines
import json
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient

sas_token = "sp=rle&st=2023-07-25T18:12:36Z&se=2023-08-13T02:12:36Z&sv=2022-11-02&sr=c&sig=l2TCTwPWN8LSM922lR%2Fw78mZWQK2ErEOQDUaCJosIaw%3D"
account_url = "https://safactoreddatathon.dfs.core.windows.net/"

service_client = DataLakeServiceClient(account_url,
                                       credential=sas_token)
file_system_name = "source-files"

file_path_review = "amazon_reviews/partition_10/part-00001-tid-698064602200227711-29b88890-b701-4ddb-82cf-535e4b44c9cf-89302-1-c000.json.gz"

file_client_review = service_client.get_file_client(file_system=file_system_name,
                                                    file_path=file_path_review)

with gzip.GzipFile(fileobj=file_client_review.download_file()) as gz_file:
    # Utilizar jsonlines para leer el archivo JSONL
    field_names = set()
    with jsonlines.Reader(gz_file) as reader:
        for item in reader:
            field_names.update(item.keys())

    # Explorar los nombres de los campos del JSON
print("Campos presentes en el JSONL de reviews:")
print(field_names)

file_path_metadata = "amazon_metadata/partition_1000/part-00000-tid-7816304841786422595-fccfa6ea-3818-4a2c-bcc0-4fdddcb1c50b-1999-1-c000.json.gz"

# Get the DataLakeFileClient for the specified file path
file_client_metadata = service_client.get_file_client(file_system=file_system_name,
                                                      file_path=file_path_metadata)

with gzip.GzipFile(fileobj=file_client_metadata.download_file()) as gz_file:
    field_names = set()
    category_values = []
    with jsonlines.Reader(gz_file) as reader:
        for item in reader:
            field_names.update(item.keys())
            if "category" in item:
                category_values.append(item["category"])

    # Explorar los nombres de los campos del JSON
    print("Campos presentes en el JSONL de metadata:")
    print(field_names)

    # Los primeros elementos en 'category', presentes en el JSONL de metadata
    cats = set()
    for value in category_values:
        if isinstance(value, list) and len(value) > 0:
            cats.add(value[0])
    print("Los primeros elementos en 'category', presentes en el JSONL de metadata:")
    print(cats)