# Databricks notebook source
# Replace the <SAS_TOKEN> with the provided SAS token
sas_token = "sp=rle&st=2023-07-18T20:32:29Z&se=2023-08-13T04:32:29Z&spr=https&sv=2022-11-02&sr=c&sig=yO52pA9Gzo1c3MQX5b7mOUbX3l%2FwBe3lg%2Bi2Gkxm40U%3D"

# Replace 'source-files' with the name of your container
container_name = "source-files"

# Mount the container to Databricks
mount_point = "/mnt/source_files"  # You can specify any desired mount point
dbutils.fs.mount(
    source=f"wasbs://{container_name}@safactoreddatathon.blob.core.windows.net",
    mount_point=mount_point,
    extra_configs={f"fs.azure.sas.{container_name}.safactoreddatathon.blob.core.windows.net": sas_token}
)


# COMMAND ----------

# Define the folder paths for both 'amazon_metadata' and 'amazon_reviews'
metadata_folder_path = "/mnt/source_files/amazon_metadata/"
reviews_folder_path = "/mnt/source_files/amazon_reviews/"

def read_files_recursively(folder_path):
    # List all files and subdirectories in the folder
    items = dbutils.fs.ls(folder_path)
    
    for item in items:
        if item.isDir():  # If it's a subdirectory, recursively read files from it
            read_files_recursively(item.path)
        else:  # If it's a file, read its content
            file_path = item.path
            file_content = dbutils.fs.head(file_path)
            print(f"Content of the file: {file_path}")
            print(file_content)

# Read files from 'amazon_metadata' folder and its subfolders
print("Reading files from 'amazon_metadata' folder:")
read_files_recursively(metadata_folder_path)

# Read files from 'amazon_reviews' folder and its subfolders
print("Reading files from 'amazon_reviews' folder:")
read_files_recursively(reviews_folder_path)


# COMMAND ----------

# Define the folder paths for both 'amazon_metadata' and 'amazon_reviews'
metadata_folder_path = "/mnt/source_files/amazon_metadata/"
reviews_folder_path = "/mnt/source_files/amazon_reviews/"

def read_json_gz_files_recursively_spark(folder_path):
    # List all files and subdirectories in the folder
    items = dbutils.fs.ls(folder_path)

    # Variable to store the content of the chosen .json.gz file
    chosen_file_path = None

    for item in items:
        if item.isDir():  # If it's a subdirectory, recursively find .json.gz files in it
            chosen_file_path = read_json_gz_files_recursively_spark(item.path)
            if chosen_file_path:
                break
        else:  # If it's a file, check if it ends with .json.gz
            if item.name.endswith(".json.gz"):
                chosen_file_path = item.path
                break

    return chosen_file_path

# Read the content of the chosen .json.gz file in 'amazon_metadata' folder
print("Reading .json.gz file in 'amazon_metadata' folder:")
metadata_json_gz_file_path = read_json_gz_files_recursively_spark(metadata_folder_path)
if metadata_json_gz_file_path:
    # Read the JSON file with Spark
    metadata_df = spark.read.json(metadata_json_gz_file_path)
    metadata_df.show()  # Displaying the DataFrame in Databricks
else:
    print("No .json.gz file found in 'amazon_metadata' folder.")

# Read the content of the chosen .json.gz file in 'amazon_reviews' folder
print("Reading .json.gz file in 'amazon_reviews' folder:")
reviews_json_gz_file_path = read_json_gz_files_recursively_spark(reviews_folder_path)
if reviews_json_gz_file_path:
    # Read the JSON file with Spark
    reviews_df = spark.read.json(reviews_json_gz_file_path)
    reviews_df.show()  # Displaying the DataFrame in Databricks
else:
    print("No .json.gz file found in 'amazon_reviews' folder.")


# COMMAND ----------

reviews_df.describe().show()

# COMMAND ----------

metadata_df.dtypes

# COMMAND ----------


