from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests

# ******Caution : you should use and replace all parameters in this script    ******
# ******          by using your parameter and your project configuration !!! ******

# MYSQL connection (setting on Airflow connection)
MYSQL_CONNECTION = "mysql_default"  # setting connection in airflow (Credential!!!)
# URI for fetch conversion rate
CONVERSION_URL = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate" 

# Airflow auto-generated bucket location : "Datalake"
PROJECT_ID = "audiobook-project-xxxxxx"
BUCKET_NAME = "asia-east2-audible-proj-air-xxxxxxxx-bucket"
REGION = "asia-east2" 
ZONE = "asia-east2-b" 

# File path staging area : Airflow path
datalake = "/home/airflow/gcs/data"
# File path staging area : gs:// path
datalake_gs = f"gs://{BUCKET_NAME}"

# Data file path
data_path = f"{datalake}/input/audible_data.csv"
trans_path = f"{datalake}/input/audible_transaction_data.csv"
mysql_output_path = f"{datalake}/input/audible_data_merged.csv"
conversion_output_path = f"{datalake}/input/conversion_rate.csv"
output_path = f"{datalake}/output/output.csv"
cleaned_output_path = f"{datalake}/output/cleaned_output.csv"

# Dataproc cluster name (REGION and ZONE are same location as Datalake )
CLUSTER_NAME = "audible-proj-spark-cluster"
CLUSTER_STAGING_NAME = "dataproc-staging-asia-east2-xxxxxxxxxxxx-xxxxxxxx" 
# Pyspark python script path
PYSPARK_FILE = "cleaning_Pyspark.py"
PYSPARK_PATH = f"{datalake_gs}/spark_job/{PYSPARK_FILE}"
# Jobs definitions for DataprocSubmitJobOperator
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_PATH}
}

# Destination dataset in BigQuery
destination_project_dataset_table = 'audiobook_proj.audible_data'  

# Function for PythonOperator (Task)
def get_data_from_mysql(transaction_path,data_path,trans_path):

    mysqlserver = MySqlHook(MYSQL_CONNECTION)

    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

    df = audible_transaction.merge(audible_data, left_on="book_id", right_on="Book_ID", how="left")
    df.drop('book_id', axis=1, inplace=True)

    audible_data.to_csv(data_path, index=False)
    audible_transaction.to_csv(trans_path, index=False)
    df.to_csv(transaction_path, index=False)
    print(f"audible_data_merged.csv (transaction data) to {transaction_path}")

def get_conversion_rate(conversion_rate_path):

    r = requests.get(CONVERSION_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)
    df = df.reset_index().rename(columns={"index":"date"})
    df.to_csv(conversion_rate_path, index=False)
    print(f"conversion_rate.csv to {conversion_rate_path}")

def merge_data(transaction_path, conversion_rate_path, output_path):
    transaction = pd.read_csv(transaction_path)
    conversion_rate = pd.read_csv(conversion_rate_path)

    # create date column (for merging transaction and conversion rate by using date column)
    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

    # merge 2 dataframes
    final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")
    # translate string price to be float price
    final_df["Price"] = final_df["Price"].str.replace("$","",regex=False).astype(float)
    # create THBPrice column
    final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
    final_df = final_df.drop("date",axis=1)

    # edit column name, use easily for cleaning 
    final_df.columns = [ "_".join(("".join(c.strip().split("."))).split(" ")) for c in final_df.columns ]
    print(final_df.columns)

    # save output as CSV file
    final_df.to_csv(output_path, index=False)
    print(f"Output to {output_path}")

with DAG(
    "audiobook_project_pipeline",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["audiobook_project"]
) as dag:
    
    t1 = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=get_data_from_mysql,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "data_path":data_path,
            "trans_path":trans_path
        }
    )

    t2 = PythonOperator(
        task_id="get_conversion_rate",
        python_callable=get_conversion_rate,
        op_kwargs={"conversion_rate_path": conversion_output_path},
    )

    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "conversion_rate_path": conversion_output_path,
            "output_path": output_path 
        }
    )

    t4 = GCSToGCSOperator(
        task_id="output.csv_to_spark_staging",
        source_bucket=BUCKET_NAME,
        source_object="data/output/output.csv",
        destination_bucket=CLUSTER_STAGING_NAME,  
        destination_object="data/output.csv"  
    )
    
    t5 = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    t6 = BashOperator(
        task_id='cleaned_output_to_Datalake_and_change_file_name',
        bash_command = f'gsutil cp gs://{CLUSTER_STAGING_NAME}/data/cleaned_output.csv/part*.csv \
            gs://{BUCKET_NAME}/data/output/cleaned_output.csv'
    )

    t7 = BashOperator(
        task_id="load_to_bq",
        bash_command=f"bq load \
            --source_format=CSV --skip_leading_rows=1 \
            {destination_project_dataset_table} \
            gs://{BUCKET_NAME}/data/output/cleaned_output.csv \
            timestamp:timestamp,user_id:string,country:string,Book_ID:integer,Book_Title:string,Book_Subtitle:string,Book_Author:string,Book_Narrator:string,Audio_Runtime:string,Audiobook_Type:string,Categories:string,Rating:string,Total_No_of_Ratings:float,Price:float,conversion_rate:float,THBPrice:float"
    )


    [t1,t2] >> t3 >> t4 >> t5 >> t6 >> t7


