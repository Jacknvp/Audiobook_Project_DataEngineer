# Data Engineer project : Audiobook shop Project
This project is the project in course **"Road to Data Engineer 2.0 Bootcamp"** by **Data TH**.

## ***Introduction***
----------------
 The objective of this project is to manipulate, collect, and prepare data for Data Analyst as **Data Engineer** in the Audiobook shop company. In addition, use the completed data to create a dashboard following the business requirement that needs to know the best selling product for selecting the product and preparing appropriate promotions to increase audiobook sales. The initial data we used in this project is in the company's database (**MySQL**) that collect from the company website.

## ***Folders and Files explaination***
---------------------
- **dags** : store DAG script
    - audiobook_dag.py : DAG Python script for data pipeline by using Apache Airflow
- **spark_job**
    - cleaning_Pyspark.py : Python script(PySpark) used to summit in Dataproc for cleaning data
- **data** : store data in this project
    - **input**
        - audible_data.csv : Audiobook data
        - conversion_rate.csv : Conversion rate data for convert US Dollar to Thai Baht
        - audible_transaction_data.csv : Transaction sales data
        - audible_data_merged.csv : Data from merge between audible_data.csv and audible_transaction_data.csv 
    - **output**
        - output.csv : Data from merge between audible_data_merged.csv and conversion_rate.csv along with data transformation
        - cleaned_output.csv : Data from cleaning process by using Spark

## ***Methodology***
--------------
0. Plan to manipulate the project using colab notebook
    - Explore data in database(MySQL in this project), in database has 2 tables : audible_data and audible_transaction.
    - Then fetch conversion rate data from RESR API 
    - Mock the Transformation process using PANDAS and Data Cleaning using PySpark (Python API of Apache Spark) in colab notebook
1. Create **Cloud Composer Cluster** for running **Apache Airflow** and install python package in Airflow (pymysql, requests, pandas)
2. **[ Credential!!! ]** Set MySQL connection on Apache Aiflow web server (Admin-->Connection-->mysql_default)
3. Create **Cloud Dataproc Cluster** for running **Apache Spark** (in this project we use PySpark python script for cleaning data)
4. Manipulate and Create folders as below:
    - Create folder **input** and **output** in folder **data** that is in the auto-generated Cloud Storage when we create Cloud Composer Cluster
    -  Create folder **spark_job** for Python script(PySpark) used to summit in Dataproc for cleaning data
5. Upload pipeline python script : *audiobook_dag.py* to folder **dags**(connected to dag folder in Airflow), and PySpark python script : *cleaning_Pyspark.py* to folder **spark_job** by using Cloud shell
6. Airflow generates Data Pipeline Orchestration and triggers following a schedule that config in *audiobook_dag.py*. DAG of this pipeline is shown below:

    ![DAG_pic](C:\Coding\audiobook_project\pic_project\DAG.png)  

7. Create a view table from a table in Dataset in **BigQuery**. Then use this view table to make Sales Dashboard in **Looker Studio**
8. See the Sales Dashboard as the attached link below:  
    [Sales Dashboard](https://lookerstudio.google.com/s/oV4qnybsMjY)

## ***Technologies Used***
----------------------
- MySQL
- Google Cloud Platform
    - Cloud Storage
    - Cloud Composer
    - Cloud Dataproc
    - BigQuery
- Looker Studio
- Apache Airflow
- Apache Spark
## Languages
-------------
- Python
- SQL