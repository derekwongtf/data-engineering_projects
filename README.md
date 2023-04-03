# data-engineering_projects

Infrastructure
The following tools and technologies are used:

Cloud - Google Cloud Platform
Infrastructure as Code software - Terraform
Containerization - Docker, Docker Compose
Batch Processing - Python, Spark
Orchestration - Prefect
Transformation - dbt
Data Lake - Google Cloud Storage
Data Warehouse - BigQuery
Data Visualization - Data Studio

# Airbnb in Hong Kong

Create a service account in the project.
![image](https://user-images.githubusercontent.com/113747768/227586360-3563a04e-e7d0-4c03-9725-cebfc136e327.png)

Create a json key for the service account.
![image](https://user-images.githubusercontent.com/113747768/227587342-7a2d10ab-09bd-4008-9eee-fcef1bf19e75.png)

Use service-account's auth-token
```
gcloud auth application-default login
```
Build the google cloud reosurce throught the use of IaC tool, Terraform
Create a terraform configuration folder and type the following command 
```
Terraform init
```
Create 2 files as below and put them into a folder. These 2 files are the configuration files which are to create a bucket and a data set in big query in google cloud. For the content of these 2 files, please refer them in this project.
```
touch main.tf

touch variable.tf
```

Generates a speculative execution plan, showing what actions Terraform would take to apply the current configuration.
```
Terraform plan
```
![image](https://user-images.githubusercontent.com/113747768/227893898-72b89558-b052-4c5e-a0f9-064f1146cca0.png)
![image](https://user-images.githubusercontent.com/113747768/227894189-8740f8c1-698c-4907-85f8-9b3d711e0f58.png)
![image](https://user-images.githubusercontent.com/113747768/227894296-23e82d81-3e3f-44df-b9f9-f1e8c895a445.png)


Creates or updates infrastructure according to Terraform configuration files in the current directory.
```
Terraform apply
```
![image](https://user-images.githubusercontent.com/113747768/227894543-b832a8da-4d47-4b9a-9331-dd893d733d58.png)
![image](https://user-images.githubusercontent.com/113747768/227894684-d2abac4f-e341-40aa-89ec-a62e3c58832a.png)
![image](https://user-images.githubusercontent.com/113747768/227894793-ab444896-3156-4b68-9f08-c29167e7716e.png)
![image](https://user-images.githubusercontent.com/113747768/227894900-6dfea994-f2e7-48a6-abb4-e07022caf016.png)

Set up the prefect cloud account and get the API key.
![image](https://user-images.githubusercontent.com/113747768/228734231-64a4868a-333e-471c-93ad-1fd5fb0475ca.png)

Check if prefect in the local machine is configured to use prefect cloud
```
prefect profile inspect
```
![image](https://user-images.githubusercontent.com/113747768/228734855-c1e421f9-8532-41d3-9f4d-a4cac33940a7.png)

Inside Airbnb is the source data for this project. A bash script (download_data.sh) is created for downloading the raw dataset from the website to local machine. 
The script is then wrapped with python code with prefect decorator. When the script is run, the output in local machine is as below.
```
python python_download_data.py
```
![image](https://user-images.githubusercontent.com/113747768/228735560-bfe82fbd-ac7e-42f3-90e6-325898d6c310.png)
![image](https://user-images.githubusercontent.com/113747768/228735676-c481bdb2-396d-44f2-b1bf-0ae6c78541da.png)

The process will be recorded on prefect cloud.
<img width="310" alt="image" src="https://user-images.githubusercontent.com/113747768/228735236-65ebf77c-25ef-45ad-92b8-6b4e9dc90226.png">

The structure of the raw data is organised as below. 
![image](https://user-images.githubusercontent.com/113747768/228484051-b5821811-9ecd-4113-b70e-f831d2f01703.png)

Convert the raw compressed csv files to parquet files using pyspark
<img width="805" alt="image" src="https://user-images.githubusercontent.com/113747768/229048780-1ccae0af-6e01-4a79-9319-3ed3828ae8db.png">
<img width="812" alt="image" src="https://user-images.githubusercontent.com/113747768/229048896-5fa76d6f-3cb3-47e9-971c-fffaaf7ec036.png">



