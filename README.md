# data-engineering_projects

Infrastructure
The following tools and technologies are used:

Cloud - Google Cloud Platform
Infrastructure as Code software - Terraform
Containerization - Docker, Docker Compose
Batch Processing - Python
Orchestration - Prefect
Transformation - dbt
Data Lake - Google Cloud Storage
Data Warehouse - BigQuery
Data Visualization - Data Studio


Create a service account in the project.
![image](https://user-images.githubusercontent.com/113747768/227586360-3563a04e-e7d0-4c03-9725-cebfc136e327.png)

Create a json key for the service account.
![image](https://user-images.githubusercontent.com/113747768/227587342-7a2d10ab-09bd-4008-9eee-fcef1bf19e75.png)

# use service-account's auth-token for this session
gcloud auth application-default login

# Build the google cloud reosurce throught the use of IaC tool, Terraform
Create a terraform configuration folder and type the following command 

Terraform init

Create 2 files as below in the folder

touch main.tf
touch variable.tf

Add the configuration to these files which are to create a bucket and a data set in big query in google cloud. Please refer the files in this project for the content of them.

Generates a speculative execution plan, showing what actions Terraform would take to apply the current configuration.
Terraform plan

![image](https://user-images.githubusercontent.com/113747768/227893898-72b89558-b052-4c5e-a0f9-064f1146cca0.png)
![image](https://user-images.githubusercontent.com/113747768/227894189-8740f8c1-698c-4907-85f8-9b3d711e0f58.png)
![image](https://user-images.githubusercontent.com/113747768/227894296-23e82d81-3e3f-44df-b9f9-f1e8c895a445.png)


Creates or updates infrastructure according to Terraform configuration files in the current directory.
Terraform apply

![image](https://user-images.githubusercontent.com/113747768/227894543-b832a8da-4d47-4b9a-9331-dd893d733d58.png)
![image](https://user-images.githubusercontent.com/113747768/227894684-d2abac4f-e341-40aa-89ec-a62e3c58832a.png)
![image](https://user-images.githubusercontent.com/113747768/227894793-ab444896-3156-4b68-9f08-c29167e7716e.png)
![image](https://user-images.githubusercontent.com/113747768/227894900-6dfea994-f2e7-48a6-abb4-e07022caf016.png)



