# Airflow Pipeline

### Finished Project Demo

![Dags_example](https://user-images.githubusercontent.com/49330823/180494629-e4ac049b-dd49-416e-9a6c-e7200607b98c.JPG)
![Airflow_tree_view](https://user-images.githubusercontent.com/49330823/180496356-58840724-9034-4b4f-b233-3ce284b5e393.JPG)

Note: As you can tell, I ran into alot of errors while making it 😆

### Intro

The goal of this task is to create a pipeline in Apache Airflow that:
1. Creates a bucket in Google Cloud Storage for a particular project
2. Loads information from a url that downloads a file such as a .csv file from a website like kaggle
3. Stores the file into the bucket in Google Cloud Storage
4. Loads the file into BigQuery and performs desired queries onto the dataset
5. Transfers queries into local computer

### Tools

Apache Airflow  
Google Cloud  
Bash  
Python  
