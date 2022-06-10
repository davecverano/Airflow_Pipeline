import os
import datetime
import logging
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import bigquery_to_gcs

sql="""SELECT s.year, s.artist, s.popularity AS most_popular FROM `polar-standard-352713.example1.spotify_songs` AS s INNER JOIN (SELECT year, MAX(popularity) as most_popular FROM `polar-standard-352713.example1.spotify_songs` WHERE NOT CONTAINS_SUBSTR(genre, "pop") GROUP BY year) AS t ON s.year = t.year AND s.popularity = t.most_popular ORDER BY year DESC""" 

dataset_table = '{}:demo_dataset.demo_table'.format(os.environ['project_name'])
out_file = 'gs://{}/dags/data/out_data.csv'.format(os.environ['bucket_name'])

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2018, 1, 1),
}

with models.DAG(
        'composer_test_dag',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

	create_bucket = bash_operator.BashOperator(
		task_id="create_bucket",
		bash_command="bash {{params.env_dags_path}}/scripts/create_bucket.sh {{params.project_name}} {{params.bucket_name}}",
		params = {'project_name' : os.environ['project_name'], 'bucket_name' : os.environ['bucket_name'], 'env_dags_path' : os.environ['env_dags_path']},
	)

	url_to_gcs = bash_operator.BashOperator(
		task_id="url_to_gcs",
		bash_command="bash {{params.env_dags_path}}/scripts/url_to_gcs.sh {{params.data_url}} {{params.bucket_name}}",
		params = {'bucket_name' : os.environ['bucket_name'], 'data_url' : os.environ['data_url'], 'env_dags_path' : os.environ['env_dags_path']},
	)

	load_query_table = bigquery_operator.BigQueryOperator(
		task_id ='load_query_table',
		dag=dag,
		bql=sql,
		destination_dataset_table=dataset_table,
		write_disposition='WRITE_TRUNCATE',
		create_disposition='CREATE_IF_NEEDED',
		allow_large_results='true',
		use_legacy_sql=False,
	)

	transfer_table = bigquery_to_gcs.BigQueryToCloudStorageOperator(
		task_id ='transfer_table',
		source_project_dataset_table = dataset_table,
		destination_cloud_storage_uris = out_file,
		export_format='CSV',
	)
	
	
	

	create_bucket >> url_to_gcs >> load_query_table >> transfer_table 
	
