# Data-Pipeline-with-Airflow

![test image](https://video.udacity-data.com/topher/2019/January/5c48a861_example-dag/example-dag.png)

### Goal

Create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Run tests after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in the Redshift data warehouse. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Dataset
* Log data: s3://udacity-dend/log_data
* Song data: s3://udacity-dend/song_data

### Prerequisites
* AWS with Cluster running on us-west-2
* Airflow running locally

### AWS Cluster Setup
* Located in the us-west-2 region
* Publicly accessible: *open cluster --> actions --> modify to make publicly accessible*
* The associated security group must allow the inbound traffic on the default port 5439: *Cluster Parameters --> Network/Security settings --> VPC security group URL --> add an inbound rule for ALL TRAFIC with 0.0.0.0/0*

### Airflow Setup with Docker via CLI
1. Install and launch Docker. Make sure the engine's running.
2. Cmd: *docker pull puckel/docker-airflow*
3. Connect your DAG directory to Docker and open webserver on local host, example: *docker run -d -p 8080:8080 -v /Users/tatianatikhonova/airflow/dags/:/usr/local/airflow/dags  puckel/docker-airflow webserver*
**Note**: [this article](https://towardsdatascience.com/getting-started-with-airflow-using-docker-cd8b44dbff98) provides additional detail on how to get started with Airflow using Docker.

### Structure

1. **Sql_statements**: contains SQL queries for the ETL pipeline.
2. **Main_dag**: defines ETL tasks and their sequence.
3. **stage_redshift.py**: instantiates StageToRedshiftOperator to copy JSON data from S3 to staging tables in Redshift
4. **load_dimension.py**: instantiates LoadDimensionOperator to load dimension tables.
5. **load_fact.py**: instantiates LoadFactOperator to load the fact table.
6. **data_quality.py**: instantiates DataQualityOperator to run data quality checks.


### Steps
1. Create a Redshift cluster following the below requirements.
2. Create an Airflow connection via Postgres to your AWS cluster; call it 'redshift'.
3. Create an Airflow connection with your AWS User credentials; call it 'aws_credentials'.
4. Launch the Airflow web server to run the DAG.
