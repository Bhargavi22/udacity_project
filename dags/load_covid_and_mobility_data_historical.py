"""A DAG that will run once to -
    1. Load covid and mobility data from s3 to Redshift
    2. Do data quality checks
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import CreateTablesInRedshiftOperator,\
    PullFileFromRemoteIntoS3, S3ToRedshiftOperator,\
    DataQualityCheckOperator
from airflow.operators.postgres_operator import PostgresOperator
from helpers import sqlStatements
import logging

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'btrivedi',
    'start_date': datetime(2020, 7, 1),
    'end_date': datetime(2020, 7, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 30,
    'email_on_retry': False,
    'catchup': False
}


# DAG definition
dag = DAG(dag_id="load_covid_and_mobility_data_historical",
          default_args=default_args,
          description='Extract, load and, transform covid \
          and google mobility data into redshift',
          schedule_interval='@once',
          catchup=True
          )

# task definition to create tables in redshift
create_tables = CreateTablesInRedshiftOperator(
    task_id="create_redshift_tables",
    dag=dag,
    redshift_conn_id="redshift"
)

# task definition to copy location lookup data into staging table
copy_location_lookup_data_task = S3ToRedshiftOperator(
    task_id="copy_location_lookup_data_from_s3_to_redshift",
    dag=dag,
    table="location_lookup_staging",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="btrivedi-udacityproject",
    s3_key=f"location_lookup/location_lookup_11-24-2020.psv.gz",
    provide_context=True,
)

# task definition to copy covid data into staging table
copy_covid_data_task = S3ToRedshiftOperator(
    task_id="copy_covid_data_from_s3_to_redshift",
    dag=dag,
    table="covid_data_staging",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="btrivedi-udacityproject",
    s3_key=f"covid/covid",
    file_format="json",
    file_format_path="s3://btrivedi-udacityproject/\
covid_data_json_path/covid_data_path.json",
    provide_context=True,
)

# task definition to copy google mobility data into staging table
copy_mobility_data_task = S3ToRedshiftOperator(
    task_id="copy_google_mobility_data_from_s3_to_redshift",
    dag=dag,
    table="google_mobility_data_staging",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="btrivedi-udacityproject",
    s3_key=f"google_mobility/Global_Mobility_Report_Full_20201124.psv.gz",
    provide_context=True,
)

# task definition to check staging table data
staging_data_checks = DataQualityCheckOperator(
    task_id='staging_table_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['google_mobility_data_staging', 'covid_data_staging',
            'location_lookup_staging'],
    test_type=['has_rows']
)

# task definition to check final table data
final_table_checks = DataQualityCheckOperator(
    task_id='final_table_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['google_mobility_data_us', 'google_mobility_data_non_us',
            'covid_data_us', 'covid_data_non_us',
            'location_lookup'],
    primary_key_column={'google_mobility_data_us':
                        'location_identifier||file_date',
                        'google_mobility_data_non_us':
                        'location_identifier||file_date',
                        'covid_data_us':
                        'location_identifier||file_date',
                        'covid_data_non_us': 'location_identifier||file_date',
                        'location_lookup': 'location_identifier'},
    # test_type=['check_nulls', 'has_rows']
    test_type=['check_nulls', 'has_rows', 'check_duplicates']
)

# task definition to copy google mobility data from staging to final table
load_mobility_data_task_list = []
for sql_statement_title,\
        sql_statement in sqlStatements['load_google_mobility_data'].items():
    task_name = f"{sql_statement_title}_task"
    task_name = PostgresOperator(task_id=f"{sql_statement_title}",
                                 dag=dag,
                                 postgres_conn_id="redshift",
                                 sql=sql_statement,
                                 provide_context=True,)
    load_mobility_data_task_list.append(task_name)


# task definition to insert lookup data from staging to final table
load_lookup_data_task_list = []
for sql_statement_title, \
        sql_statement in sqlStatements['load_lookup_data'].items():
    task_name = f"{sql_statement_title}_task"
    task_name = PostgresOperator(task_id=f"{sql_statement_title}",
                                 dag=dag,
                                 postgres_conn_id="redshift",
                                 sql=sql_statement,
                                 provide_context=True,)
    load_lookup_data_task_list.append(task_name)


# task definition to insert covid data from staging to final table
load_covid_data_task_list = []
for sql_statement_title, \
        sql_statement in sqlStatements['load_covid_data'].items():
    task_name = f"{sql_statement_title}_task"
    task_name = PostgresOperator(task_id=f"{sql_statement_title}",
                                 dag=dag,
                                 ui_color='#D7BDE2',
                                 postgres_conn_id="redshift",
                                 sql=sql_statement,
                                 provide_context=True,)
    load_covid_data_task_list.append(task_name)

# task to drop staging and temp tables
database_cleanup_task = PostgresOperator(
                                         task_id="database_cleanup_sqls",
                                         dag=dag,
                                         postgres_conn_id="redshift",
                                         sql=sqlStatements[
                                             'database_cleanup_sqls'],
                                         provide_context=True,)

# defining task hierarchy

# create tables if not exist and load staging tables
create_tables >> copy_mobility_data_task
create_tables >> copy_covid_data_task
create_tables >> copy_location_lookup_data_task

# check if staging tables have data
copy_location_lookup_data_task >> staging_data_checks
copy_mobility_data_task >> staging_data_checks
copy_covid_data_task >> staging_data_checks


# load final lookup tables
staging_data_checks >> load_lookup_data_task_list[0]
load_lookup_data_task_list[0] >> load_lookup_data_task_list[1]

# load final covid and google mobility data tables
load_lookup_data_task_list[1] >> load_covid_data_task_list[0]
load_covid_data_task_list[0] >> load_covid_data_task_list[1:]
load_lookup_data_task_list[1] >> load_mobility_data_task_list

# check final table load
load_mobility_data_task_list >> final_table_checks
load_covid_data_task_list[1:] >> final_table_checks

# cleanup tables
final_table_checks >> database_cleanup_task
