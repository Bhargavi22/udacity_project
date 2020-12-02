"""A DAG that runs daily to -
    1. Pull below items into s3-
       a. covid data file from JHU's github repo
       b. location lookup data from JHU's github repo
       c. google mobility data provided by google
    2. Load data from s3 to Redshift
    3. Do data quality checks
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
    'start_date': datetime(2020, 11, 26),
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 3,
    'retry_delay': 30,
    'email_on_retry': False,
    'catchup': False
}

# DAG definition
dag = DAG(dag_id="load_covid_and_mobility_data_daily",
          default_args=default_args,
          description='Extract, load and, transform covid \
and google mobility data into redshift',
          schedule_interval='30 15 * * *',
          catchup=True,
          max_active_runs=1
          )

# task definition to create tables in redshift
create_tables = CreateTablesInRedshiftOperator(
    task_id="create_redshift_tables",
    dag=dag,
    redshift_conn_id="redshift"
)

# task definition to pull location lookup file from JHU covid github repo to s3
pull_location_lookup_file_into_s3 = PullFileFromRemoteIntoS3(
    task_id="pull_location_lookup_file_into_s3",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    source_url=f"https://github.com/CSSEGISandData/COVID-19/\
blob/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv?raw=true",
    s3_bucket="btrivedi-udacityproject",
    s3_key="location_lookup",
    provide_context=True,
)

# task definition to pull covid data file from JHU covid github repo to s3
pull_covid_file_into_s3 = PullFileFromRemoteIntoS3(
    task_id="pull_covid_data_file_into_s3",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    source_url=f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/\
master/csse_covid_19_data/csse_covid_19_daily_reports/\
{{file_date}}.csv",
    s3_bucket="btrivedi-udacityproject",
    s3_key="covid",
    provide_context=True,
)

# task definition to pull google mobility data file from google's site to s3
pull_google_mobility_file_into_s3 = PullFileFromRemoteIntoS3(
    task_id="pull_google_mobility_data_file_into_s3",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    source_url=f"https://www.gstatic.com/covid19/\
mobility/Global_Mobility_Report.csv",
    s3_bucket="btrivedi-udacityproject",
    s3_key="google_mobility",
    provide_context=True,
)

# task definition to copy location lookup data into staging table
copy_location_lookup_data_task = S3ToRedshiftOperator(
    task_id="copy_location_lookup_data_from_s3_to_redshift",
    dag=dag,
    table="location_lookup_staging",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="btrivedi-udacityproject",
    s3_key=f"location_lookup/location_lookup_{{file_date}}.psv.gz",
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
    s3_key=f"covid/covid_{{file_date}}.json",
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
    s3_key=f"google_mobility/google_mobility_{{file_date}}.psv.gz",
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
for sql_statement_title, \
        sql_statement in sqlStatements['load_google_mobility_data'].items():
    task_name = f"{sql_statement_title}_task"
    task_name = PostgresOperator(task_id=f"{sql_statement_title}",
                                 dag=dag,
                                 postgres_conn_id="redshift",
                                 sql=sql_statement,
                                 provide_context=True,)
    load_mobility_data_task_list.append(task_name)

load_mobility_data_task_list

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

# create tables if not exist and pull files from remote to s3
create_tables >> pull_location_lookup_file_into_s3
create_tables >> pull_google_mobility_file_into_s3
create_tables >> pull_covid_file_into_s3

# truncate and load staging tables
pull_location_lookup_file_into_s3 >> copy_location_lookup_data_task
pull_google_mobility_file_into_s3 >> copy_mobility_data_task
pull_covid_file_into_s3 >> copy_covid_data_task

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
