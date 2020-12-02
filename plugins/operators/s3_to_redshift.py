from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from datetime import datetime

logger = logging.getLogger("airflow.task")


class S3ToRedshiftOperator(BaseOperator):
    """ Operator to copy data from s3 to Redshift

    Parameters:
        redshift_conn_id: redshift credential name
                          stored under airflow
                          Admins> Connections
        aws_credentials_id (Str): aws credential name
                          stored under airflow
                          Admins> Connections
         table: table name to copy data to
         s3_bucket: source s3 bucket
         s3_key: source s3 filename
         delimiter: record delimiter
         ignore_headers: 0 for No, 1 for yes
         file_format: format of file. E.g. json, psv, csv etc
         file_format_path: location of json data path
                           (applicable only when fie_format is json)

    Methods:
        execute
    """
    ui_color = '#D7BDE2'
    template_fields = ("s3_key",)
    copy_sql_all = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}' GZIP
        EMPTYASNULL
    """

    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        EMPTYASNULL
        DATEFORMAT 'auto'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter="|",
                 ignore_headers=1,
                 file_format="",
                 file_format_path="auto",
                 *args, **kwargs):
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format
        self.file_format_path = file_format_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        logger.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))

        logger.info("Copying data from S3 to Redshift")
        if context["prev_ds"] is not None:
            rendered_key = self.s3_key.\
                format(file_date=datetime.strptime(context["prev_ds"],
                                                   "%Y-%m-%d").
                       strftime("%m-%d-%Y"))
        else:
            rendered_key = self.s3_key
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.file_format == "json":
            formatted_sql = S3ToRedshiftOperator.\
                copy_sql_json.format(self.table,
                                     s3_path,
                                     credentials.access_key,
                                     credentials.secret_key,
                                     self.file_format_path)
        else:
            formatted_sql = S3ToRedshiftOperator.copy_sql_all.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
        print(formatted_sql)
        redshift.run(formatted_sql)
        for output in redshift.conn.notices:
            logger.info(output)
        redshift.run('COMMIT;')
