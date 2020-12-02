from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import CreateTableSQLs
import logging

logger = logging.getLogger("airflow.task")


class CreateTablesInRedshiftOperator(BaseOperator):
    """ Operator to create tables in redshift by
        running sqls specified in CreateTableSQLs
        dictionary of create_tables.py helper

    Parameters:
        redshift_conn_id: redshift connection_id name
                          stored under airflow
                          Admins> Connections
    Methods:
        execute
    """
    ui_color = '#AED6F1'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):
        super(CreateTablesInRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        for sql_name, create_statement in CreateTableSQLs.items():
            self.log.info(f"Running sql {sql_name}")
            redshift.run(create_statement)
            for output in redshift.conn.notices:
                self.log.info(output)
            self.log.info(f"{sql_name} ran successfully")
            redshift.run("COMMIT;")
