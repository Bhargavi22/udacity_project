from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

logger = logging.getLogger("airflow.task")


class DataQualityCheckOperator(BaseOperator):
    """ Operator to check quality of data
        loaded into redshift tables
    Parameters:
        redshift_conn_id: redshift connection_id name
                          stored under airflow
                          Admins> Connections
        tables: table names to run the data check
                queries on
        primary_key_column: dictionary where key is
                            redshift table name and,
                            values are double pipe
                            (||) separated primary
                            key columns
        test_type: type of test to be performed on
                   the specified tables
    Methods:
        has_rows: method to check if a given table
                  is not empty
        check_nulls: method to check if the primary key
                     columns are null in a table
        check_duplicates: method to check if
                          primary key columns have
                          duplicates
        execute
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 primary_key_column={},
                 test_type=[],
                 *args, **kwargs):

        super(DataQualityCheckOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.primary_key_column = primary_key_column
        self.test_type = test_type

    def has_rows(self, table_name, primary_key_column, redshift_hook):
        """Check if a table has rows

        Args:
                table_name: name of table
                primary_key_column: primary key columns if any
                redshift_hook: redshift connection info

        Raises:
            ValueError: when table is empty
        """
        logger.info(f"Checking if table {table_name} is empty")

        records = redshift_hook.get_records(
            f"SELECT COUNT(*) FROM {table_name}")

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. \
{table_name} table returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. \
{table_name} table contained 0 rows")
        logger.info(f"Table{table_name} has {records[0][0]} records")

    def check_nulls(self, table_name, primary_key_column, redshift_hook):
        """Check if a table's primary key columns have null values

        Args:
                table_name: name of table
                primary_key_column: primary key column(s)
                redshift_hook: redshift connection info

        Raises:
            ValueError: when table's primary key columns have NULL values
        """
        logger.info(f"Checking if table {table_name} has NULL in \
primary key field {primary_key_column}")

        count_null_primary_key = """
        SELECT count (distinct {primary_key_column})
        FROM {table_name}
        WHERE ({primary_key_column}) is NULL
        """.format(primary_key_column=primary_key_column,
                   table_name=table_name)
        records = redshift_hook.get_records(count_null_primary_key)

        num_records = records[0][0]
        if num_records > 0:
            raise ValueError(f"Data quality check failed. {table_name} \
contained NULL in primary key field {primary_key_column}")
        logger.info(f"Table {table_name} contains 0 records \
with NULL primary key")

    def check_duplicates(self, table_name, primary_key_column, redshift_hook):
        """Check if a table's primary key columns have duplicate values

        Args:
                table_name: name of table
                primary_key_column: primary key column(s)
                redshift_hook: redshift connection info

        Raises:
            ValueError: when table's primary key columns have duplicate values
        """
        logger.info(f"Checking if table {table_name} has duplicates in \
primary key field {primary_key_column}")

        count_distinct_primary_key = """
        SELECT count (distinct {primary_key_column})
        FROM {table_name}
        """.format(primary_key_column=primary_key_column,
                   table_name=table_name)
        records = redshift_hook.get_records(count_distinct_primary_key)
        distinct_records = records[0][0]

        count_all_primary_key = """
        SELECT count ({primary_key_column})
        FROM {table_name}
        """.format(primary_key_column=primary_key_column,
                   table_name=table_name)
        records = redshift_hook.get_records(count_all_primary_key)
        all_records = records[0][0]

        duplicate_records = all_records-distinct_records

        if int(distinct_records) != int(all_records):
            raise ValueError(f"Data quality check failed. {table_name} \
table has {duplicate_records} duplicate records")
        logger.info(f"Table {table_name} has {duplicate_records} records")

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for check in self.test_type:
            logger.info('\n')
            logger.info(f"Calling {check} method")
            for table_name in self.tables:
                func = getattr(DataQualityCheckOperator, f"{check}")
                if self.primary_key_column == {}:
                    func(self,
                         table_name,
                         {},
                         redshift_hook)
                else:
                    func(self,
                         table_name,
                         self.primary_key_column[table_name],
                         redshift_hook)
                logger.info('')
