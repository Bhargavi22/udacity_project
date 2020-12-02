from airflow.plugins_manager import AirflowPlugin
import operators
import helpers


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    helpers = [helpers.ConnectionInfo,
               helpers.CreateTableSQLs,
               helpers.sqlStatements]
    operators = [operators.CreateTablesInRedshiftOperator,
                 operators.PullFileFromRemoteIntoS3,
                 operators.S3ToRedshiftOperator,
                 operators.DataQualityCheckOperator]
