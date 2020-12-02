from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Connection
from airflow import (models, settings)
from airflow.operators.python_operator import PythonOperator
from helpers import ConnectionInfo
import logging

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'btrivedi',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}


def create_airflow_conn(conf):
    conn = Connection()
    conn.conn_id = conf.get('conn_id')
    conn.conn_type = conf.get('conn_type')
    conn.host = conf.get('host')
    conn.port = conf.get('port')
    conn.login = conf.get('login')
    conn.password = conf.get('password')
    conn.schema = conf.get('schema')
    conn.extra = conf.get('extra')

    session = settings.Session()
    try:
        conn_name = session.\
            query(Connection).\
            filter(Connection.conn_id == conn.conn_id).first()
        if str(conn_name) == str(conf.get('conn_id')):
            logger.info(f"Connection {conn.conn_id} already exists")
            logger.info(f"Deleting connection {conn.conn_id}")
            session.delete(conn_name)
        else:
            session.add(conn)
            session.commit()
        session.close()
        logger.info(Connection.log_info(conn))
        logger.info(f'Connection {conn.conn_id} is created')
    except Exception as e:
        logging.info(f"Exception occured while creating connection \n {e}")


dag = DAG(
    dag_id='create_connections',
    schedule_interval='@once',
)

for connection in ConnectionInfo.get('connections'):
    crt_conn = PythonOperator(
        task_id='create_conn_{}'.format(connection.get('conn_id')),
        python_callable=create_airflow_conn,
        op_kwargs={'conf': connection},
        provide_context=False,
        default_args=default_args,
        dag=dag,
    )
