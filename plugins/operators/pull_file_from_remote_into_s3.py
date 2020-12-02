from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import pandas as pd
import csv
import boto3
import os
import gzip

from datetime import datetime

logger = logging.getLogger("airflow.task")


class PullFileFromRemoteIntoS3(BaseOperator):
    """ Operator to-
            - pull file from a source url into local
            - convert file from csv to psv.gz
            - upload file from local to s3
            - remove local temp path
    Parameters:
        aws_credentials_id (Str): aws credential name
                          stored under airflow
                          Admins> Connections
        source_url (Str): url to fetch the input file from
        s3_bucket (Str): destination s3 bucket
        s3_key (Str): destination s3 directory with filename
    Methods:
        download_file_to_local: method to download file from
                                source_url to local path
        upload_to_s3: method to upload file to s3
        execute
    """
    ui_color = '#A3E4D7'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 source_url="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):
        super(PullFileFromRemoteIntoS3, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.source_url = source_url
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def download_file_to_local(self, task_id, file_key, file_date):
        """Download file from source url into local based on task_id

        Args:
                task_id: to determine the task name
                file_key: string to prefix file_name
                file_date: date to pull file for.
                           Normally this should be previous_date
                           (if applicable)
        """
        try:
            if task_id == 'pull_covid_data_file_into_s3':
                localFilePath = \
                    f"/usr/local/airflow/tmp/{file_key}_{file_date}.json"
                if not os.path.exists(localFilePath):
                    try:
                        logging.info(self.source_url.
                                     format(file_date=file_date))
                        df = pd.read_csv(self.source_url.
                                         format(file_date=file_date),
                                         index_col=False,
                                         low_memory=False)
                    except Exception as e:
                        logging.info(e)
                else:
                    df = pd.read_csv(localFilePath,
                                     index_col=False,
                                     low_memory=False)
                df = df.rename(columns={
                                         "Last Update": "Last_Update",
                                         "Province/State": "Province_State",
                                         "Country/Region": "Country_Region"
                                        })
                df['Last_Update'] = df['Last_Update'].astype('datetime64[ns]')
                df['Last_Update'] = df['Last_Update'].\
                    dt.strftime('%Y-%m-%d %H:%M:%S')
                df['file_date'] = file_date
                df.to_json(localFilePath,
                           orient="records",
                           lines=True,
                           force_ascii=True)

            else:
                file_date = datetime.\
                    strptime(file_date, "%m-%d-%Y").\
                    strftime("%m-%d-%Y")
                localFilePath = \
                    f"/usr/local/airflow/tmp/{file_key}_{file_date}.psv"
                if not os.path.exists(localFilePath):
                    df = pd.read_csv(self.source_url,
                                     index_col=0,
                                     low_memory=False)
                else:
                    df = pd.read_csv(localFilePath,
                                     index_col=0, sep='|',
                                     low_memory=False)
                if df.empty:
                    raise Exception("There is no data received for the day")
                    logging.info("Retrying fetching the file")
                df.to_csv(localFilePath, sep="|")
                # gzip file
                localFilePathGzip = f"{localFilePath}.gz"
                with open(localFilePath, 'rb') as f_in,\
                        gzip.open(localFilePathGzip, 'wb') as f_out:
                    f_out.writelines(f_in)
                os.remove(localFilePath)
                localFilePath = localFilePathGzip
            return localFilePath

        except Exception as e:
            raise Exception(f"Error while downloading file \
                            from %s.\n Exception: \n %s"
                            % (self.source_url.
                               format(file_date=file_date), e))

    def upload_to_s3(self, credentials, localFilePath, s3_bucket, s3_key):
        """Upload file from local into s3

        Args:
            credentials: aws credential
            localFilePath: local file path for input file
            s3_bucket: destination s3 bucket
            s3_key: destination directory location
        """
        s3 = boto3.client('s3',
                          aws_access_key_id=credentials.access_key,
                          aws_secret_access_key=credentials.secret_key)
        fileName = os.path.basename(localFilePath)
        s3FilePath = f"{s3_key}/{fileName}"
        try:
            s3.upload_file(localFilePath, s3_bucket, s3FilePath)
            return
        except Exception as e:
            raise Exception("%s file failed to upload to s3. Exception: \n %s"
                            % (localFilePath, e))

    def execute(self, context):
        task_id = context["task"].task_id
        logger.info('Downloading file into local...')
        # download file to local
        localFilePath = self.download_file_to_local(task_id,
                                                    self.s3_key,
                                                    datetime.
                                                    strptime(
                                                        context["prev_ds"],
                                                        "%Y-%m-%d").
                                                    strftime("%m-%d-%Y"))
        logger.info(f"File downloadied to local path: {localFilePath}")

        logger.info('Moving file from local to s3...')
        # upload file to s3
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.upload_to_s3(credentials,
                          localFilePath,
                          self.s3_bucket,
                          self.s3_key)

        # remove file from local
        os.remove(localFilePath)
        logger.info(f"File {os.path.basename(localFilePath)} \
                    moved from local to s3 location: \
                    {self.s3_key}/{self.s3_bucket}...")

        logger.info('***** Task COMPLETE *****')
