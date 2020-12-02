## File Info


<b>Create\_connection.py</b>
- DAG to create airflow connections based on the connection info specified in plugins/helpers/connection_info.py.  
- This DAG should be runs ONCE before running any other DAG.  
- [DAG Flow Diagram](https://github.com/Bhargavi22/udacity_project/blob/develop/dags/create_connections.png)

<b>load\_covid\_and\_mobility\_data\_daily.py</b>
- DAG to load covid and mobility data into redshift daily
- This DAG should be run once a day at 6PM PST
- Task flow:
  - Create Tables in Redshift if they donot exist already
  - Pull Covid data file for previous day into s3   
  - Pull google mobility data file into s3  
  - Pull location lookup file into s3  
  - Load the files from s3 into staging tables on s3  
  - Clean the data by running queries in plugins/helpers/sql\_statements.py  
  - Load cleaned data into to Redshift final table-  
    - Covid_data_us  
    - Covid_data_non_us  
    - Google_mobility_data_us  
    - Google_mobility_data_non_us  
    - Location_lookup  
  - Check staging and final data quality  
  - Drop all the staging tables at the end  
- [DAG flow diagram](https://github.com/Bhargavi22/udacity_project/blob/develop/dags/load_covid_and_mobility_data_daily.png)

<b>load\_covid\_and\_mobility\_data\_historical.py</b>
- DAG to load historical covid and mobility data  already present in s3 into redshift daily
- This DAG should be run ONLY once before starting to run load\_covid\_and\_mobility\_data\_daily.py DAG, to catchup on the historical data that have been retrieved so far. This DAG does following tasks -   
- Task flow:
  - Create Tables in Redshift if they do not exist already  
  - Load pre-existing files from s3 into staging tables on s3  
  - Clean the data by running queries in plugins/helpers/sql\_statements.py  
  - Load cleaned data into to Redshift final table-  
    - Covid_data_us  
    - Covid_data_non_us  
    - Google_mobility_data_us  
    - Google_mobility_data_non_us  
    - Location_lookup   
  - Check staging and final data quality  
  - Drop all the staging tables at the end  
  - NOTE: load\_covid\_and\_mobility\_data\_daily.py can also be used to do the catch by changing.
start\_time and catchup arguments of the DAG to a back date.
- [DAG flow diagram](https://github.com/Bhargavi22/udacity_project/blob/develop/dags/load_covid_and_mobility_data_historical.png)
