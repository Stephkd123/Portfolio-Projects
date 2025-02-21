# Airflow and Standard Library Includes
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
import logging, time, os
# For every database you connect to using Airflow you must install the
# provider.
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# Airflow Operator for executing SQL from a file
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Pull in our custom operators
from includes.operators.MsSqlCopyOperator import MsSqlCopyOperator


# Notice the template_searchpath directive, it is used by the SQLExecuteQueryOperator
# to find your SQL files.
# For our airflow Docker install os.getcwd() returns '/opt/airflow'
@dag(
    schedule=None,
    catchup=False,
    tags=["IST346", "testing"],
    template_searchpath=os.path.join(os.getcwd(), 'dags', 'includes', 'sql')
)

def dim_film():
    """
    Create the film dimensions, dim_film.
    """

    @task_group(group_id="film_load")
    def load():
        @task
        def truncate_dest():
            logger = logging.getLogger(__name__)
            # Get the connection to the database that we configured in Airflow
            hook_db = MsSqlHook(mssql_conn_id="2024DWFall_Stephen_Keyen_DB")
            conn = hook_db.get_conn()
            # Get the cursor
            cursor = conn.cursor()
            # First truncate the [dim_date] table
            cursor.execute("truncate table [sakila_dwh].[dim_film]")
            # Reset the IDENTITY column
            cursor.execute("DBCC CHECKIDENT ('[SAKILA_DWH].[dim_film]', RESEED, 1)")
            conn.commit()
            logger.info("finished truncating destination and resetting the IDENTITY column.")

        # In sakila_dwh dim_film has an identity column film_key, since it auto populates MS SQL seems to know
        # not to try to insert a values, and so the insert statement with no column names still works. However,
        # it would probably be best to rewrite the operator to specify column names, at least optionally.
        load_data = MsSqlCopyOperator(task_id = "load_dim_film",
                                      src_conn_id = "2024DWFall_Stephen_Keyen_DB", src_schema = "stage", src_table = "dim_film",
                                      dest_conn_id = "2024DWFall_Stephen_Keyen_DB", dest_schema = "SAKILA_DWH", dest_table = "dim_film",
                                      batch_size = 5000)

        truncate_dest() >> load_data
        


    @task_group(group_id="film_extract")
    def extract():
        # Configure our operator to copy the film table from Sakila to our stage schema
        # note that we need a task_id, this is a requirement of the BaseOperator class.
        extract_film = MsSqlCopyOperator(task_id = "extract_film_table",
                                        src_conn_id = "Sakila_DB", src_schema = "sakila", src_table = "film",
                                        dest_conn_id = "2024DWFall_Stephen_Keyen_DB", dest_schema = "stage", dest_table = "film",
                                        batch_size = 5000)
        extract_film_category = MsSqlCopyOperator(task_id = "extract_film_category_table",
                                        src_conn_id = "Sakila_DB", src_schema = "sakila", src_table = "film_category",
                                        dest_conn_id = "2024DWFall_Stephen_Keyen_DB", dest_schema = "stage", dest_table = "film_category",
                                        batch_size = 5000)
        extract_category = MsSqlCopyOperator(task_id = "extract_category_table",
                                        src_conn_id = "Sakila_DB", src_schema = "sakila", src_table = "category",
                                        dest_conn_id = "2024DWFall_Stephen_Keyen_DB", dest_schema = "stage", dest_table = "category",
                                        batch_size = 5000)
        extract_language = MsSqlCopyOperator(task_id = "extract_language_table",
                                        src_conn_id = "Sakila_DB", src_schema = "sakila", src_table = "language",
                                        dest_conn_id = "2024DWFall_Stephen_Keyen_DB", dest_schema = "stage", dest_table = "language",
                                        batch_size = 5000)
        extract_film_text = MsSqlCopyOperator(task_id = "extract_film_text_table",
                                        src_conn_id = "Sakila_DB", src_schema = "sakila", src_table = "film_text",
                                        dest_conn_id = "2024DWFall_Stephen_Keyen_DB", dest_schema = "stage", dest_table = "film_text",
                                        batch_size = 5000)

        # call our operator like it is a task in the DAG
        # everything on its own line will be executed in parallel by Airflow
        # everything that is seperated by the >> operator will be executed in series by airflow
        extract_film
        extract_film_category >> extract_category
        extract_language
        extract_film_text
    
    @task_group(group_id='transform')
    def transform():
        
        transform = SQLExecuteQueryOperator(
                        task_id='transform_data',
                        conn_id='2024DWFall_Stephen_Keyen_DB',
                        sql='transform_dim_film.sql',
                    )

        @task
        def truncate():
            logger = logging.getLogger(__name__)
            # Get the connection to the database that we configured in Airflow
            hook_db = MsSqlHook(mssql_conn_id="2024DWFall_Stephen_Keyen_DB")
            conn = hook_db.get_conn()
            # Get the cursor
            cursor = conn.cursor()
            # First truncate the [dim_date] table
            cursor.execute("truncate table [stage].[dim_film]")
            conn.commit()
            logger.info("finished truncating [stage].[dim_film]")

        @task
        def verify():
            logger = logging.getLogger(__name__)
            # Get the connection to the database that we configured in Airflow
            hook_db = MsSqlHook(mssql_conn_id="2024DWFall_Stephen_Keyen_DB")
            conn = hook_db.get_conn()
            # Get the cursor
            cursor = conn.cursor()
            # In this case the transform is not adding any rows to the table
            # so a very simple (and not really sufficient) verification is to just
            # count the number of rows and compare them.
            verification_src = "select count(*) from [stage].[film]"
            verification_dest = "select count(*) from [stage].[dim_film]"
            cursor.execute(verification_src)
            row = cursor.fetchone()
            src_rows = row[0]
            cursor.execute(verification_dest)
            row = cursor.fetchone()
            dest_rows = row[0]
            if src_rows != dest_rows:
                raise AirflowFailException("Transform failed, [stage].[dim_film] does not have the correct number of rows.")
            logger.info("Transform Verified Successfully")
        
        # first we truncate our stage table, then do the transform that loads dim_fact, then verify the transform
        truncate() >> transform >> verify()
    
    # first extract, then transform
    extract() >> transform() >> load()

dim_film()