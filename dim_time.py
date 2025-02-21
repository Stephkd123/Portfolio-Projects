# Airflow and Standard Library Includes
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import logging, time
# For every database you connect to using Airflow you must install the
# provider.
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# Our includes that generate the time and date dimensional data
from includes.dim_time_lib import generate_dim_time

@dag(
    schedule=None,
    catchup=False,
    tags=["IST346", "testing"],
)

def dim_time():
    
    @task
    def load():
        """
        Loads dim_time using the pymssql executemany method and specifying the 
        batch size.

        Strangely enough the option to specify the batchsize is not documented
        in the pymssql documentation for the stable version:
        https://www.pymssql.org/en/stable/ref/pymssql.html#pymssql.Cursor.executemany

        However, you can see that it was added to the source code on Nov 13, 2023,
        https://github.com/pymssql/pymssql/commit/4866d619d62782f8b6b579d0441c3defb90e712e
        closing, in particular, this issue https://github.com/pymssql/pymssql/issues/332
        """

        # Define our SQL, we have one SQL statement we are using to insert
        # the data, and another SQL statement we can use to retrieve the 
        # number of rows we inserted from the DB
        # Note that we do not insert a value for "time_key", since that is an
        # identity column the DB will increment that automatically.
        sql = """
        insert into [sakila_dwh].[dim_time] 
          (time_key, time_value, hours24, hours12, minutes, seconds, am_pm)
           values
          (%s, %s, %s, %s, %s, %s, %s)
        """
        verification_sql = """
        select count(*) from [sakila_dwh].[dim_time]
        """

        # Open our logger
        logger = logging.getLogger(__name__)

        # Get the connection to the database that we configured in Airflow
        hook_db = MsSqlHook(mssql_conn_id="2024DWFall_Stephen_Keyen_DB")
        # Get the actual pymssql database connect from the Airflow "hook"
        conn = hook_db.get_conn()
        # Set the connection to auto-commit. This means that it will commit
        # between batches. This is necessary, otherwise, if you are loading
        # tens of thousands of rows, you will run out of transaction memory 
        # on the database and your job will fail with an error like the following:
        #
        # 9002, b"The transaction log for database '2024DWFall_Stephen_Keyen_DB' is full 
        # due to 'ACTIVE_TRANSACTION'.DB-Lib error message 20018
        hook_db.set_autocommit(conn, True)

        # Generate our new time data using our date_time.py code
        times = generate_dim_time()

        # First truncate the [dim_time] table, we are going to completely
        # replaces the contents
        cursor = conn.cursor()
        cursor.execute("truncate table [sakila_dwh].[dim_time]")

        # Start a timer for our load
        start = time.perf_counter()

        # At a batch size of 15,000 the load missed rows! So the total count of rows
        # in the database didn't match the size of the array we loaded. Reducing the
        # batch size to ~10,000 allowed all rows to be inserted correctly.
        # I suspect this is a bug in pymssql.
        batch_size = 10000
        logger.info(f"Setting the batch size to {batch_size}.")
        cursor.executemany(sql, times['values'], batch_size=batch_size)

        # End the timer
        end = time.perf_counter()
        # Calculate how long our insert took
        insert_duration = end - start
        logger.info(f"Batch insert took {insert_duration:0.4f} seconds.")

        # Verify that all data was loaded successfully
        # If all data was not loaded, fail the job by raising an
        # AirflowFailException
        cursor.execute(verification_sql)
        row = cursor.fetchone()
        if row[0] != len(times['values']):
            msg = f"Failed to insert all rows into table. Loaded {row[0]} of {len(times['values'])} rows."
            logger.error(msg)
            raise AirflowFailException(msg)
        
        # Log our success
        success_msg = f"Success {len(times['values'])} rows loaded in {insert_duration:0.4f} seconds."
        logger.info(success_msg)
        return(success_msg)

    load()

dim_time()