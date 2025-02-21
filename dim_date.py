from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import logging, time
import time
# For every database you connect to using Airflow you must install the
# provider.
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from includes.dim_date_lib import generate_dim_date

@dag(
    schedule=None,
    catchup=False,
    tags=["IST346", "testing"],
)

def dim_date():

    @task
    def load_dim_date():
        """
        Load the date dimension using the fast method that we used above
        to load the time dimension.
        """
        # Open our logger
        logger = logging.getLogger(__name__)

        # Generate our date data using our dim_date_lib.py code
        dates = generate_dim_date()

        # We have 26 column names, so use the list of column names
        # from our dim_date_lib library to create the SQL. (I don't like
        # to type that much, I already typed out the column names once.)
        column_name_list = dates['columns']
        # create a column delimited string
        # e.g. 'date_value, date_short, date_medium, ...'
        # that we can use in our SQL query
        column_names = ", ".join(column_name_list)
        # Define our SQL. dim_date has 26 columns, so we need 26 bind variables
        # this creates string of 26 %s characters seperated by commas
        bind_vars = ", ".join(['%s' for i in range(26)])
        sql = f"""
        insert into [sakila_dwh].[dim_date]
            ({column_names})
          values 
            ({bind_vars})
        """
        verification_sql = """
        select count(*) from [sakila_dwh].[dim_date]
        """
        logger.info(f"Dynamically Generated SQL Query is:\n{sql}")


        # Get the connection to the database that we configured in Airflow
        hook_db = MsSqlHook(mssql_conn_id="2024DWFall_Stephen_Keyen_DB")
        conn = hook_db.get_conn()
        # Set the connection to auto-commit. This means that it will commit
        hook_db.set_autocommit(conn, True)
        # Get the cursor
        cursor = conn.cursor()

        # First truncate the [dim_date] table
        cursor.execute("truncate table [sakila_dwh].[dim_date]")

        start = time.perf_counter() # start timer
        # Insert using our standard batch size of 5,000
        batch_size = 5000
        logger.info(f"Setting the batch size to {batch_size}.")
        cursor.executemany(sql, dates['values'], batch_size=batch_size)
        end = time.perf_counter() # end our timer

        # Calculate how long our insert took
        insert_duration = end - start
        logger.info(f"Batch insert took {insert_duration:0.4f} seconds.")

        # Verify that all data was loaded successfully
        # If all data was not loaded, fail the job by raising an
        # AirflowFailException
        cursor.execute(verification_sql)
        row = cursor.fetchone()
        if row[0] != len(dates['values']):
            msg = f"Failed to insert all rows into table dim_date. Loaded {row[0]} of {len(dates['values'])} rows."
            logger.error(msg)
            raise AirflowFailException(msg)
        
        # Log our success
        success_msg = f"Success {len(dates['values'])} rows loaded in {insert_duration:0.4f} seconds."
        logger.info(success_msg)
        return(success_msg)

    load_dim_date()

dim_date()