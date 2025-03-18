import re
import os
import pandas as pd
import psycopg2
import typer
from loguru import logger

from src.config import PROJ_ROOT
from src.features.utils import load_params

app = typer.Typer()

def clean_timestamps(df) -> None:
    """
    Clean timestamp columns by:
    1. Converting valid timestamps to datetime
    2. Converting dates with placeholder times (hh:mm) to midnight (00:00)
    3. Setting invalid or placeholder dates to None
    
    Args:
        df: DataFrame containing timestamp columns
        
    Returns:
        DataFrame with cleaned timestamp columns
    """
    # List of timestamp columns to process
    timestamp_columns = load_params("load")["timestamp_columns"]
    # Process only timestamp columns that exist in the DataFrame
    for col in [c for c in timestamp_columns if c in df.columns]:
        # logger.info(f"Processing timestamp column: {col}")
        
        # Create a temporary column to store processed values
        df[f"temp_{col}"] = None
        
        # Process each value in the column
        for idx in df.index:
            value = df.loc[idx, col]
            
            # Skip None/NaN values
            if pd.isna(value):
                continue
                
            # Convert to string for processing
            if not isinstance(value, str):
                value = str(value)
                
            # Case 1: Date with placeholder time (hh:mm) - convert to midnight
            if "hh:mm" in value:
                date_part = value.split(" ")[0]  # Extract date part
                try:
                    # Try to convert the date part to datetime with midnight time
                    date_obj = pd.to_datetime(date_part, format="%d/%m/%Y")
                    df.loc[idx, f"temp_{col}"] = date_obj
                    # logger.debug(f"Converted '{value}' to midnight: {date_obj}")
                except:
                    # If date part is invalid, set to None
                    df.loc[idx, f"temp_{col}"] = None
                    # logger.debug(f"Invalid date in '{value}', set to None")
            
            # Case 2: Placeholder date (dd/mm/yyyy) - set to None
            elif "dd/mm/yyyy" in value or "DD/MM/YYYY" in value:
                df.loc[idx, f"temp_{col}"] = None
                # logger.debug(f"Placeholder date '{value}', set to None")
            
            # Case 3: Try to parse as regular timestamp
            else:
                try:
                    # Try parsing with various formats
                    for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"]:
                        try:
                            date_obj = pd.to_datetime(value, format=fmt)
                            df.loc[idx, f"temp_{col}"] = date_obj
                            # logger.debug(f"Parsed '{value}' as timestamp: {date_obj}")
                            break  # Stop after successful parsing
                        except:
                            continue
                    else:  # This runs if no format worked
                        # Try pandas' flexible parser as last resort
                        try:
                            date_obj = pd.to_datetime(value, dayfirst=True)
                            df.loc[idx, f"temp_{col}"] = date_obj
                            # logger.debug(f"Parsed '{value}' with flexible parser: {date_obj}")
                        except:
                            df.loc[idx, f"temp_{col}"] = None
                            # logger.debug(f"Failed to parse '{value}', set to None")
                except:
                    df.loc[idx, f"temp_{col}"] = None
                    # logger.debug(f"Failed to parse '{value}', set to None")
        
        # Replace original column with processed values
        df[col] = df[f"temp_{col}"]
        # Drop temporary column
        df = df.drop(columns=[f"temp_{col}"])
    return df

def create_new_connection():
    """
    Creates a new connection to the database.
    If the target database doesn't exist, connects to postgres DB first and creates it.
    
    Returns:
        psycopg2.extensions.connection: Connection to the database
    """
    db_name = os.environ.get("DB_NAME", "donor_db")
    db_user = os.environ.get("DB_USER", "postgres")
    db_password = os.environ.get("DB_PASSWORD", "Clinic")
    db_host = os.environ.get("DB_HOST", "postgres")
    db_port = os.environ.get("DB_PORT", "5432")
    
    try:
        # First try connecting directly to the database
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        logger.info(f"Connection established successfully to database '{db_name}'.")
        return conn
        
    except psycopg2.OperationalError as e:
        # If the database doesn't exist, this will fail with "database does not exist"
        if "database" in str(e) and "does not exist" in str(e):
            logger.warning(f"Database '{db_name}' does not exist. Attempting to create it.")
            
            try:
                # Connect to default 'postgres' database to create the required database
                postgres_conn = psycopg2.connect(
                    dbname="postgres",
                    user=db_user,
                    password=db_password,
                    host=db_host,
                    port=db_port,
                )
                postgres_conn.autocommit = True  # Required for CREATE DATABASE
                postgres_cursor = postgres_conn.cursor()
                
                # Check if database already exists
                postgres_cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
                exists = postgres_cursor.fetchone()
                
                if not exists:
                    # Create the database
                    postgres_cursor.execute(f"CREATE DATABASE {db_name}")
                    logger.info(f"Database '{db_name}' created successfully.")
                
                postgres_cursor.close()
                postgres_conn.close()
                
                # Now connect to the newly created database
                conn = psycopg2.connect(
                    dbname=db_name,
                    user=db_user,
                    password=db_password,
                    host=db_host,
                    port=db_port,
                )
                logger.info(f"Connection established successfully to new database '{db_name}'.")
                return conn
                
            except psycopg2.Error as inner_e:
                raise psycopg2.Error(f"Failed to create database '{db_name}': {inner_e}")
        else:
            # Some other connection error
            raise psycopg2.Error(f"Error occurred while establishing connection: {e}")
            
    except psycopg2.Error as e:
        # Any other psycopg2 error
        raise psycopg2.Error(f"Error occurred while establishing connection: {e}")
@app.command()
def create_tables():
    """
    Creates a new table in the database with the specified columns.
    
    :param table_name: Name of the new table.
    :param columns: Dictionary containing the column names and data types.
    :param conn: psycopg2 connection object.
    """
    conn = create_new_connection()
    cursor = conn.cursor()
    logger.info("Dropping tables...")
    # SQL commands to drop tables if they exist
    drop_tables_sql = """
    DROP TABLE IF EXISTS donant CASCADE;
    DROP TABLE IF EXISTS analitica CASCADE;
    DROP TABLE IF EXISTS orina CASCADE;
    DROP TABLE IF EXISTS micro CASCADE;
    DROP TABLE IF EXISTS gsa CASCADE;
    DROP TABLE IF EXISTS imatge CASCADE;
    DROP TABLE IF EXISTS receptor CASCADE;
    DROP TABLE IF EXISTS oferta CASCADE;
    """
    # Execute the SQL commands to drop the tables
    cursor.execute(drop_tables_sql)
    logger.info("Tables dropped successfully.")
    cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    """)
    
    # Fetch all table names
    logger.info("Creating tables...")
    # Read the SQL commands from the file
    with open(PROJ_ROOT/'src/pipeline/tables_download.sql', 'r') as file:
        sql_commands = file.read()
    
    # Execute the SQL commands to create the table
    cursor.execute(sql_commands)
    logger.info("Tables created successfully.")
    conn.commit()
    cursor.close()
    return conn

def insert_data_to_db(df: pd.DataFrame, table_name: str, conn: psycopg2.extensions.connection) -> bool:
    """
    Inserts the data from the DataFrame into the specified database table.
    
    :param df: DataFrame containing the data to be inserted.
    :param table_name: Name of the database table.
    :param conn: psycopg2 connection
    :return: True if the data was successfully inserted, False otherwise.
    """
    cursor = conn.cursor()
    
    # Retrieve the column names from the database table
    cursor.execute("""
    SELECT EXISTS (
       SELECT FROM information_schema.tables 
       WHERE table_schema = 'public'
       AND table_name = %s
    );
    """, (table_name.lower(),))
    
    table_exists = cursor.fetchone()[0]
    if not table_exists:
        logger.error(f"Table '{table_name}' does not exist in the database")
        cursor.close()
        conn.close()
        return False

    cursor.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = %s
    ORDER BY ordinal_position
    """, (table_name.lower(),))

    db_columns = [row[0] for row in cursor.fetchall()]
    
    if not db_columns:
        logger.error(f"No columns found for table '{table_name}'. Check if the table exists and has columns.")
        cursor.close()
        conn.close()
        return False
    
    diff = len(df.columns) - len(db_columns)

    if diff != 0:
        logger.error(f"Columns in DataFrame do not match columns in database table: {diff} columns missing")
        logger.info(f"Found {len(df.columns)} columns in dataframe table:")
        logger.info(f"Found {len(db_columns)} columns in database table:")
        logger.info(f"Columns in DataFrame: {list(df.columns)}")
        logger.info(f"Columns in database: {db_columns}")
        return False

    # Generate the SQL insert statement using the database table columns
    columns = ', '.join(db_columns)
    data_tuples = [tuple(row) for row in df.values]
    # Use execute_batch for better performance with many rows
    try:
        if len(data_tuples) > 0:
            # Using psycopg2's execute_values for efficient batch insertion
            from psycopg2.extras import execute_values

            # Create the base INSERT statement
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
            
            # Execute the batch insertion
            execute_values(cursor, insert_query, data_tuples)
            
            affected_rows = len(data_tuples)
            logger.success(f"Successfully inserted {affected_rows} rows into {table_name}")
        else:
            logger.warning(f"No data to insert into {table_name}")

        conn.commit()
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data into {table_name}: {e}")
        return False
    finally:
        cursor.close()
    return True


@app.command()
def insert_data(db : dict) -> bool:
    """
    Inserts the data from the CSV file into the specified database table.
    
    :param table_name: Name of the database table.
    :param csv_path: Path to the CSV file.
    """
    connection = create_tables()
    for table, df in db.items():
        df = df.copy()
        
        # Clean timestamp data
        df = clean_timestamps(df)
        
        # Replace all pandas NA, NaN, etc. with None for SQL NULL
        df = df.astype(object).replace({pd.NA: None, pd.NaT: None})
        
        insert = insert_data_to_db(df, table, connection)
        if not insert:
            return False

    connection.close()
    return True

if __name__ == "__main__":
    app()