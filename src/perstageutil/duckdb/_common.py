"""
This package is an implementation detail and should not be used externally.

Contains common functions that are used across different areas.
"""

#This package is an implementation detail and should not be used externally.
from perstageutil.duckdb.session import Session
from collections import namedtuple
import pandas
import jinja2
from datetime import datetime

JINJA_PACKAGE_NAME = "perstageutil.duckdb.load"

DataObject = namedtuple("DataObject", "table_catalog table_schema table_name")

def split_three_part_name(three_part_name : str) -> DataObject:
    """
    This function is an implementation detail and should not be used externally.
    Splits a database name db.schema.table and returns as DataObject tuple which 
    is a named tuple that contains the three part name.
    
    :param three_part_name: Description
    :type three_part_name: str
    :return: Description
    :rtype: DataObject
    """
    table_catalog, table_schema, table_name = tuple(three_part_name.split("."))
    data_object = DataObject(table_catalog, table_schema, table_name)
    return data_object

#TODO Impplement this
def check_tables(session : Session, landing_table_object : DataObject, current_table_object : DataObject, hist_table_object : DataObject) -> bool:
    """
    This function is an implementation detail and should not be used externally.
    Check the tables to see if the proper metadata and keys are implemented.  Raise an error to stop processing in the event of missing 
    metadata columns or missing keys.
    
    :param session: Description
    :param landing_table_object: Description
    :type landing_table_object: DataObject
    :param current_table_object: Description
    :type current_table_object: DataObject
    :param hist_table_object: Description
    :type hist_table_object: DataObject
    """
    #landing just needs a load date timestamp
    check_landing_table_structure(session, landing_table_object)
    check_current_table_structure(session, current_table_object)
    check_hist_table_structure(session, hist_table_object)
    return True

def check_landing_table_structure(session : Session, landing_table_object : DataObject):
    """
    This function is an implementation detail and should not be used externally.
    Checks the landing table structure for the needed column.
    
    :param session: The current session as a Session object.
    :type session: Session
    :param landing_table_object: The landing table as a DataObject.
    :type landing_table_object: DataObject
    """
    df = return_columns_df(session, landing_table_object)
    if ((df["column_name"] == "__pstage_inserted_timestamp").any()) == False:
        raise Exception("Landing table is missing the needed __pstage_inserted_timestamp column.")
    
def current_date_time_file_case() -> str:
        now = datetime.now()
        date_return = now.strftime("%Y%m%d%H%M%S%f%p")
        return date_return

def check_current_table_structure(session : Session, current_table_object : DataObject):
    """This function is an implementation detail and should not be used externally."""
    df = return_columns_df(session, current_table_object)
    if ((df["column_name"] == "__pstage_inserted_timestamp").any()) == False:
        raise Exception("Current table is missing the needed __pstage_inserted_timestamp column.")
    if ((df["column_name"] == "__pstage_updated_timestamp").any()) == False:
        raise Exception("Current table is missing the needed __pstage_updated_timestamp column.")
    if ((df["column_name"] == "__pstage_deleted_indicator").any()) == False:
        raise Exception("Current table is missing the needed __pstage_deleted_indicator column.")
    if ((df["column_name"] == "__pstage_hash_diff").any()) == False:
        raise Exception("Current table is missing the needed __pstage_hash_diff column.")
    if ((df["column_name"] == "__pstage_dedupe_confidence_percent").any()) == False:
        raise Exception("Current table is missing the needed __pstage_dedupe_confidence_percent column.")
    if ((df["primary_key_indicator"] == True).any()) == False:
        raise Exception("Current table is missing the needed primary key.")
    
def check_and_fix_current_table_structure(session : Session, current_table_object : DataObject, primary_key_columns : list):
    """This function is an implementation detail and should not be used externally."""
    df = return_columns_df(session, current_table_object)
    if ((df["column_name"] == "__pstage_inserted_timestamp").any()) == False:
        sql = f"ALTER TABLE {current_table_object.table_catalog}.{current_table_object.table_schema}.{current_table_object.table_name} ADD COLUMN __pstage_inserted_timestamp TIMESTAMP NOT NULL;"
        exec_ddl(session, sql)
        session.logger.info("Current table was missing the needed __pstage_inserted_timestamp column which was added")
    if ((df["column_name"] == "__pstage_updated_timestamp").any()) == False:
        sql = f"ALTER TABLE {current_table_object.table_catalog}.{current_table_object.table_schema}.{current_table_object.table_name} ADD COLUMN __pstage_updated_timestamp TIMESTAMP NULL;"
        exec_ddl(session, sql)
        session.logger.info("Current table was missing missing the needed __pstage_updated_timestamp column which was added")
    if ((df["column_name"] == "__pstage_deleted_indicator").any()) == False:
        sql = f"ALTER TABLE {current_table_object.table_catalog}.{current_table_object.table_schema}.{current_table_object.table_name} ADD COLUMN __pstage_deleted_indicator boolean NOT NULL;"
        exec_ddl(session, sql)
        session.logger.info("Current table was missing missing the needed __pstage_deleted_indicator column which was added")
    if ((df["column_name"] == "__pstage_hash_diff").any()) == False:
        sql = f"ALTER TABLE {current_table_object.table_catalog}.{current_table_object.table_schema}.{current_table_object.table_name} ADD COLUMN __pstage_hash_diff varchar(32) NULL;"
        exec_ddl(session, sql)
        session.logger.info("Current table was missing missing the needed __pstage_hash_diff column which was added")
    if ((df["column_name"] == "__pstage_dedupe_confidence_percent").any()) == False:
        sql = f"ALTER TABLE {current_table_object.table_catalog}.{current_table_object.table_schema}.{current_table_object.table_name} ADD COLUMN __pstage_dedupe_confidence_percent float NOT NULL;"
        exec_ddl(session, sql)
        session.logger.info("Current table was missing missing the needed __pstage_dedupe_confidence_percent column which was added")
    if ((df["primary_key_indicator"] == True).any()) == False:
        primary_key_columns_sql = ", ".join(primary_key_columns)
        sql = f"ALTER TABLE {current_table_object.table_catalog}.{current_table_object.table_schema}.{current_table_object.table_name} ADD ADD PRIMARY KEY ({primary_key_columns_sql});"
        exec_ddl(session, sql)
        session.logger.info("Current table was missing missing needed primary key which was added")
    
def check_hist_table_structure(session : Session, hist_table_object : DataObject):
    """This function is an implementation detail and should not be used externally."""
    df = return_columns_df(session, hist_table_object)
    if ((df["column_name"] == "__pstage_effective_timestamp").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_effective_timestamp column.")
    if ((df["column_name"] == "__pstage_expiration_timestamp").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_expiration_timestamp column.")
    if ((df["column_name"] == "__pstage_current_version_indicator").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_current_version_indicator column.")
    if ((df["column_name"] == "__pstage_inserted_timestamp").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_inserted_timestamp column.")
    if ((df["column_name"] == "__pstage_updated_timestamp").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_updated_timestamp column.")
    if ((df["column_name"] == "__pstage_deleted_indicator").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_deleted_indicator column.")
    if ((df["column_name"] == "__pstage_hash_diff").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_hash_diff column.")
    if ((df["column_name"] == "__pstage_dedupe_confidence_percent").any()) == False:
        raise Exception("Historical table is missing the needed __pstage_dedupe_confidence_percent column.")
    if ((df["primary_key_indicator"] == True).any()) == False:
        raise Exception("Historical table is missing the needed primary key.")
    if (((df["primary_key_indicator"] == True) & (df["column_name"] == "__pstage_effective_timestamp")).any()) == False:
        raise Exception("Historical tables primary key does not include the __pstage_effective_timestamp column.")
    
def check_and_fix_hist_table_structure(session : Session, hist_table_object : DataObject, primary_key_columns : list):
    """This function is an implementation detail and should not be used externally."""
    df = return_columns_df(session, hist_table_object)
    if ((df["column_name"] == "__pstage_effective_timestamp").any()) == False:
        sql = f"ALTER TABLE {hist_table_object.table_catalog}.{hist_table_object.table_schema}.{hist_table_object.table_name} ADD COLUMN __pstage_effective_timestamp TIMESTAMP NOT NULL;"
        exec_ddl(session, sql)
        session.logger.info("Historical table was missing the needed __pstage_effective_timestamp column which was added")
    if ((df["column_name"] == "__pstage_expiration_timestamp").any()) == False:
        sql = f"ALTER TABLE {hist_table_object.table_catalog}.{hist_table_object.table_schema}.{hist_table_object.table_name} ADD COLUMN __pstage_expiration_timestamp TIMESTAMP NULL;"
        exec_ddl(session, sql)
        session.logger.info("Historical table was missing the needed __pstage_expiration_timestamp column which was added")
    if ((df["column_name"] == "__pstage_current_version_indicator").any()) == False:
        sql = f"ALTER TABLE {hist_table_object.table_catalog}.{hist_table_object.table_schema}.{hist_table_object.table_name} ADD COLUMN__pstage_current_version_indicator BOOLEAN NULL;"
        exec_ddl(session, sql)
        session.logger.info("Historical table was missing the needed __pstage_current_version_indicator column which was added")
    if ((df["column_name"] == "__pstage_inserted_timestamp").any()) == False:
        sql = f"ALTER TABLE {hist_table_object.table_catalog}.{hist_table_object.table_schema}.{hist_table_object.table_name} ADD COLUMN __pstage_inserted_timestamp TIMESTAMP NOT NULL;"
        exec_ddl(session, sql)
        session.logger.info("Historical table was missing the needed __pstage_inserted_timestamp column which was added")
    if ((df["column_name"] == "__pstage_updated_timestamp").any()) == False:
        sql = f"ALTER TABLE {hist_table_object.table_catalog}.{hist_table_object.table_schema}.{hist_table_object.table_name} ADD COLUMN __pstage_updated_timestamp TIMESTAMP NULL;"
        exec_ddl(session, sql)
        session.logger.info("Historical table was missing missing the needed __pstage_updated_timestamp column which was added")
    if ((df["column_name"] == "__pstage_deleted_indicator").any()) == False:
        sql = f"ALTER TABLE {hist_table_object.table_catalog}.{hist_table_object.table_schema}.{hist_table_object.table_name} ADD COLUMN __pstage_deleted_indicator boolean NOT NULL;"
        exec_ddl(session, sql)
        session.logger.info("Historical table was missing missing the needed __pstage_deleted_indicator column which was added")
    if ((df["column_name"] == "__pstage_hash_diff").any()) == False:
        sql = f"ALTER TABLE {hist_table_object.table_catalog}.{hist_table_object.table_schema}.{hist_table_object.table_name} ADD COLUMN __pstage_hash_diff varchar(32) NULL;"
        exec_ddl(session, sql)
        session.logger.info("Historical table was missing missing the needed __pstage_hash_diff column which was added")
    if ((df["column_name"] == "__pstage_dedupe_confidence_percent").any()) == False:
        sql = f"ALTER TABLE {hist_table_object.table_catalog}.{hist_table_object.table_schema}.{hist_table_object.table_name} ADD COLUMN __pstage_dedupe_confidence_percent float NOT NULL;"
        exec_ddl(session, sql)
        session.logger.info("Historical table was missing missing the needed __pstage_dedupe_confidence_percent column which was added")
    if ((df["primary_key_indicator"] == True).any()) == False:
        primary_key_columns_sql = ", ".join(primary_key_columns) + ", __pstage_effective_timestamp"
        sql = f"ALTER TABLE {hist_table_object.table_catalog}.{hist_table_object.table_schema}.{hist_table_object.table_name} ADD ADD PRIMARY KEY ({primary_key_columns_sql});"
        exec_ddl(session, sql)
        session.logger.info("Historical table was missing missing needed primary key which was added")
    if (((df["primary_key_indicator"] == True) & (df["column_name"] == "__pstage_effective_timestamp")).any()) == False:
        raise Exception("Historical tables primary key does not include the __pstage_effective_timestamp column.")

def return_columns_df(session : Session, table_object : DataObject) -> pandas.DataFrame:
    """This function is an implementation detail and should not be used externally."""
    sql = f"""
    SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.ordinal_position, c.is_nullable, c.data_type,
    c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.datetime_precision,
    CASE
        WHEN kcu.constraint_name IS NULL THEN FALSE
        ELSE TRUE
    END AS primary_key_indicator
    FROM information_schema.columns c
    LEFT JOIN information_schema.table_constraints tc
    ON c.table_catalog = tc.table_catalog
    AND c.table_schema = tc.table_schema
    AND c.table_name = tc.table_name
    AND tc.constraint_type = 'PRIMARY KEY'
    LEFT JOIN information_schema.key_column_usage kcu
    ON c.table_catalog = kcu.table_catalog
    AND c.table_schema = kcu.table_schema
    AND c.table_name = kcu.table_name
    AND c.column_name = kcu.column_name
    AND tc.constraint_name = kcu.constraint_name
    WHERE c.table_catalog = '{table_object.table_catalog}'
    AND c.table_schema = '{table_object.table_schema}'
    AND c.table_name = '{table_object.table_name}'
    ORDER BY c.table_catalog, c.table_schema, c.table_name, c.ordinal_position;
    """
    df = exec_sql_return_df(session, sql)
    num_rows = df.shape[0]
    if num_rows == 0:
        table_name = table_object.table_catalog + "." + table_object.table_schema + "." + table_object.table_name
        raise Exception(f"Table {table_name} does not exist.")
    return df

def return_any_new_columns_df(session : Session, landing_table_object : DataObject, check_table_object : DataObject) -> pandas.DataFrame:
    """This function is an implementation detail and should not be used externally.
        Returns new columns only.  All that is checked is the column name between the landing table and the 
        check table (the current table is usually used.)
    """
    sql = f"""
    WITH CTE_NEW_COLUMNS
    AS
    (
    SELECT column_name AS new_column_name
    FROM information_schema.columns l
    WHERE table_catalog = '{landing_table_object.table_catalog}'
    AND table_schema = '{landing_table_object.table_schema}'
    AND table_name = '{landing_table_object.table_name}'
    AND column_name NOT LIKE '__pstage%'
    EXCEPT
    SELECT column_name as new_column_name
    FROM information_schema.columns l
    WHERE table_catalog = '{check_table_object.table_catalog}'
    AND table_schema = '{check_table_object.table_schema}'
    AND table_name = '{check_table_object.table_name}'
    AND column_name NOT LIKE '__pstage%'
    )
    SELECT l.column_name, l.data_type,
    l.character_maximum_length, l.numeric_precision, l.numeric_scale, l.datetime_precision
    FROM information_schema.columns l
    INNER JOIN CTE_NEW_COLUMNS cnc
    ON l.column_name = cnc.new_column_name
    WHERE table_catalog = '{landing_table_object.table_catalog}'
    AND table_schema = '{landing_table_object.table_schema}'
    AND table_name = '{landing_table_object.table_name}'
    AND column_name NOT LIKE '__pstage%';
    """
    df = exec_sql_return_df(session, sql)
    # num_rows = df.shape[0]
    # if num_rows == 0:
    #     table_name = landing_table_object.table_catalog + "." + landing_table_object.table_schema + "." + landing_table_object.table_name
    #     raise Exception(f"Table {table_name} does not exist.")
    return df

def return_any_changed_columns_df(session : Session, landing_table_object : DataObject, check_table_object : DataObject) -> pandas.DataFrame:
    """This function is an implementation detail and should not be used externally."""
    sql = f"""
    SELECT column_name, data_type,
    character_maximum_length, numeric_precision, numeric_scale, datetime_precision
    FROM information_schema.columns l
    WHERE table_catalog = '{landing_table_object.table_catalog}'
    AND table_schema = '{landing_table_object.table_schema}'
    AND table_name = '{landing_table_object.table_name}'
    AND column_name NOT LIKE '__pstage%'
    EXCEPT
    SELECT column_name, data_type,
    character_maximum_length, numeric_precision, numeric_scale, datetime_precision
    FROM information_schema.columns l
    WHERE table_catalog = '{check_table_object.table_catalog}'
    AND table_schema = '{check_table_object.table_schema}'
    AND table_name = '{check_table_object.table_name}'
    AND column_name NOT LIKE '__pstage%';
    """
    df = exec_sql_return_df(session, sql)
    # num_rows = df.shape[0]
    # if num_rows == 0:
    #     table_name = landing_table_object.table_catalog + "." + landing_table_object.table_schema + "." + landing_table_object.table_name
    #     raise Exception(f"Table {table_name} does not exist.")
    return df

def exec_sql_return_df(session : Session, sql : str) -> pandas.DataFrame:
    """
    This function is an implementation detail and should not be used externally.
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    :return: Description
    :rtype: DataFrame
    """
    try:
        session.logger.debug(f"SQL: {sql}")
        results = session.conn.execute(sql).df()
        #results = session.conn.sql(sql).df()
    except Exception as err:
        session.logger.error(err)
        raise err
    return results

def create_sql(template : str, context : dict):
    """
    This function is an implementation detail and should not be used externally.
    
    :param template: Description
    :type template: str
    :param context: Description
    :type context: dict
    """
    template : jinja2.Template
    env = jinja2.Environment(
        loader=jinja2.PackageLoader(JINJA_PACKAGE_NAME),
        trim_blocks=True,
        lstrip_blocks = True,
        autoescape=jinja2.select_autoescape()
    )
    template = env.get_template(template)
    #,
    #    autoescape=jinja2.select_autoescape()
    sql = template.render(context)
    return sql

def exec_dml(session : Session, sql : str):
    """
    This function is an implementation detail and should not be used externally.
    Executes data manipulation language statements in DuckDb and captures and logs the Updated rows output.
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    """
    try:
        session.logger.debug(f"SQL: {sql}")
        session.logger.info(f"Partial output of query being executed (first 256 characters):\n{sql[:256]}\n...")
        #session.conn.sql(sql)
        results = session.conn.execute(sql).fetchall()
        for result in results:
            updated_rows, = result
            updated_rows = str(updated_rows)
            session.logger.info(f"Updated rows: {updated_rows}.")
    except Exception as err:
        session.logger.error(err)
        raise err
    
def exec_sql_return(session : Session, sql : str):
    """
    This function is an implementation detail and should not be used externally.
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    """
    try:
        session.logger.debug(f"SQL: {sql}")
        results = session.conn.execute(sql).fetchall()
        #results = session.conn.sql(sql).fetchall()
    except Exception as err:
        session.logger.error(err)
        raise err
    return results

def exec_ddl(session : Session, sql : str):
    """
    This function is an implementation detail and should not be used externally.
    Executes data manipulation language statements in DuckDb and captures and logs the Updated rows output.
    
    :param session: Description
    :type session: Session
    :param sql: Description
    :type sql: str
    """
    try:
        session.logger.debug(f"SQL: {sql}")
        session.logger.info(f"Partial output of query being executed (first 256 characters):\n{sql[:256]}\n...")
        #session.conn.sql(sql)
        results = session.conn.execute(sql).fetchall()
        for result in results:
            # updated_rows, = result
            # updated_rows = str(updated_rows)
            session.logger.info(f"Result: {result}.")
    except Exception as err:
        session.logger.error(err)
        raise err

def convert_df_to_records(df : pandas.DataFrame):
    """
    This function is an implementation detail and should not be used externally.
    
    :param df: Description
    :type df: pandas.DataFrame
    """
    records = df.to_dict("records")
    return records
