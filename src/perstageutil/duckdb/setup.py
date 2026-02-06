#
# Duckdb persisted staging load program
#

#import jinja2
import pandas
import warnings

# need the below to be able to run this and pick up the local lib if testing this by itself.
# if not running this directly, this import will be skipped.
if __name__ == '__main__':
    import sys
    import os
    sys.path.insert(1, os.path.join(os.getcwd(), 'src'))

from perstageutil.duckdb.session import Session
import perstageutil.duckdb._common as _common
from perstageutil.duckdb._common import DataObject

#constant for setting the jinja package name
JINJA_PACKAGE_NAME = "perstageutil.duckdb.load"

#The main exec of this function.  This mimics Snowflake python procedures a bit on that you need a main handeler to call so that
# this codd can then be used as a template to create procedures within cloud RDBMS environments.
def exec(session : Session, landing_table_full_name : str, current_table_full_name : str, hist_table_full_name : str, cks_table_full_name: str, primary_key_columns : list):
    """
    Executes a persisted staging load within a persisted staging namespace.
    
    :param session: Contains the connection information holds the session state for a load.
    :type session: Session
    :param landing_table_full_name: The landing table name that is the source for the persisted staging load data.  This table contains the imported data and is used as the source to merge into the current table and insert and update the historical table.  The table needs to have the needed metadata columns or the load will error.
    :type landing_table_full_name: str
    :param current_table_full_name: The current table which will be created if it does not exist.
    :type current_table_full_name: str
    :param hist_table_full_name: The historical table which will be created if it does not exist.
    :type hist_table_full_name: str
    :param cks_table_full_name: The current key snapshot table which will be created if it does not exist.
    :type cks_table_full_name: str
    :param primary_key_columns:  The primary key columns for the tables in this namespace.
    :type primary_key_columns: list
    """
    session.logger.info(f"Starting setup for namespace {current_table_full_name}.")
    landing_table_object = _common.split_three_part_name(landing_table_full_name)
    current_table_object = _common.split_three_part_name(current_table_full_name)
    hist_table_object = _common.split_three_part_name(hist_table_full_name)
    cks_table_object = _common.split_three_part_name(cks_table_full_name)
    #a landing table needs to be there...if it does not exist throw an error.
    if _check_table_exists(session, landing_table_object) == False:
        raise Exception(f"The landing table {landing_table_object.table_catalog}.{landing_table_object.table_schema}.{landing_table_object.table_name} was not found.")

    landing_table_df = _common.return_columns_df(session, landing_table_object)
    keys_df = _return_key_columns_df(landing_table_df, primary_key_columns)
    attributes_df = _return_attribute_columns_only_df(landing_table_df, primary_key_columns)
    key_records = _add_data_type_sql(_common.convert_df_to_records(keys_df))
    attribute_records = _add_data_type_sql(_common.convert_df_to_records(attributes_df))

    #check the landing table for the metadata columns:
    if _check_landing_table_structure(session, landing_table_object) == False:
        _fix_landing_table(session, landing_table_object)
    
    if _check_table_exists(session, current_table_object) == False:
        create_current_table_sql = _create_table_sql(current_table_object, "create_current_table", key_records, attribute_records)
        _common.exec_ddl(session, create_current_table_sql)
    else:
        _common.check_and_fix_current_table_structure(session, current_table_object, primary_key_columns)
        _evolve_table(session, landing_table_object, current_table_object)

    if _check_table_exists(session, hist_table_object) == False:
        create_hist_table_sql = _create_table_sql(hist_table_object, "create_hist_table", key_records, attribute_records)
        _common.exec_ddl(session, create_hist_table_sql)
    else:
        _common.check_and_fix_hist_table_structure(session, hist_table_object, primary_key_columns)
        _evolve_table(session, landing_table_object, hist_table_object)

    if _check_table_exists(session, cks_table_object) == False:
        create_cks_table_sql = _create_table_sql(cks_table_object, "create_cks_table", key_records, attribute_records)
        _common.exec_ddl(session, create_cks_table_sql)
    else:
        #cks is not yet implemented so do nothing for now.
        create_cks_table_sql = ""
        warnings.warn("CKS evolution is not yet implemented.", category=UserWarning)
    
    session.logger.info(f"Setup completed for namespace {current_table_full_name}.")
    #test_select2(session)

def _evolve_table(session: Session, landing_table_object : DataObject, check_table_object : DataObject):
    #first add any new columns.
    df = _common.return_any_new_columns_df(session, landing_table_object, check_table_object)
    num_rows = df.shape[0]
    if num_rows > 0:
        rows = _add_data_type_sql(_common.convert_df_to_records(df))
        for row in rows:
            sql = f"ALTER TABLE {check_table_object.table_catalog}.{check_table_object.table_schema}.{check_table_object.table_name} ADD COLUMN {row['column_name']} {row['data_type_sql']} NULL;"
            _common.exec_ddl(session, sql)
        df = _common.return_columns_df(session, check_table_object)
        hash_diff_build_df = (df[(df["primary_key_indicator"] == False) & (~df["column_name"].str.startswith("__pstage_", na=False))]).sort_values(by=["column_name"])
        rows = _add_md5_sql_column(_common.convert_df_to_records(hash_diff_build_df))
        sql = _create_update_hash_diff_sql(check_table_object, "update_hash_diff", rows)
        _common.exec_dml(session, sql)
    #now handle any columns that changed datatypes or lengths...
    df = _common.return_any_changed_columns_df(session, landing_table_object, check_table_object)
    num_rows = df.shape[0]
    if num_rows > 0:
        date_time = _common.current_date_time_file_case()
        rows = _add_data_type_sql(_common.convert_df_to_records(df))
        for row in rows:
            column_name = row['column_name']
            renamed_column_name = f"{row['column_name']}_{date_time}"
            data_type_sql = row['data_type_sql']
            sql = f"ALTER TABLE {check_table_object.table_catalog}.{check_table_object.table_schema}.{check_table_object.table_name} RENAME COLUMN {column_name} TO {renamed_column_name};"
            _common.exec_ddl(session, sql)
            sql = f"ALTER TABLE {check_table_object.table_catalog}.{check_table_object.table_schema}.{check_table_object.table_name} ADD COLUMN {column_name} {data_type_sql} NULL;"
            _common.exec_ddl(session, sql)
            #note we do these one at a time instead of bulk in case one conversion fails.  If it fails, 
            #we need to assume the conversion is not possible, but since we don't drop the updated column 
            #there is no data loss.
            sql = _create_update_converted_column_sql(check_table_object, column_name, renamed_column_name, data_type_sql, "update_converted_column")
            try:
                _common.exec_dml(session, sql)
            except Exception as err:
                msg = f"The column {row['column_name']} update after data type conversion failed.  The data was not converted and loaded into the updated column. The data still resides in the renamed column {row['column_name']}_{date_time}."
                session.logger.warning(msg)
        df = _common.return_columns_df(session, check_table_object)
        hash_diff_build_df = (df[(df["primary_key_indicator"] == False) & (~df["column_name"].str.startswith("__pstage_", na=False))]).sort_values(by=["column_name"])
        rows = _add_md5_sql_column(_common.convert_df_to_records(hash_diff_build_df))
        sql = _create_update_hash_diff_sql(check_table_object, "update_hash_diff", rows)
        _common.exec_dml(session, sql)


def _add_md5_sql_column(rows) -> list:
    for row in rows:
        if row["data_type"].upper() == "VARCHAR":
            row["column_md5_sql"] = f"IFNULL(TRIM({row["column_name"]}), '')"
        else:
            row["column_md5_sql"] = f"IFNULL(CAST({row["column_name"]} AS VARCHAR(128)), '')"
    return rows

def _create_update_hash_diff_sql(table_object : DataObject, jinja_template_name : str, records : list = []) -> str:
    context = {"table_catalog":table_object.table_catalog, "table_schema":table_object.table_schema, "table_name":table_object.table_name, "records":records}
    template = f"{jinja_template_name}.jinja"
    sql = _common.create_sql(template, context)
    return sql

def _create_update_converted_column_sql(table_object : DataObject, column_name : str, renamed_column_name: str,
                                        data_type_sql: str, jinja_template_name : str) -> str:
    context = {"table_catalog":table_object.table_catalog, "table_schema":table_object.table_schema, "table_name":table_object.table_name, 
               "column_name":column_name, "renamed_column_name":renamed_column_name, "data_type_sql": data_type_sql}
    template = f"{jinja_template_name}.jinja"
    sql = _common.create_sql(template, context)
    return sql

def _add_data_type_sql(records : list):
    return_records = []
    for record in records:
        data_type_sql = ""
        if record["data_type"].upper() == "DECIMAL":
            data_type_sql = f"{record['data_type'].upper()}({record['numeric_precision']},{record['numeric_scale']})"
        else:
            data_type_sql = record["data_type"].upper()
        record["data_type_sql"]=data_type_sql
        return_records.append(record)
    return return_records

def _return_key_columns_df(df : pandas.DataFrame, primary_key_columns : list) -> pandas.DataFrame:
    """This function is an implementation detail and should not be used externally."""
    #df["column_sql"] = numpy.where(df["source_data_type"] == "VARCHAR", f"TRIM({df["source_column_name"]})", df["source_column_name"])
    return_df = df[df['column_name'].isin(primary_key_columns) & (~df['column_name'].str.startswith('__pstage_', na=False))]
    return return_df

def _return_attribute_columns_only_df(df : pandas.DataFrame, primary_key_columns : list) -> pandas.DataFrame:
    """
    This function is an implementation detail and should not be used externally.
    Returns non key columns and non-metadata columns (attribute columns only).
    
    :param df: Description
    :type df: pandas.DataFrame
    :return: Description
    :rtype: DataFrame
    """
    #df["column_sql"] = numpy.where(df["source_data_type"] == "VARCHAR", f"TRIM({df["source_column_name"]})", df["source_column_name"])
    #df["column_md5_sql"] = numpy.where(df["source_data_type"] == "VARCHAR", f"IFNULL(TRIM({df["source_column_name"]}, '')", f"IFNULL(CAST({df["source_column_name"]} AS VARCHAR(128)), '')")
    return_df = df[(~df['column_name'].isin(primary_key_columns)) & (~df['column_name'].str.startswith('__pstage_', na=False))]
    return return_df

def _create_table_sql(table_object : DataObject, jinja_template_name : str, key_records : list, attribute_records : list = []) -> str:
    """This function is an implementation detail and should not be used externally."""
    
    if attribute_records == []:
        context = {"table_catalog":table_object.table_catalog, "table_schema":table_object.table_schema, "table_name":table_object.table_name,
               "key_records":key_records}
    else:
        context = {"table_catalog":table_object.table_catalog, "table_schema":table_object.table_schema, "table_name":table_object.table_name,
               "key_records":key_records, "attribute_records":attribute_records}
    template = f"{jinja_template_name}.jinja"
    sql = _common.create_sql(template, context)
    return sql

def _check_table_exists(session : Session, table_object : DataObject) -> bool:
    """This function is an implementation detail and should not be used externally."""
    return_value = True
    try:
        #we get a df back, but we don't use it...
        df = _common.return_columns_df(session, table_object)
    except:
        #the return columns df function throws an error if the table being searched for dos not exist...so an error means the table does not exist.
        return_value = False
    return return_value

def _check_landing_table_structure(session : Session, table_object : DataObject) -> bool:
    return_value = True
    try:
        #we get a df back, but we don't use it...
        _common.check_landing_table_structure(session, table_object)
    except:
        #the return columns df function throws an error if the table being searched for dos not exist...so an error means the table does not exist.
        return_value = False
    return return_value

def _fix_landing_table(session : Session, table_object : DataObject):
    #Parser Error: Adding columns with constraints not yet supported (so I had to do it this way).  site-packages (1.4.3) I guess....  Java connector does support this and the doc states it as well.
    #sql = f"ALTER TABLE {table_object.table_catalog}.{table_object.table_schema}.{table_object.table_name} ADD COLUMN __pstage_inserted_timestamp TIMESTAMP NOT NULL DEFAULT current_localtimestamp();"
    #workaround....
    sql = f"ALTER TABLE {table_object.table_catalog}.{table_object.table_schema}.{table_object.table_name} ADD COLUMN __pstage_inserted_timestamp TIMESTAMP;"
    _common.exec_ddl(session, sql)
    sql = f"ALTER TABLE {table_object.table_catalog}.{table_object.table_schema}.{table_object.table_name} ALTER COLUMN __pstage_inserted_timestamp SET DEFAULT current_localtimestamp();"
    _common.exec_ddl(session, sql)
    sql = f"ALTER TABLE {table_object.table_catalog}.{table_object.table_schema}.{table_object.table_name} ALTER COLUMN __pstage_inserted_timestamp SET NOT NULL;"
    _common.exec_ddl(session, sql)