"""
Docstring for tests.test_duckdb_1
Full load test of most of the base data types.
1.  First a landing table is created.
2.  Setup is then run and the needed columns are added to the landing table as they did not exist and the 3 persisted stage tables are created.
3.  A load is run to test the merge, insert, and updated processes.
4.  Some results are displayed.
5.  Teardown is done.
Not included in this test are LIST, STRUCT, and UNION types.  Those will be tested in another version.
Included in the test are 3 new record inserts and 1 record update.
"""

import sys
import os
import unittest
import logging
import duckdb

repo_path = os.path.split(sys.path[0])[0]
  
# Now add the various levels for search purposes
# adding src to path for testing
sys.path.insert(1, os.path.join(repo_path, 'src'))

from perstageutil.duckdb import load, setup
from perstageutil.duckdb.session import Session

class TestDuckDBLoad(unittest.TestCase):

    def setUp(self):
        #set up the test database and create the tables.
        self.test_db_path = os.path.join(repo_path, 'tests', 'data', 'test_persisted_stage.duckdb')
        conn = duckdb.connect(self.test_db_path)
        sql = """
        CREATE OR REPLACE TABLE test_all__land(id INTEGER null,
        column_1_bigint BIGINT,
        column_2_binary BLOB,
        column_3_bit BIT,
        column_4_bitstring BIT,
        column_5_blob BLOB,
        column_6_bool BOOLEAN,
        column_7_boolean BOOLEAN,
        column_8_bpchar VARCHAR,
        column_9_bytea BLOB,
        column_10_char VARCHAR,
        column_11_date DATE,
        column_12_datetime TIMESTAMP,
        column_13_decimal DECIMAL(18,4),
        column_14_double DOUBLE,
        column_15_float FLOAT,
        column_16_float4 FLOAT,
        column_17_float8 DOUBLE,
        column_18_hugeint HUGEINT,
        column_19_int INTEGER,
        column_20_int1 TINYINT,
        column_21_int2 SMALLINT,
        column_22_int4 INTEGER,
        column_23_int8 BIGINT,
        column_24_integer INTEGER,
        column_25_interval INTERVAL,
        column_26_json JSON,
        column_27_logical BOOLEAN,
        column_28_long BIGINT,
        column_29_numeric DECIMAL(18,4),
        column_30_real FLOAT,
        column_31_short SMALLINT,
        column_32_signed INTEGER,
        column_33_smallint SMALLINT,
        column_34_string VARCHAR,
        column_35_text VARCHAR,
        column_36_time TIME,
        column_37_timestamp TIMESTAMP,
        column_38_timestamp_with_time_zone TIMESTAMP WITH TIME ZONE,
        column_39_timestamptz TIMESTAMP WITH TIME ZONE,
        column_40_tinyint TINYINT,
        column_41_ubigint UBIGINT,
        column_42_uhugeint UHUGEINT,
        column_43_uinteger UINTEGER,
        column_44_usmallint USMALLINT,
        column_45_utinyint UTINYINT,
        column_46_uuid UUID,
        column_47_varbinary BLOB,
        column_48_varchar VARCHAR);

        """
        conn.execute(sql)
        conn.close()
    
    def tearDown(self):
        #we leave the test db alone for now...
        self.myteardown()
        #return super().tearDown()

    def myteardown(self):
        self.test_db_path = os.path.join(repo_path, 'tests', 'data', 'test_persisted_stage.duckdb')
        conn = duckdb.connect(self.test_db_path)
        sql = """DROP TABLE IF EXISTS test_all__land;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS test_all;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS test_all__land__hist;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS test_all__land__cks;"""
        conn.execute(sql)
    
    def test_1(self):

        #setup logging config as one would when using the library
        print("Disconnect any other processes from the target duckdb database as connecting to it locks it.")
        logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d :: %(message)s', level = logging.DEBUG)
        logger = logging.getLogger("duckdb_ps_load")
        #test_db_path = os.path.join(repo_path, 'tests', 'data', 'test_persisted_stage.duckdb')
        #create a session.
        session = Session(self.test_db_path, logger)
        #run a persisted staging load
        setup.exec(session, "test_persisted_stage.main.test_all__land", "test_persisted_stage.main.test_all", "test_persisted_stage.main.test_all__land__hist", "test_persisted_stage.main.test_all__land__cks", ["id"])
        #insert some data and run a load test.
        sql = r"""INSERT INTO test_persisted_stage.main.test_all__land BY NAME
        SELECT
        1 AS id,
        2**62::BIGINT AS column_1_bigint,
        '\xAA\xAB\xAC'::BLOB AS column_2_binary,
        '101010'::BITSTRING AS column_3_bit,
        '101010'::BITSTRING AS column_4_bitstring,
        '\xAA\xAB\xAC'::BLOB AS column_5_blob,
        true AS column_6_bool,
        false AS column_7_boolean,
        'some text as bpchar'::VARCHAR AS column_8_bpchar,
        '\xAA\xAB\xAC'::BLOB AS column_9_bytea,
        'some text as char'::VARCHAR AS column_10_char,
        '1992-09-20'::DATE AS column_11_date,
        '1992-09-20 11:30:00.123456789'::TIMESTAMP AS column_12_datetime,
        12345678901234.1234::DECIMAL(18,4) AS column_13_decimal,
        231.12::DOUBLE AS column_14_double,
        123.11::FLOAT AS column_15_float,
        1234.123::FLOAT AS column_16_float4,
        12234.11::DOUBLE AS column_17_float8,
        170141183460469230000000000000000000000::HUGEINT AS column_18_hugeint,
        1234::INT AS column_19_int,
        (2**6)::TINYINT AS column_20_int1,
        (2^14)::SMALLINT AS column_21_int2,
        (2^30)::INTEGER AS column_22_int4,
        9223372036854775807::BIGINT AS column_23_int8, 
        (2^30)::INTEGER AS column_24_integer,
        INTERVAL '1.5' YEARS AS column_25_interval,
        to_json('{"the_key":1, "the_value":"value 1"') AS column_26_json,
        true AS column_27_logical,
        (2^62)::BIGINT AS column_28_long,
        12345678901234.1234::DECIMAL(18,4) AS column_29_numeric,
        123.11::FLOAT AS column_30_real,
        (2^14)::SMALLINT AS column_31_short,
        (2^30)::INTEGER AS column_32_signed,
        (2^14)::SMALLINT  AS column_33_smallint,
        'my string' AS column_34_string,
        'my string' AS column_35_text,
        '11:30:00.123456'::time AS column_36_time,
        '1992-09-20 11:30:00.123456789'::TIMESTAMP AS column_37_timestamp,
        '1992-09-20 11:30:00.123456789'::TIMESTAMPTZ AS column_38_timestamp_with_time_zone,
        '1992-09-20 11:30:00.123456789'::TIMESTAMPTZ AS column_39_timestamptz,
        (2^6)::TINYINT AS column_40_tinyint,
        (2^63)::UBIGINT AS column_41_ubigint,
        (2^127)::UHUGEINT AS column_42_uhugeint,
        (2^30) AS column_43_uinteger,
        (2^14) AS column_44_usmallint,
        (2^6) AS column_45_utinyint,
        uuid() AS column_46_uuid,
        '\xAA\xAB\xAC'::BLOB AS column_47_varbinary,
        'my string'  AS column_48_varchar;"""
        session.conn.execute(sql)

        sql = r"""INSERT INTO test_persisted_stage.main.test_all__land BY NAME
        SELECT
        2 AS id,
        2**62::BIGINT AS column_1_bigint,
        '\xAA\xAB\xAC'::BLOB AS column_2_binary,
        '101010'::BITSTRING AS column_3_bit,
        '101010'::BITSTRING AS column_4_bitstring,
        '\xAA\xAB\xAC'::BLOB AS column_5_blob,
        true AS column_6_bool,
        false AS column_7_boolean,
        'some text as bpchar'::VARCHAR AS column_8_bpchar,
        '\xAA\xAB\xAC'::BLOB AS column_9_bytea,
        'some text as char'::VARCHAR AS column_10_char,
        '1992-09-20'::DATE AS column_11_date,
        '1992-09-20 11:30:00.123456789'::TIMESTAMP AS column_12_datetime,
        12345678901234.1234::DECIMAL(18,4) AS column_13_decimal,
        231.12::DOUBLE AS column_14_double,
        123.11::FLOAT AS column_15_float,
        1234.123::FLOAT AS column_16_float4,
        12234.11::DOUBLE AS column_17_float8,
        170141183460469230000000000000000000000::HUGEINT AS column_18_hugeint,
        1234::INT AS column_19_int,
        (2**6)::TINYINT AS column_20_int1,
        (2^14)::SMALLINT AS column_21_int2,
        (2^30)::INTEGER AS column_22_int4,
        9223372036854775807::BIGINT AS column_23_int8, 
        (2^30)::INTEGER AS column_24_integer,
        INTERVAL '1.5' YEARS AS column_25_interval,
        to_json('{"the_key":1, "the_value":"value 1"') AS column_26_json,
        true AS column_27_logical,
        (2^62)::BIGINT AS column_28_long,
        12345678901234.1234::DECIMAL(18,4) AS column_29_numeric,
        123.11::FLOAT AS column_30_real,
        (2^14)::SMALLINT AS column_31_short,
        (2^30)::INTEGER AS column_32_signed,
        (2^14)::SMALLINT  AS column_33_smallint,
        'my string' AS column_34_string,
        'my string' AS column_35_text,
        '11:30:00.123456'::time AS column_36_time,
        '1992-09-20 11:30:00.123456789'::TIMESTAMP AS column_37_timestamp,
        '1992-09-20 11:30:00.123456789'::TIMESTAMPTZ AS column_38_timestamp_with_time_zone,
        '1992-09-20 11:30:00.123456789'::TIMESTAMPTZ AS column_39_timestamptz,
        (2^6)::TINYINT AS column_40_tinyint,
        (2^63)::UBIGINT AS column_41_ubigint,
        (2^127)::UHUGEINT AS column_42_uhugeint,
        (2^30) AS column_43_uinteger,
        (2^14) AS column_44_usmallint,
        (2^6) AS column_45_utinyint,
        uuid() AS column_46_uuid,
        '\xAA\xAB\xAC'::BLOB AS column_47_varbinary,
        'my string'  AS column_48_varchar;"""
        session.conn.execute(sql)

        sql = r"""INSERT INTO test_persisted_stage.main.test_all__land BY NAME
        SELECT
        3 AS id,
        2**62::BIGINT AS column_1_bigint,
        '\xAA\xAB\xAC'::BLOB AS column_2_binary,
        '101010'::BITSTRING AS column_3_bit,
        '101010'::BITSTRING AS column_4_bitstring,
        '\xAA\xAB\xAC'::BLOB AS column_5_blob,
        true AS column_6_bool,
        false AS column_7_boolean,
        'some text as bpchar'::VARCHAR AS column_8_bpchar,
        '\xAA\xAB\xAC'::BLOB AS column_9_bytea,
        'some text as char'::VARCHAR AS column_10_char,
        '1992-09-20'::DATE AS column_11_date,
        '1992-09-20 11:30:00.123456789'::TIMESTAMP AS column_12_datetime,
        12345678901234.1234::DECIMAL(18,4) AS column_13_decimal,
        231.12::DOUBLE AS column_14_double,
        123.11::FLOAT AS column_15_float,
        1234.123::FLOAT AS column_16_float4,
        12234.11::DOUBLE AS column_17_float8,
        170141183460469230000000000000000000000::HUGEINT AS column_18_hugeint,
        1234::INT AS column_19_int,
        (2**6)::TINYINT AS column_20_int1,
        (2^14)::SMALLINT AS column_21_int2,
        (2^30)::INTEGER AS column_22_int4,
        9223372036854775807::BIGINT AS column_23_int8, 
        (2^30)::INTEGER AS column_24_integer,
        INTERVAL '1.5' YEARS AS column_25_interval,
        to_json('{"the_key":1, "the_value":"value 1"') AS column_26_json,
        true AS column_27_logical,
        (2^62)::BIGINT AS column_28_long,
        12345678901234.1234::DECIMAL(18,4) AS column_29_numeric,
        123.11::FLOAT AS column_30_real,
        (2^14)::SMALLINT AS column_31_short,
        (2^30)::INTEGER AS column_32_signed,
        (2^14)::SMALLINT  AS column_33_smallint,
        'my string' AS column_34_string,
        'my string' AS column_35_text,
        '11:30:00.123456'::time AS column_36_time,
        '1992-09-20 11:30:00.123456789'::TIMESTAMP AS column_37_timestamp,
        '1992-09-20 11:30:00.123456789'::TIMESTAMPTZ AS column_38_timestamp_with_time_zone,
        '1992-09-20 11:30:00.123456789'::TIMESTAMPTZ AS column_39_timestamptz,
        (2^6)::TINYINT AS column_40_tinyint,
        (2^63)::UBIGINT AS column_41_ubigint,
        (2^127)::UHUGEINT AS column_42_uhugeint,
        (2^30) AS column_43_uinteger,
        (2^14) AS column_44_usmallint,
        (2^6) AS column_45_utinyint,
        uuid() AS column_46_uuid,
        '\xAA\xAB\xAC'::BLOB AS column_47_varbinary,
        'my string'  AS column_48_varchar;"""
        session.conn.execute(sql)

        sql = r"""INSERT INTO test_persisted_stage.main.test_all__land BY NAME
        SELECT
        3 AS id,
        2**62::BIGINT AS column_1_bigint,
        '\xAA\xAB\xAC'::BLOB AS column_2_binary,
        '101010'::BITSTRING AS column_3_bit,
        '101010'::BITSTRING AS column_4_bitstring,
        '\xAA\xAB\xAC'::BLOB AS column_5_blob,
        true AS column_6_bool,
        false AS column_7_boolean,
        'some text as bpchar'::VARCHAR AS column_8_bpchar,
        '\xAA\xAB\xAC'::BLOB AS column_9_bytea,
        'some text as char'::VARCHAR AS column_10_char,
        '1992-09-20'::DATE AS column_11_date,
        '1992-09-20 11:30:00.123456789'::TIMESTAMP AS column_12_datetime,
        12345678901234.1234::DECIMAL(18,4) AS column_13_decimal,
        231.12::DOUBLE AS column_14_double,
        123.11::FLOAT AS column_15_float,
        1234.123::FLOAT AS column_16_float4,
        12234.11::DOUBLE AS column_17_float8,
        170141183460469230000000000000000000000::HUGEINT AS column_18_hugeint,
        1234::INT AS column_19_int,
        (2**6)::TINYINT AS column_20_int1,
        (2^14)::SMALLINT AS column_21_int2,
        (2^30)::INTEGER AS column_22_int4,
        9223372036854775807::BIGINT AS column_23_int8, 
        (2^30)::INTEGER AS column_24_integer,
        INTERVAL '1.5' YEARS AS column_25_interval,
        to_json('{"the_key":1, "the_value":"value 1"') AS column_26_json,
        true AS column_27_logical,
        (2^62)::BIGINT AS column_28_long,
        12345678901234.1234::DECIMAL(18,4) AS column_29_numeric,
        123.11::FLOAT AS column_30_real,
        (2^14)::SMALLINT AS column_31_short,
        (2^30)::INTEGER AS column_32_signed,
        (2^14)::SMALLINT  AS column_33_smallint,
        'my string' AS column_34_string,
        'my string' AS column_35_text,
        '11:30:00.123456'::time AS column_36_time,
        '1992-09-20 11:30:00.123456789'::TIMESTAMP AS column_37_timestamp,
        '1992-09-20 11:30:00.123456789'::TIMESTAMPTZ AS column_38_timestamp_with_time_zone,
        '1992-09-20 11:30:00.123456789'::TIMESTAMPTZ AS column_39_timestamptz,
        (2^6)::TINYINT AS column_40_tinyint,
        (2^63)::UBIGINT AS column_41_ubigint,
        (2^127)::UHUGEINT AS column_42_uhugeint,
        (2^30) AS column_43_uinteger,
        (2^14) AS column_44_usmallint,
        (2^6) AS column_45_utinyint,
        uuid() AS column_46_uuid,
        '\xAA\xAB\xAC'::BLOB AS column_47_varbinary,
        'my string gets changed'  AS column_48_varchar;"""
        session.conn.execute(sql)

        load.exec(session, "test_persisted_stage.main.test_all__land", "test_persisted_stage.main.test_all", "test_persisted_stage.main.test_all__land__hist")
        session.conn.sql("SELECT id, column_46_uuid, column_9_bytea, column_48_varchar, __pstage_effective_timestamp, __pstage_expiration_timestamp FROM test_persisted_stage.main.test_all__land__hist").show()
        session.conn.sql("SELECT id, column_18_hugeint, column_37_timestamp FROM test_persisted_stage.main.test_all").show()
        session.conn.sql("SELECT id, column_1_bigint, column_25_interval, column_37_timestamp FROM test_persisted_stage.main.test_all__land__hist").show()
        #close the sessions connection.
        session.close()


if __name__ == '__main__':
    unittest.main()