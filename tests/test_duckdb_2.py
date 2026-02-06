"""
Docstring for tests.test_duckdb_2
Testing of load functionality of multiple batches in one landing AND changes to an existing row and de duplication confidence percentage calc for some intentional duplicates that 
cannot be resolved by the use of the timestamp or source system watermark columns.
1.  Create loan stage and loan land.  Load data into stage and run a single insert into loan__land to prep for checking that the watermark functionality works.
2.  Create the persisted staging tables current, hist, and cks from the landing table.  The landing table does have the needed metadata columns so it is not updated.
3.  Load the data including the data with will have de duplication confidence percentages that are off.
"""


import sys
import os
import unittest
import logging
import duckdb
#may not need this....
#from duckdb import DuckDBPyConnection

repo_path = os.path.split(sys.path[0])[0]
  
# Now add the various levels for search purposes
# adding src to path for testing
sys.path.insert(1, os.path.join(repo_path, 'src'))

#sys.path.insert(1, os.path.join(repo_path, 'src', 'topname'))

from perstageutil.duckdb import load, setup
from perstageutil.duckdb.session import Session

class TestDuckDBLoad(unittest.TestCase):

    def setUp(self):
        #set up the test database and create the tables.
        self.test_db_path = os.path.join(repo_path, 'tests', 'data', 'test_persisted_stage.duckdb')
        conn = duckdb.connect(self.test_db_path)
        sql = """
        CREATE or replace TABLE loan__land(loan_number VARCHAR,
        loan_amount DECIMAL(14, 2),
        loan_officer VARCHAR,
        create_timestamp TIMESTAMP,
        update_timestamp TIMESTAMP,
        __pstage_inserted_timestamp TIMESTAMP);
        """
        conn.execute(sql)

        sql = """
        CREATE OR REPLACE TABLE loan__stage(loan_number VARCHAR,
        loan_amount DECIMAL(14,2),
        loan_officer VARCHAR,
        create_timestamp TIMESTAMP,
        update_timestamp TIMESTAMP);
        """
        conn.execute(sql)

        sql = """
        INSERT INTO main.loan__stage
        (loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp)
        VALUES('1', 100.00, 'john smith', '1992-09-20 11:30:00.123456789'::TIMESTAMP, NULL),
        ('2', 110.00, NULL, '1992-09-20 11:31:00.123456789'::TIMESTAMP, '1992-09-23 11:30:00.000000000'::TIMESTAMP),
        ('3', 130.00, 'bob willis', '1992-09-20 11:32:00.123456789'::TIMESTAMP, '1992-09-24 11:30:00.000000000'::TIMESTAMP);
        """
        conn.execute(sql)

        sql = """
        --ADD updates and dupes.
        INSERT INTO main.loan__stage
        (loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp)
        VALUES('3', 130.00, 'bob willis', '1992-09-20 11:32:00.123456789'::TIMESTAMP, '1992-09-25 11:30:00.000000000'::TIMESTAMP),
        ('4', 130.00, 'Joe Strummer', '1992-09-20 11:32:00.123456789'::TIMESTAMP, NULL),
        ('4', 140.00, 'Joe Strummer', '1992-09-20 11:32:00.123456789'::TIMESTAMP, NULL),
        ('5', 140.00, 'Mick Jones', '1992-09-20 11:40:00.123456789'::TIMESTAMP, NULL),
        ('5', 140.00, 'Mick Jones', '1992-09-20 11:40:00.123456789'::TIMESTAMP, NULL),
        ('5', 145.00, 'Mick Jones', '1992-09-20 11:40:00.123456789'::TIMESTAMP, NULL);
        """
        conn.execute(sql)

        sql = """
        INSERT INTO main.loan__land
        (loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp, __pstage_inserted_timestamp)
        SELECT loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp, current_localtimestamp()
        FROM test_persisted_stage.main.loan__stage;
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
        sql = """DROP TABLE IF EXISTS loan__land;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS loan__stage;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS loan;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS loan__hist;"""
        conn.execute(sql)
        sql = """DROP TABLE IF EXISTS loan__cks;"""
        conn.execute(sql)
    
    def test_one(self):
        #setup logging config as one would when using the library
        print("Disconnect any other processes from the target duckdb database as connecting to it locks it.")
        logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d :: %(message)s', level = logging.DEBUG)
        logger = logging.getLogger("duckdb_ps_load")
        #test_db_path = os.path.join(repo_path, 'tests', 'data', 'test_persisted_stage.duckdb')
        #create a session.
        session = Session(self.test_db_path, logger)
        #run a persisted staging load
        setup.exec(session, "test_persisted_stage.main.loan__land", "test_persisted_stage.main.loan", "test_persisted_stage.main.loan__hist", "test_persisted_stage.main.loan__cks", ["loan_number"])
        load.exec(session, "test_persisted_stage.main.loan__land", "test_persisted_stage.main.loan", "test_persisted_stage.main.loan__hist", ["update_timestamp", "create_timestamp"])
        session.conn.sql("SELECT * FROM test_persisted_stage.main.loan order by loan_number, __pstage_inserted_timestamp;").show()
        session.conn.sql("SELECT * FROM test_persisted_stage.main.loan__hist order by loan_number, __pstage_effective_timestamp desc;").show()
        #close the sessions connection.
        session.close()

if __name__ == '__main__':
    unittest.main()