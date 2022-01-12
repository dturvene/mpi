#!/usr/bin/env python3
"""
Python3 sqlite3 usage examples

sqlite3 *should* be installed by default

References:
* 
* https://docs.python.org/3/library/sqlite3.html
* https://www.digitalocean.com/community/tutorials/how-to-use-the-sqlite3-module-in-python-3

"""

__author__ = 'Dave Turvene'
__email__  = 'dturvene at dahetral.com'
__copyright__ = 'Copyright (c) 2022 Dahetral Systems'
__date__ = '20220110'
__version__ = '0.1'

import sys
import sqlite3
# python unittest framework to verify tests are successful
import unittest
# python source debugger (PBD) for deep debug
from pdb import set_trace as bp

# I'm running linux and the /tmp directory is a ramdisk, not persistent 
# over reboots
mydb = '/tmp/ex_db.db'

class Sqldb(object):
    '''
    Create and manage sqlite3 tables
    '''
    def __init__(self, dbname=":memory:"):
        # define an empty tables dictionary
        self.tables = {}

        self.dbname = dbname

        self.con = sqlite3.connect(self.dbname,
                                   timeout=10.0,
                                   detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.cur = self.con.cursor()

    def __del__(self):
        '''
        flush and close the db.
        Warning: for an inmemory DB this will destroy it!
        '''
        print('Commit/Close db={}'.format(self.dbname))
        self.con.commit()
        self.con.close()

    def get_con(self):
        return self.con
        
    def get_cur(self):
        return self.cur

class tab_person(object):
    '''
    person table class
    '''
    insert = "INSERT INTO person (lname, fname, age, sch_id) VALUES "

    def __init__(self, mgr):
        '''
        create/populate person table if it does not already exist
        '''
        cur = mgr.get_cur()
        try:
            # https://www.sqlite.org/autoinc.html
            cur.execute('''CREATE TABLE person 
              (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              lname TEXT,
              fname TEXT, 
              age INTEGER,
              sch_id INTEGER
              )'''
            )
            # insert players
            cur.execute(tab_person.insert + "('Miller', 'Eric', 55, 1)")
            cur.execute(tab_person.insert + "('Miller', 'Nicky', 23, 2)")
            cur.execute(tab_person.insert + "('Stanley', 'Jay', 53, 3)")
            cur.execute(tab_person.insert + "('Stanley', 'Ben', 23, 2)")

            print(f'Created person table in {mydb}')
        except Exception as e:
            print(f'failed:database={mydb} create table cause={str(e)}')

        def __del__(self):
            pass    

class tab_school(object):
    '''
    '''
    insert = "INSERT INTO school VALUES "
    
    def __init__(self, mgr):
        '''
        create/populate person table if it does not already exist
        '''
        cur = mgr.get_cur()
        # create/populate school table if it does not already exist
        try: 
            cur.execute(
                '''CREATE TABLE school (
                id INTEGER PRIMARY KEY,
                NAME TEXT
                )'''
            )

            # insert schools
            cur.execute(tab_school.insert + "(1, 'Lehigh')")
            cur.execute(tab_school.insert + "(2, 'W-L HS')")
            cur.execute(tab_school.insert + "(3, 'Williams')")        

            print(f'Created school table in {mydb}')
        except Exception as e:
            print(f'failed:database={mydb} create table cause={str(e)}')

        def __del__(self):
            pass    
            
# global database context
mgr=Sqldb(dbname=mydb)
def create_db():
    '''
    Create a new sqlite3 db and populate with schemas and then data.  
    Finally commit the new records and close the database to flush the changes.
    '''

    # Open persistent DB and return context manager class
    global mgr
    #mgr = Sqldb(dbname=mydb)

    tab_person(mgr)
    tab_school(mgr)

    # commit new tables
    mgr.get_con().commit()

def ut_r_dbs(self):
    '''
    Connect to the MPI database and read the tables
    '''
    bp()
    
    cur = mgr.get_cur()

    # get all school records
    schools = cur.execute("SELECT * FROM school").fetchall()

    # convert schools records to a dictionary(id, name)
    school_dict = dict(schools)

    # get all person records
    persons = cur.execute("SELECT * FROM person").fetchall()
    for person in persons:
        # print person records, converting the school id to a school name
        print('person idx={}: lname={} fname={} age={} school={}'
              .format(person[0],
                      person[1],
                      person[2],
                      person[3],
                      school_dict[person[4]])
                      )
        # bp()
        
class Ut(unittest.TestCase):
    def setUp(self):
        '''setup called twice!'''
        pass
    def tearDown(self):
        pass
    def test1(self):
        '''
        create the database and tables
        if already exist then catch exception
        '''
        create_db()

    # @unittest.skip('good')
    def test3(self):
        ut_r_dbs(self)

if __name__ == '__main__':
    # reload in PDB
    # exec(open('sqlite3_miller.py').read())
    print('ver=', sys.version)
    
    unittest.main(exit=False)
