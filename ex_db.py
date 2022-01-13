#!/usr/bin/env python3
"""
Python3 sqlite3 usage examples

sqlite3 *should* be installed by default

References:
* https://github.com/dturvene/mpi
* https://docs.python.org/3/library/sqlite3.html
* https://www.digitalocean.com/community/tutorials/how-to-use-the-sqlite3-module-in-python-3

"""

__author__ = 'Dave Turvene'
__email__  = 'dturvene at dahetral.com'
__copyright__ = 'Copyright (c) 2022 Dahetral Systems'
__date__ = '20220110'
__version__ = '0.3'

import sys
import sqlite3
# python unittest framework to verify tests are successful
import unittest
# python source debugger (PBD) for deep debug
from pdb import set_trace as bp
import re

# I'm running linux and the /tmp directory is a ramdisk, not persistent 
# over reboots
MYDB = '/tmp/ex_db.db'

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

        self.schemas = self.get_table_schemas()

    def __del__(self):
        '''
        flush and close the db.
        Warning: for an inmemory DB this will destroy it!
        '''
        print('Commit/Close db={}'.format(self.dbname))
        self.con.commit()
        self.con.close()

    def getcon(self):
        return self.con
        
    def getcur(self):
        return self.cur

    def get_table(self, tabname):
        if self.tables[tabname]:
            return self.tables[tabname];
    
    def get_table_schemas(self):
        '''
        Get schema for all tables in DB
        '''
        tbldict = {}
        self.getcur().execute("select tbl_name from sqlite_master \
                                            where type='table' order by tbl_name")
        for tbl in self.getcur().fetchall():

            tblname = tbl[0];
            self.getcur().execute('pragma table_info("%s")' % tblname)
            flds = self.getcur().fetchall()
            tbldict[tblname] = flds
            
        return tbldict

    def get_schema(self, tabname):
        '''
        '''
        return self.tables[tabname]

class tab_person(object):
    '''
    person table class
    '''
    sql_insert = "INSERT INTO person (lname, fname, age, sch_id) VALUES "

    def __init__(self, mgr):
        '''
        create/populate person table if it does not already exist
        '''
        cur = mgr.getcur()
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
            print(f'Created person table in {MYDB}')
        except Exception as e:
            print(f'failed:database={MYDB} create table cause={str(e)}')

    def __del__(self):
        pass

    def dump_recs_all(self, school_dict):
        '''
        display all person records
        replace school id with school name using the school_dict
        '''
        cur = mgr.getcur()
        persons = cur.execute("SELECT * FROM person").fetchall()
        for person in persons:
            # print person records, using school_dict to convert school id to a school name
            print('person idx={}: lname={} fname={} age={} school={}'
                  .format(person[0],
                          person[1],
                          person[2],
                          person[3],
                          school_dict[person[4]])
            )
        # debug: bp()

    def convert_tuple(self, raw_tup):
        '''
        convert a tuple of lname, fname, age, school idx
        to a string
        '''
        if not isinstance(raw_tup, tuple):
            print("{} not a tuple".format(raw_tup))
            return ''

        sqlparams = '('
        for fld in raw_tup:
            if isinstance(fld, str):
                sqlparams += "'" + fld + "',"
            if isinstance(fld, int):
                sqlparams += str(fld) + ","

        # remove final comma and add a paren to end the parameters
        # sqlparams = sqlparams[:-1] + ')'
        sqlparams = re.sub(',$', ')', sqlparams)

        return sqlparams
        
    def rec_insert(self, person_params):
        '''
        give raw record as a string
        should be a tuple but more logic to convert
        check if record already exists
        '''

        sql_cmd = tab_person.sql_insert + person_params
        
        # insert into table
        mgr.getcur().execute(sql_cmd)

class tab_school(object):
    '''
    '''
    sql_insert = "INSERT INTO school VALUES "
    
    def __init__(self, mgr):
        '''
        create/populate person table if it does not already exist
        '''
        cur = mgr.getcur()
        # create/populate school table if it does not already exist
        try: 
            cur.execute(
                '''CREATE TABLE school (
                id INTEGER PRIMARY KEY,
                NAME TEXT
                )'''
            )
            print(f'Created school table in {MYDB}')
        except Exception as e:
            print(f'failed:database={MYDB} create table cause={str(e)}')

    def __del__(self):
        pass

    def get_dict(self):
        '''
        return all school records as a dictionary of {schid, name}
        '''
        schools = mgr.getcur().execute("SELECT * FROM school").fetchall()
        
        # convert schools records to a dictionary(id, name)
        school_dict = dict(schools)

        return school_dict
    
    def dump_recs_all(self):
        '''display all school records'''
        cur = mgr.getcur()
        schools = cur.execute("SELECT * FROM school").fetchall()
        for school in schools:
            print('school idx={}: name={}'
                  .format(school[0],
                          school[1]))

    def convert_tuple(self, raw_tup):
        '''
        convert a tuple of idx, school name
        to a string
        '''
        if not isinstance(raw_tup, tuple):
            print("{} not a tuple".format(raw_tup))
            return ''

        sqlparams = '('
        for fld in raw_tup:
            if isinstance(fld, str):
                sqlparams += "'" + fld + "',"
            if isinstance(fld, int):
                sqlparams += str(fld) + ","

        # remove final comma and add a paren to end the parameters
        # sqlparams = sqlparams[:-1] + ')'
        sqlparams = re.sub(',$', ')', sqlparams)

        return sqlparams
        
    def rec_insert(self, school_params):
        '''
        give raw record as a string
        use convert_tuple to make the string
        '''

        sql_cmd = tab_school.sql_insert + school_params
        
        # insert into table
        try:
            mgr.getcur().execute(sql_cmd)
        except sqlite3.IntegrityError as e:
            print(f'failed: {sql_cmd} cause={str(e)}')
            
# global database context
# this is always called, in methods define it as 'global mgr'
mgr=None

def open_db():
    '''
    Once the database is created and populated with table schemas
    add the known tables into the mgr table dict
    '''
    global mgr

    mgr = Sqldb(dbname=MYDB)    
    mgr.tables['person'] = tab_person(mgr)
    mgr.tables['school'] = tab_school(mgr)

    # if anything is created/modified, commit to disk
    mgr.getcon().commit()

def ut_read_tabs(self):
    '''
    Read and display all the tables in the database
    '''
    global mgr

    mgr.get_table('school').dump_recs_all()

    school_dict = mgr.get_table('school').get_dict()

    mgr.get_table('person').dump_recs_all(school_dict)
    

def ut_add_persons(self):
    '''
    add new person records and display to stdio
    '''

    global mgr

    persons = [
        # insert players
        ('Miller', 'Eric', 55, 1),
        ('Miller', 'Nicky', 23, 2),
        ('Stanley', 'Jay', 53, 3),
        ('Stanley', 'Ben', 23, 2),
    ]

    perstab = mgr.get_table('person')
    for person in persons:
        sql_person = perstab.convert_tuple(person)
        perstab.rec_insert(sql_person)

    # save changes
    mgr.getcon().commit()

def ut_add_schools(self):

    global mgr

    schools = [
        # insert schools        
        (1, 'Lehigh'),
        (2, 'W-L HS'),
        (3, 'Williams')
    ]

    schooltab = mgr.get_table('school')
    for school in schools:
        sql_school = schooltab.convert_tuple(school)
        schooltab.rec_insert(sql_school)
        
    # save changes
    mgr.getcon().commit()
        
    
class Ut(unittest.TestCase):
    def setUp(self):
        '''
        open database, if it does not exist will create it
        otherwise it will report tables exist

        This is called for each test!
        '''
        open_db()

    def tearDown(self):
        pass
    
    @unittest.skip('good')    
    def test01(self):
        pass

    @unittest.skip('good')
    def test03(self):
        '''
        simple person table insert tests
        '''
        perstab = mgr.get_table('person')
        
        person_str = "('Lname', 'Fname', 18, 1)"
        perstab.rec_insert(person_str)

        person_str = "(\'Lname\', \'Fname\', 19, 2)"
        perstab.rec_insert(person_str)

        # format used in tab_person.convert_tuple
        person_str = '(\'Lname\', \'Fname\', 19, 2)'
        perstab.rec_insert(person_str)

    # @unittest.skip('good')
    def test05(self):
        ut_add_persons(self)

    # @unittest.skip('good')        
    def test06(self):
        ut_add_schools(self)

    # @unittest.skip('good')
    def testLast(self):
        '''display state of current database'''
        ut_read_tabs(self)

if __name__ == '__main__':
    # reload in PDB when making mods interactively
    # exec(open('ex_db.py').read())
    print('ver=', sys.version)
    
    unittest.main(exit=False)
