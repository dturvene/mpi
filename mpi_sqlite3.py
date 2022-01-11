#!/usr/bin/env python3
"""
Python3 sqlite3 usage example for Miller(s)

sqlite3 *should* be installed by default

References:
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
mydb = '/tmp/mpi.db'

def print_persons(persons):
    '''
    print given player list to stdout
    '''
    for person in persons:
        print_person(person)

def print_person(person):
        print(f'idx={person[0]}: lname={person[1]} fname={person[2]} age={person[3]}')

def ut_fn1(self):
    '''
    framework verificationa and diagnostic
    '''
    print('ver=', sys.version)

def ut_c_dbs(self):
    '''
    Create a new mpi db and populate with schemas and then data.  
    Finally commit the new records and close the database to flush the changes.

    Need to remove/delete mydb before running this....

    '''
    conn = sqlite3.connect(mydb)
    cur = conn.cursor()

    # create player table
    cur.execute('''CREATE TABLE player 
               (ID INT PRIMARY KEY,
                LNAME TEXT,
                FNAME TEXT, 
                AGE INTEGER)''')

    # insert players
    cur.execute("INSERT INTO player VALUES (1, 'Miller', 'Eric', 55)")
    cur.execute("INSERT INTO player VALUES (2, 'Miller', 'Nicky', 23)")
    cur.execute("INSERT INTO player VALUES (3, 'Stanley', 'Jay', 53)")
    cur.execute("INSERT INTO player VALUES (4, 'Stanley', 'Ben', 23)")

    # commit changes and close to flush them
    conn.commit()
    conn.close()

def ut_r_dbs(self):
    '''
    Connect to the MPI database and read the tables
    '''
    conn = sqlite3.connect(mydb)
    cur = conn.cursor()
    
    person_rows = cur.execute("SELECT * FROM player").fetchall()
    print_persons(person_rows)

    #for row in cur.execute("SELECT * FROM player"):
    #    print_person(row)
    
    conn.close()
    
class Ut(unittest.TestCase):
    def setUp(self):
        self.d1={'A':1, 'B':2, 'C':3}
    def tearDown(self):
        pass
    @unittest.skip('good')
    def test1(self):
        ut_fn1(self)
    #@unittest.skip('good')
    def test2(self):
        ut_c_dbs(self)
    # @unittest.skip('good')        
    def test3(self):
        ut_r_dbs(self)

if __name__ == '__main__':
    # reload in PDB
    # exec(open('sqlite3_miller.py').read())
    unittest.main(exit=False)
