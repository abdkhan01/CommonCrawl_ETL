#!python3
import mysql.connector
import logging
import os.path
from datetime import datetime
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
from config import config as cfg

def load_paths(paths=[]):
    """load path rows into DB

    Parameters
    ----------
    keywords : list
        list of paths from CommonCrawl file
    """

    if len(paths) == 0:
        logging.error("Emty list of keywords received")

    # Prepare values that need to be inserted into DB:
    sql = """INSERT INTO `""" + cfg.wat_path_table + """`
    (`id`, `path`, `created`, `status`)
    VALUES (NULL, %s, %s, %s)"""
    
    str_now = datetime.now().isoformat()

    # Prepare data for inserting into DB:
    insert_data = []
    for p in paths:
        temp_tuple = (p.strip(), str_now, "new")
        insert_data.append(temp_tuple)
    
    # Connect to DB:
    try:
        mydb = mysql.connector.connect(
            host=cfg.mysql_host,
            user=cfg.mysql_user,
            password=cfg.mysql_password,
            database=cfg.mysql_database
        )
    except Exception as e:
        logging.error("MySQL Connection could not be established: %s", e)

    try:
        mycursor = mydb.cursor(buffered=True, dictionary=True)
    except Exception:
        logging.error('MySQL Connection could not be established')

    # Insert data into DB:
    try:
        mycursor.executemany(sql, insert_data)
    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)
    mydb.commit()
    mycursor.close()
    mydb.close()


filename = 'trigger/wat.paths'
paths = list(open(filename, 'r'))
load_paths(paths)
