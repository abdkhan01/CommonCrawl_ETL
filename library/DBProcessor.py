import logging
# sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
import mysql.connector
from config import config as cfg
from datetime import datetime

today = datetime.now()  # current date and time
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")
logname = "log/" + year + "-" + month + "-" + day + "-" + "KWExtractor.log"
logging.basicConfig(filename=logname,
                    filemode='a',
                    format='%(asctime)s:%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
# logging.getLogger().addHandler(logging.StreamHandler())

"""
Database related functios abstraction
"""


class DBProcessor:
    """
    This is a class for database operations.

    Attributes:
        mydb (sqlconnecter): connector to db
    """

    def __init__(self):
        try:
            self.mydb = mysql.connector.connect(
                host=cfg.mysql_host,
                user=cfg.mysql_user,
                password=cfg.mysql_password,
                database=cfg.mysql_database
            )
        except Exception as e:
            logging.error("MySQL Connection could not be established: %s", e)

    def set_processor(self, processor):
        """
        This function sets the processor object
        """
        self.processor = processor

    def update_wat_file_status(self, status):
        """
        update file the status in db specified for the id of the process

        Parameters:
            status (string) : status of the file
        """
        sql = f"""UPDATE {cfg.wat_path_table} SET status = '{status}' WHERE id = '{self.processor.id}'"""
        # print(sql)
        try:
            mycursor = self.mydb.cursor(buffered=True, dictionary=True)
        except Exception:
            logging.error('MySQL Connection could not be established')

        # Insert data into DB:
        try:
            mycursor.execute(sql)
        except mysql.connector.Error as err:
            logging.error(err)
            logging.error("Error Code:", err.errno)
            logging.error("SQLSTATE", err.sqlstate)
            logging.error("Message", err.msg)

        self.mydb.commit()
        logging.info(str(mycursor.rowcount) + " record(s) affected")

    def get_new_file_from_db(self, status):
        """
        get file from database with the status specified

        Parameters:
            status (string): status of the file
        """
        sql = f"""select id,path from {cfg.wat_path_table} where status='{status}' LIMIT 1"""

        try:
            mycursor = self.mydb.cursor(buffered=True, dictionary=True)
        except Exception:
            logging.error('MySQL Connection could not be established')

        # Insert data into DB:
        try:
            mycursor.execute(sql)
        except mysql.connector.Error as err:
            logging.error(err)
            logging.error("Error Code:", err.errno)
            logging.error("SQLSTATE", err.sqlstate)
            logging.error("Message", err.msg)

        to_download = mycursor.fetchall()

        if to_download is not None:
            return to_download[0]
        else:
            logging.error("New files not found!")

    def get_next_wat_file_to_download(self):
        """
        get path of the wat file with the status "new" to download from database

        Returns:
            Dictionary: dictionary of id and path
        """
        return self.get_new_file_from_db("new")

    def get_next_wat_file_to_process(self):
        """
        get path of the wat file with the status "downloaded" to extract from database

        Returns:
            Dictionary: dictionary of id and path
        """
        return self.get_new_file_from_db("downloaded")

    def bulk_insert_ignore(self, wat_details):
        """
        inserts wat data into domains table
        """
        if len(wat_details) == 0:
            logging.error("Empty list of wat data received")

        try:
            mycursor = self.mydb.cursor(buffered=True, dictionary=True)
        except Exception:
            logging.error('MySQL Connection could not be established')

        # Insert data into DB:
        logging.info(f"inserting {len(wat_details)} records")

        for wat in wat_details:
            try:
                domain_table_identifier = wat[1][0] #decodes a byte stringand gets the first character
                sql = f"""INSERT IGNORE INTO {cfg.domain_db_table}{domain_table_identifier} (ip, domain, tld) VALUES (%s, %s, %s)"""
                mycursor.execute(sql, wat)
                # logging.info(sql)
            except mysql.connector.Error as err:
                logging.info(f"Table: {cfg.domain_db_table}{domain_table_identifier} " + wat[1] + " " + wat[2])
                logging.error(err)
                logging.error("Error Code:" + str(err.errno))
                logging.error("SQLSTATE" + str(err.sqlstate))
                logging.error("Message" + err.msg)

        logging.info("Record(s) added into domain tables")

        self.mydb.commit()

        mycursor.close()
