import logging
import sys
import os
# sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
# print(os.path.realpath(__file__),os.pardir)
from config import config as cfg
import requests
import tldextract
from . import wat_processor as wat_proc
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


class Processor:
    """
    This is a class for processing a file.

    Attributes:
        busy (bool): to specify if a process is running on a file.
        id (int): id of the file gettig processed
        path (str): s3 path of the wat file getting processed
        wat_proc (WatProcessor): object of WatProcessor
    """
    def __init__(self, busy):
        """
        The constructor for Processor class.

        Parameters:
           busy (bool): to specify if a process is running on a file.
        """
        self.busy = busy
        self.id = None
        self.path = None
        self.wat_proc = wat_proc.WatProcessor()

        logging.info("Processor Created")

    def set_busy(self, busy):
        """
        The function to sets the busy attribute.

        Parameters:
            busy (bool): to specify if a process is running on a file.
        """
        self.busy = busy

    def set_wat_file(self, id, path):
        """
        The function to sets the wat file

        Parameters:
            id (int): id of the file gettig processed
            path (str): s3 path of the wat file getting processed
        """
        if (self.id is None and self.path is None):
            self.id = id
            self.path = path
        else:
            logging.error("File already set")

    def download_wat_file(self):
        """
        The function to downloads the wat file and places it into download folder.
        """
        url = f"https://commoncrawl.s3.amazonaws.com/{self.path}"
        wat_file_path = self.path.split('/')[5]
        logging.info(wat_file_path)
        logging.info(f"wat file {self.path} downoading")
        r = requests.get(url)
        open(f'download/{wat_file_path}', 'wb').write(r.content)
        logging.info("Download complete!")

    def extract_wat_file(self):
        """
        calls the wat_proc.extract_wat_details() and extracts ip,domain and tld details.

        Returns:
            list (tuple): List of tuples with wat details (ip,domain,tld)
        """

        file_path = self.path.split('/')[5]
        logging.info(f"Extracting file: {file_path}")
        wat_file_path = 'download/' + file_path
        self.wat_proc.set_path(wat_file_path)
        return self.wat_proc.extract_wat_details()

    def remove_wat_file(self):
        """
        The function removes the current file in path from download folder
        """
        delete_file_path = 'download/' + self.path.split('/')[5]

        logging.info(f"deleting file: {delete_file_path}")
        if os.path.exists(delete_file_path):
            os.remove(delete_file_path)
        else:
            print("The file does not exist")

    def check_file_exist(self):
        """
        The function check if any file exists in the download folder and sets the process as busy if there is any file.
        """
        file_path = os.getcwd()+"\\download"

        if any(os.scandir(file_path)):
            self.busy = True
        else:
            self.busy = False
