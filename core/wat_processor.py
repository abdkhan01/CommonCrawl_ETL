import logging
from warcio.archiveiterator import ArchiveIterator
from pprint import pprint
import sys
import os
import json
import tldextract
import re
from datetime import datetime
import pandas as pd
# sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
import idna

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


class WatProcessor:
    """
    This is a class for wat extraction process.

    Attributes:
        path (str): s3 path of file to extract details from
    """

    def __init__(self):
        pass

    def set_path(self, path):
        """
        This funcation sets the path to the file with the status "downloaded"
        """
        self.path = path

    def remove_duplicates(self, wat_details):
        """
        utility function to remove the duplicates from a list of tuples
        """
        return list(set([i for i in wat_details]))

    def extract_wat_details(self):
        """
        This is a funcation for wat extraction process.

        Returns:
            List of tuples: list of unique tuples to insert in the database
        """
        wat_file_path = self.path
        logging.info("Extracting WAT file: " + wat_file_path)
        with open(wat_file_path, 'rb') as stream:
            i = 0
            insert_data = []
            for record in ArchiveIterator(stream):
                i += 1
                # if i == 20:
                #     # For testing only
                #     break
                if record.rec_type == 'metadata':
                    my_json = record.content_stream().read().decode('utf8')  # .replace("'", '"')
                    try:
                        metadata = json.loads(my_json)
                        urls = \
                            metadata['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Head'][
                                'Link']

                        try:
                            ip = metadata['Envelope']['WARC-Header-Metadata']['WARC-IP-Address']

                            for link in urls:
                                url = link['url']
                                url = url.replace("\\", "")
                                tld = tldextract.extract(url)

                                if ((tld.subdomain == '' and tld.domain == '') or
                                        (tld.domain == '' and tld.suffix == '') or
                                        (tld.subdomain == '' and tld.suffix == '') or
                                        (tld.suffix == '')):  # checking for invalid url link
                                    continue

                                list_tuple = list(tld)

                                try:
                                    list_tuple[1] = idna.encode(list_tuple[1]).decode() #encoding domain into puny code
                                    # print(list_tuple[1])
                                except:
                                    # print(list_tuple[1])
                                    pass

                                try:
                                    list_tuple[2] = idna.encode(list_tuple[2]).decode() #encoding suffix into puny code
                                    # print(list_tuple[2])
                                except:
                                    # print(list_tuple[2])
                                    pass

                                url = '.'.join(list_tuple)

                                url_final = re.sub(r"ww(w){0,1}(-){0,1}\d{1,5}\.", "", url.lower())
                                # url_final = re.sub(r"^[!-/:-@[-`{-~]+", "", url_final)
                                url_final = url_final.replace('www.', '')

                                # if url_final.startswith("."):  # checking for empty subdomains
                                #     # print(url_final, tld.suffix,tld)
                                #     url_final = url_final[1:]

                                if re.match("^[a-z0-9]+", url_final):
                                    temp_tuple = (ip, url_final, list_tuple[2])
                                    insert_data.append(temp_tuple)

                        except:
                            pass
                    except:
                        pass

                    pass
            logging.info(f"Extracted WAT file successfully. {len(insert_data)} records found!")
            insert_data = self.remove_duplicates(insert_data)
            logging.info(f"Duplicates removed. Total {len(insert_data)} records found!")
            print(len(insert_data))
            return insert_data

# proc = WatProcessor()
# proc.set_path("/Users/abdullahkhan/PycharmProjects/AnnachenETL/download/CC-MAIN-20210612103920-20210612133920-00013.warc.wat.gz")
# data = proc.extract_wat_details()
# df = pd.DataFrame(data, columns =['IP', 'URL', 'Suffix'])
# df.to_csv("URLS.csv")

#×§× ××‘×™×¡.com
# s = 'google.com'
# print(re.sub(r"^,+", "", s))
#
# print((ree.match("^[a-z0-9]+",s)))
# print(idna.encode(s,uts46=True))
#
# s = 'xn--eckzax5bza8b6eyera6fte'
# # print(s.decode())
# print(idna.decode(s))
