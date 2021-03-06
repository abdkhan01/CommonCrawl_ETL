# About

Common Crawl builds and maintains an open repository of web crawl data that can be accessed and analyzed by anyone. This project downloads and processes a month worth of data in batches using cron jobs and process locks and finally loads the data into MySql.

# Architecture

main file:
* To be executed by cron every 5 minutes
* So it's important to carry a status in the DB for the next step
* Further info:
* https://commoncrawl.org/2021/06/june-2021-crawl-archive-now-available/
* https://pypi.org/project/tldextract/
* https://pypi.org/project/warcio/

The goal is to create a software which:
1. Reads the file
2. Extracts data
3. Writes data to a MySQL

# domaindb.py

Check the current status / environment
if file exists: busy.lock
	sys.exit
if not busy, Processor starts to download the next file
	create file: busy.lock

	dl = DBProcessor.get_next_wat_file_to_download()

	if dl is not None:
	    //changed because need the id of file initally for changing status
	    Processor.set_wat_file()
		DBProcessor.update_wat_file_status('downloading')
		Processor.download_wat_file()
		DBProcessor.update_wat_file_status('downloaded')
		sys.exit


	toprocess = DBProcessor.get_next_wat_file_to_process()

	if toprocess is not None:
		Processor.set_wat_file()
		Processor.extract_wat_file()
		Processor.load_wat_file()
		Processor.iterate_wat_file()
		wat_details = Processor.extract_details()
		DBProcessor.bulk_insert_ignore(wat_details)
		Processor.remove_wat_file()
		DBProcessor.update_wat_file_status('completed')
		sys.exit

	remove file: busy.lock



# Classes:

processor.py
Processor

wat_processory.py
WatProcessor (Processor)


# DB:

DBProcessor
 
