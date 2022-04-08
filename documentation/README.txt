class Processor(builtins.object)
|  Processor(busy)
|
|  This is a class for processing a file.
|
|  Attributes:
|      busy (bool): to specify if a process is running on a file.
|      id (int): id of the file gettig processed
|      path (str): s3 path of the wat file getting processed
|      wat_proc (WatProcessor): object of WatProcessor
|
|  Methods defined here:
|
|  __init__(self, busy)
|      The constructor for Processor class.
|
|      Parameters:
|         busy (bool): to specify if a process is running on a file.
|
|  check_file_exist(self)
|      The function check if any file exists in the download folder and sets the process as busy if there is any file.
|
|  download_wat_file(self)
|      The function to downloads the wat file and places it into download folder.
|
|  extract_wat_file(self)
|      calls the wat_proc.extract_wat_details() and extracts ip,domain and tld details.
|
|      Returns:
|          list (tuple): List of tuples with wat details (ip,domain,tld)
|
|  remove_wat_file(self)
|      The function removes the current file in path from download folder
|
|  set_busy(self, busy)
|      The function to sets the busy attribute.
|
|      Parameters:
|          busy (bool): to specify if a process is running on a file.
|
|  set_wat_file(self, id, path)
|      The function to sets the wat file
|
|      Parameters:
|          id (int): id of the file gettig processed
|          path (str): s3 path of the wat file getting processed
|  ----------------------------------------------------------------------



class DBProcessor(builtins.object)
|  This is a class for database operations.
|
|  Attributes:
|      mydb (sqlconnecter): connector to db
|
|  Methods defined here:
|
|  __init__(self)
|      Initialize self.  See help(type(self)) for accurate signature.
|
|  bulk_insert_ignore(self, wat_details)
|      inserts wat data into domains table
|
|  get_new_file_from_db(self, status)
|      get file from database with the status specified
|
|      Parameters:
|          status (string): status of the file
|
|  get_next_wat_file_to_download(self)
|      get path of the wat file with the status "new" to download from database
|
|      Returns:
|          Dictionary: dictionary of id and path
|
|  get_next_wat_file_to_process(self)
|      get path of the wat file with the status "downloaded" to extract from database
|
|      Returns:
|          Dictionary: dictionary of id and path
|
|  set_processor(self, processor)
|      This function sets the processor object
|
|  update_wat_file_status(self, status)
|      update file the status in db specified for the id of the process
|
|      Parameters:
|          status (string) : status of the file
|
|  ----------------------------------------------------------------------


class WatProcessor(builtins.object)
|  This is a class for wat extraction process.
|
|  Attributes:
|      path (str): s3 path of file to extract details from
|
|  Methods defined here:
|
|  __init__(self)
|      Initialize self.  See help(type(self)) for accurate signature.
|
|  extract_wat_details(self)
|      This is a funcation for wat extraction process.
|
|      Returns:
|          List of tuples: list of unique tuples to insert in the database
|
|  removeDuplicates(self, wat_details)
|
|  set_path(self, path)
|      This funcation sets the path to the file with the status "downloaded"
|
|  ----------------------------------------------------------------------
