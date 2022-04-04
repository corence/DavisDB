from lstore.table import Table
import os
import shutil
import lstore.index
from lstore.bufferpool import Bufferpool

class Database():

    def __init__(self):
        self.table = None
        self.bufferpool = None

    # Not required for milestone1
    def open(self, path):
        self.path = path
        if not os.path.isdir(path):
            os.mkdir(path)
        # make new directory with path
        self.bufferpool = Bufferpool(path)

    def close(self):
        self.bufferpool.write_disk(self.table)
        return

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        new_name = os.path.join(self.bufferpool.path, name)

        # if calling create_table again on a table that already exists, remove directory
        if os.path.isdir(new_name):
            shutil.rmtree(new_name)

        # create new directory
        os.mkdir(new_name)
            
        self.bufferpool.path = new_name

        #Create a Table Object
        self.table = Table(name, num_columns, key_index, self.bufferpool, 1)
        return self.table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        curr_path = os.path.join(self.bufferpool.path, name)
        if os.path.exists(curr_path):
            shutil.rmtree(curr_path)
            return True
        #If Key could not be found, -1 is returned
        else:
            return False

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        curr_path = os.path.join(self.bufferpool.path, name)
        if os.path.exists(curr_path):
            # read bufferpool to get num_columns, key_index
            metadata_path = os.path.join(curr_path, 'metadata/num_columns.txt')
            num_columns = int(self.bufferpool.read_disk(metadata_path))

            metadata_path = os.path.join(curr_path, 'metadata/key.txt')
            key_index = int(self.bufferpool.read_disk(metadata_path))

            # set path to new path with inputted name
            self.bufferpool.path = curr_path
            # create table instance
            self.table = Table(name, num_columns, key_index, self.bufferpool, 0)
            return self.table

        return False