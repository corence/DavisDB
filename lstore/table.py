from lstore.index import Index
from time import time
from collections import defaultdict
from lstore.main_page import MainPage
from lstore.page_range import PageRange
from lstore.config import *
import os
import pickle
from threading import Lock
import collections

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
    def insert_record(self, table):
        pass

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, bufferpool, is_new):
        self.name = name
        # which column is primary key
        self.key = key
        # + 2 to also acccount for the indirection column and schema encoding column // 1 = indirection & 2 = schema
        # Value in the indirection column will contain latest RID of a record, if a record was never updated, will contain -1
        # Schema encoding column will contain 1 if a record was ever updated and 0 if never updated
        self.num_columns = num_columns
        self.se_len = self.num_columns//8 + 1

        self.lock_manager = {}
        self.lock_counter = {}
        #Number of page ranges this table
        self.num_of_ranges = 0
        self.is_new = is_new

        self.bufferpool = bufferpool
        self.log_serial_no = 0
        self.transaction_id_ctr = 0
        self.merged_range = 0
        self.lock = Lock()

        #To store RID as key and page range no, page no, slot no as a list of values
        # if new table, set original values
        if is_new:
            self.num_of_ranges = 0
            self.page_directory = {}
            self.key_dict = {}
            self.tps_data = {}
            self.deleted_keys = {}
            self.pra = dict()
            self.tps = {}
            self.se_tps = set() 
            self.rid = 0
            self.past_records = collections.defaultdict(list)


        # old table --> load in from disk
        else:
            # set current path
            curr_path = os.path.join(self.bufferpool.path, "metadata")

            # get page_directory from disk
            metadata_path = os.path.join(curr_path, 'page_directory.pickle')
            with open(metadata_path, "rb") as f:
                self.page_directory = pickle.load(f)

            metadata_path = os.path.join(curr_path, 'past_records.pickle')
            with open(metadata_path, "rb") as f:
                self.past_records = pickle.load(f)

            # get key_dict from disk
            metadata_path = os.path.join(curr_path, 'key_dict.pickle')
            with open(metadata_path, "rb") as f:
                self.key_dict = pickle.load(f)
                
            # get tps_data from disk
            metadata_path = os.path.join(curr_path, 'tps.pickle')
            with open(metadata_path, "rb") as f:
                self.tps = pickle.load(f)

            metadata_path = os.path.join(curr_path, 'se_tps.pickle')
            with open(metadata_path, "rb") as f:
                self.se_tps = pickle.load(f)

            metadata_path = os.path.join(curr_path, 'pra.pickle')
            with open(metadata_path, "rb") as f:
                self.pra = pickle.load(f)

            metadata_path = os.path.join(curr_path, 'deleted_keys.pickle')
            with open(metadata_path, "rb") as f:
                self.deleted_keys = pickle.load(f)

            # get num_of_ranges from disk
            metadata_path = os.path.join(curr_path, 'num_of_ranges.txt')
            self.num_of_ranges = int(self.bufferpool.read_disk(metadata_path))

            metadata_path = os.path.join(curr_path, 'rid.txt')
            self.rid = int(self.bufferpool.read_disk(metadata_path))

        # stores physical page id as key and page range no, main page no, page no as a list of values
        # self.physical_page_directory = {}
        self.index = Index(self)

        #automatically create a page range object when initialzing a Table
        #A page Range will automatically create a Base Page in its constructor
        self.create_page_range()
        pass

    def merge(self, record):
        base_page_range, base_page, base_slot = self.page_directory[record]
        base_page_id1 = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(2) + "-"
        curr_base_page1 = self.bufferpool.access(base_page_id1, None)
        binary_list = list(bin(curr_base_page1.read(base_slot, self.se_len))[2:].zfill(self.se_len * 8)) 


        curr_base_page1.pin_count -= 1
        base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(1) + "-"
        curr_base_page = self.bufferpool.access(base_page_id, None)
        tail_rid = curr_base_page.read(base_slot)


        curr_base_page.pin_count -= 1

        tail_page_range, tail_page, tail_slot = self.page_directory[tail_rid]
        for x in range(self.num_columns + 1):
            if binary_list[x] == '1':
                tail_page_id = "t" + str(tail_page_range) + "-" + str(tail_page) + "-" + str(x + 2) + "-"
                curr_tail_page = self.bufferpool.access(tail_page_id, None)
                val = curr_tail_page.read(tail_slot)
                base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(x + 3) + "-"
                curr_base_page = self.bufferpool.access(base_page_id, None)
                curr_base_page.overwrite(base_slot, val)
                self.bufferpool.access(base_page_id, None)
        if record in self.se_tps:                    
            self.se_tps.remove(record)
        self.tps[record] = tail_rid

    #This function is called in the table's constructor to create an initial page range object
    def create_page_range(self):
        # if table is new
        if self.is_new:
            #Increment the number of page ranges
            self.num_of_ranges += 1
            self.pra[self.num_of_ranges] = [0, set()] 
            #store the page range's id as key and page range object as value in the page_ranges_dictionary of the table object 
            self.curr_range = PageRange(self.num_of_ranges, self.num_columns + 2, self.bufferpool, self.is_new, self.index, self.key_dict, self.key)
            return self.curr_range
        # if table is old
        else:
            # loop through ranges from previous table
            for r in range(1, self.num_of_ranges+1):
                #self.pra[self.num_of_ranges] = [0, set()] is this right?
                self.curr_range = PageRange(r, self.num_columns + 2, self.bufferpool, self.is_new, self.index, self.key_dict, self.key)
            return self.curr_range

    def increment_rid(self):
        self.rid += 1
        rid = self.rid
        return(rid)

    def set_exclusive_rid(self, rid, locked_rids):
    # if rid present in lock manager, abort
        # self.lock.acquire()
        if rid in self.lock_manager:
            if self.lock_manager[rid] == 'X': 
                if rid in locked_rids:
                    return True
                else:
                    return False
            if self.lock_manager[rid] == 'S' and rid in locked_rids and self.lock_counter[rid] == 1:
                self.lock_manager[rid] = 'X' 
                # self.lock.release() 
                return True
            else:
                # self.lock.release() 
                return False
        # else place exclusive lock on rid in lock manager
        else:
            self.lock_manager[rid] = 'X'
        # self.lock.release() 
        return True

    def set_shared_rid(self, rid, locked_rids):
        # if rid present in lock manager
        # self.lock.acquire()
        if rid in self.lock_manager:
            # if rid isn't exclusively locked, abort
            if self.lock_manager[rid] == 'X':
                # self.lock.release() 
                if rid in locked_rids:
                    return True        
                else:
                    return False
            # otherwise keep shared lock
            else:
                return True
        # if rid not present, make it shared lock
        else:
            self.lock_manager[rid] = 'S'
        # self.lock.release() 
        return True

    def get_rid(self):
        self.lock.acquire()
        self.rid += 1
        rid = self.rid
        self.lock.release()
        return(rid)
        
#ASSUMPTIONS

#each record size os 8*number of cols

#for simplification, base pages and tail pages are both created from the main page class and 
#will have the same max number of records limit

#no need to have separate RID for all pages of a main page for a record as assuming slot numbers will be same throughout

#IDS Begin from 
# Slot: 0
# Page: 1
# Base Page: 1
# Range: 1
# Table: 1
