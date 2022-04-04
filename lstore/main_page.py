from lstore.config import *
from lstore.page import Page
import os

class MainPage:

    def __init__(self, main_page_id, num_pages, isBase, page_range_id, bufferpool, is_new, index, key, key_dict = None):
                
        #unique id 
        self.main_page_id = main_page_id
        #number of pages is basically number of colmuns + 2 
        self.num_pages = num_pages 
        self.num_records = 0
        self.page_range_id = page_range_id
        #isBase argument is only used to differentiate between a base page and tail page (if needed for the future milestones)
        self.isBase = isBase
        self.is_new = is_new
        self.key_dict = key_dict
        self.index = index
        self.key = key
        self.bufferpool = bufferpool
        #create appropriate number of pages when initializing a base page or tail page
        self.create_pages()
    
    #to check if a base page/ tail page has enough space for at least one more record
    def has_capacity(self):
        if self.num_records < MAX_RECORDS:
            return True
        else: 
            return False

    # for every base page or tail page created, automatically create empty pages for every column (with no records)
    # this function is called in the constructor above 
    def create_pages(self): 
        base_rids = []
        for col in range(1,self.num_pages+1):
            # if table is new
            if self.is_new:
                # if data is None (page doesn't have data persisted to disk)
                curr_page = Page(self.page_range_id, self.main_page_id, col, self.isBase, None)
                self.bufferpool.access(curr_page.page_id, curr_page)
            # if table already exists on disk
            else:
                self.index.create_all_indexes_except_primary()
                page_id = "b" + str(self.page_range_id) + "-" + str(self.main_page_id) + "-" + str(col) + "-"
                curr_page = self.bufferpool.access(page_id, None)  

                disk_page_data = curr_page.data
                num_records = curr_page.num_records

                if (col == self.key + 3): ## key_column
                    for slot_no in range(0, num_records):
                        primary_key = self.read(disk_page_data, slot_no)
                        if primary_key == 0: ## what if actual val is 0, hard coded primary key column, wont work
                            base_rid=0
                        else:
                            base_rid = self.key_dict[primary_key]
                        base_rids.append(base_rid)

                if (col>=3):
                    for slot_no in range(0, num_records):
                        self.index.insert(base_rids[slot_no], self.read(disk_page_data, slot_no), col)

                if len(disk_page_data) == 0:
                    disk_page_data = None
                curr_page = Page(self.page_range_id, self.main_page_id, col, self.isBase, disk_page_data)

            curr_page.pin_count -= 1

    def read(self, data, slot):
        #convert bytes to int value 
        int_value = int.from_bytes(data[slot*8 : (slot*8+8)], byteorder='big', signed=True)
        return int_value