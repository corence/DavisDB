MAX_BASE_PAGES = 16
MAX_RECORDS = 512

from lstore.main_page import MainPage
import os

class PageRange:

    def __init__(self, page_range_id, num_columns, bufferpool, is_new, index, key_dict, key):
        #A unique id which can be kept equal to the number for simplicity, so start page range ids from 1 // not sure if needed
        self.page_range_id = page_range_id
        self.is_new = is_new
        self.index = index
        self.key_dict = key_dict
        self.key = key

        self.bufferpool = bufferpool
        # self.num_tail_pages = 0

        if self.is_new:
            # start records on first base page
            self.num_base_pages = 1 
        else:
            curr_path = os.path.join(self.bufferpool.path, "metadata")

            metadata_path = os.path.join(curr_path, 'num_base_pages.txt')
            self.num_base_pages = int(self.bufferpool.read_disk(metadata_path))

        self.num_columns = num_columns

        self.curr_base_page = None
       
        # add all 16 base page while creating a new range object
        for i in range(1,17):
            self.add_base_page(i)
    
    # If a page range's last base page has enough space for at least one more record, return true  
    # otherwise return false as the page range has filled all of its base pages //not sure if needed
    def has_capacity(self):
        if self.num_base_pages[MAX_BASE_PAGES].num_records < MAX_RECORDS:
            return True
        else: 
            return False

    # to add a base page to the page range, used while initializing the page range in a loop to create all base pages
    def add_base_page(self, counter): 
        #store the pages in base_pages dictionary 
        self.curr_base_page = MainPage(counter, self.num_columns, 1, self.page_range_id, self.bufferpool, self.is_new, self.index, self.key, self.key_dict)
        return self.curr_base_page

    # add a tail page only if at least one record needs to be updated
    # number of tail pages in a page range are not fixed, they are added as needed 
    # def add_tail_page(self):
    #     self.num_tail_pages += 1
    #     #store the tail page in tail_pages dictionary
    #     self.curr_tail_page = MainPage(self.num_tail_pages, self.num_columns, 0, self.page_range_id, self.bufferpool, self.is_new)
    #     return self.curr_tail_page
