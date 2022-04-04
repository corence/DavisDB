from lstore.config import *
import os
import pickle
from lstore.page import Page
from collections import OrderedDict

class Bufferpool:
    def __init__(self, path):
        self.pool = OrderedDict()
        self.path = path
        # self.numCols = table.num_columns

    def access(self, page_id, page):
        # page id exists in bufferpool
        if page_id in self.pool:
            self.pool.move_to_end(page_id)
            self.pool[page_id].pin_count += 1
            return self.pool[page_id]
        # page id is not in bufferpool
        else:
            # set path name
            name = os.path.join(self.path, page_id + '.txt')
            # bufferpool has no capacity, have to evict (use LRU)
            if len(self.pool) >= 50:
                # get least recently used page
                evicted_page = self.pool.popitem(last=False)[1]
                # if evicted page has pin_count above 1, wait until it is back to 0
                # while(evicted_page.pin_count > 0):
                #     continue
                if evicted_page.is_dirty:
                    # get file name of file that is going to be evicted
                    evicted_name = os.path.join(self.path, evicted_page.page_id + '.txt')
                    # write to disk
                    with open(evicted_name, "wb") as f:
                        f.write(evicted_page.data)
            # if page id not in BP but path exists, read from disk, place in bufferpool
            if os.path.exists(name):
                # call read_disk to get page_data
                disk_page_data = self.read_disk(name)
                # get entire page instance using page_data from disk
                disk_page = self.get_disk_page(page_id, disk_page_data)
                # insert into bufferpool
                self.pool[disk_page.page_id] = disk_page
                self.pool[disk_page.page_id].pin_count += 1

                return disk_page
            # no path exists in directory, mark as dirty (in this case means file is being inserted for first time)
            else:
                # print('in2')
                # set path name
                # create new file
                with open(name, "wb") as f:
                    # write bytearray to file
                    f.write(page.data)
                # set dirty bit to true
                page.is_dirty = True
                # set page id as key of bufferpool and page attributes, dirty flag as list as values
                self.pool[page_id] = page
                self.pool[page_id].pin_count += 1

        
        return

    def merge_read(self, page_id):
        name = os.path.join(self.path, page_id + '.txt')
        disk_page_data = self.read_disk(name)
        disk_page = self.get_disk_page(page_id, disk_page_data)
        return disk_page
    
    # get new page instance using data read from disk and parsing the file name for its contents
    def get_disk_page(self, page_id, disk_page_data):
        is_base = None
        # get first char of page_id
        first_char = page_id[0]
        # change is_base depending on first char of page_id
        if first_char == "b":
            is_base = 1
        else:
            is_base = 0

        val = ""
        page_data = []

        #loop through page_id, parsing it to get page_range_id, main_page_id, physical_page_id
        for i in range(1, len(page_id)):
            val += page_id[i]
            if page_id[i] == "-":
                val = val[:-1]
                page_data.append(val)
                val = ""

        # create page instance
        curr_page = Page(page_data[0], page_data[1], page_data[2], is_base, disk_page_data)
        return curr_page

    # read from disk given file path name
    def read_disk(self, name):
        with open(name, "rb") as f:
            disk_page_data = f.read()
        return disk_page_data

    # used to write all dirty pages back to disk upon db.close()
    def write_disk(self, table):
        pool = self.pool.copy()
        for page_id in pool:
            # print(self.pool[page_id].pin_count)
            if pool[page_id].is_dirty:
                name = os.path.join(self.path, page_id + '.txt')
                with open(name, "wb") as f:
                    f.write(pool[page_id].data)

        # create metadata directory
        curr_path = os.path.join(self.path, "metadata")
        if not os.path.isdir(curr_path):
            os.mkdir(curr_path)

        # write key column
        name = os.path.join(curr_path, 'key.txt')
        with open(name, "w") as f:
            f.write(str(table.key))

        name = os.path.join(curr_path, 'rid.txt')
        with open(name, "w") as f:
            f.write(str(table.rid))    

        # write page range attributes to json file
        name = os.path.join(curr_path, 'pra.pickle')
        with open(name, "wb") as f:
            pickle.dump(table.pra, f)  

        # write tps data to json file
        name = os.path.join(curr_path, 'tps.pickle')
        with open(name, "wb") as f:
            pickle.dump(table.tps, f)

        name = os.path.join(curr_path, 'past_records.pickle')
        with open(name, "wb") as f:
            pickle.dump(table.past_records, f)

        name = os.path.join(curr_path, 'se_tps.pickle')
        with open(name, "wb") as f:
            pickle.dump(table.se_tps, f)

        # write num columns
        name = os.path.join(curr_path, 'num_columns.txt')
        with open(name, "w") as f:
            f.write(str(table.num_columns))

        # write page directory to json file
        name = os.path.join(curr_path, 'page_directory.pickle')
        with open(name, "wb") as f:
            pickle.dump(table.page_directory, f)

        name = os.path.join(curr_path, 'key_dict.pickle')
        with open(name, "wb") as f:
            pickle.dump(table.key_dict, f)

        name = os.path.join(curr_path, 'deleted_keys.pickle')
        with open(name, "wb") as f:
            pickle.dump(table.deleted_keys, f)

        # write num of ranges
        name = os.path.join(curr_path, 'num_of_ranges.txt')
        with open(name, "w") as f:
            f.write(str(table.num_of_ranges))

        # write num base pages
        name = os.path.join(curr_path, 'num_base_pages.txt')
        with open(name, "w") as f:
            # TO DO: loop through page ranges to write num base pages for each
            # for _ in range()
            f.write(str(table.curr_range.num_base_pages))

            
    # to write all updated pages to the disk
    def merge_write(self, a_page):
        name = os.path.join(self.path, a_page.page_id + '.txt')
        with open(name, "wb") as f:
            f.write(a_page.data)