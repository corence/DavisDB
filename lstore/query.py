from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
import threading



class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table 
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        if self.table.key_dict.get(primary_key) == None:
            return False
        rid = self.table.key_dict[primary_key]
        base_page_range, base_page, base_slot = self.table.page_directory[rid]
        page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(1) + "-"
        curr_page = self.table.bufferpool.access(page_id, None)

        #Save deleted key and rid value, will be used to undo a delete in abort
        #self.table.deleted_keys[primary_key] = rid
        # delete actual record
        del self.table.key_dict[primary_key]
        '''
        if -1 != page.read(base_slot): # not sure if we should change tail pange indirection
            tail_page_range, tail_page, tail_slot = self.table.page_directory[page.read(base_slot)]
            print(tail_page_range, tail_page)
            self.table.page_ranges_dictionary[tail_page_range].tail_pages[tail_page].pages[1].overwrite(tail_slot, 0)
        '''
        curr_page.delete(base_slot)

        # TODO: think about the old copy being in the bufferpool
        curr_page.pin_count -= 1

        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    
    # 1 = indirection & 2 = schema // null check
    # insert a record into base page with specified slot values for corresponding columns
    def insert(self, *columns):
        # get current page range, current page dictionary, current base page count
        new_rid = self.table.rid
        key_column = self.table.key
        # if self.table.key_dict.get(columns[key_column]) != None: return False

        # get page (any page works) to check if has capacity for another record
        page_id = "b" + str(self.table.num_of_ranges) + "-" + str(self.table.curr_range.num_base_pages) + "-" + str(len(columns)) + "-"
        curr_page = self.table.bufferpool.access(page_id, None)
        curr_page.pin_count -= 1

        curr_base_page = self.table.curr_range.num_base_pages

        if not curr_page.has_capacity():
            if curr_base_page == 16:
                self.table.create_page_range()
            else:
                self.table.curr_range.num_base_pages += 1
                curr_base_page += 1

        # print(self.table.curr_range.num_base_pages)
        for x in range(3, len(columns)+3):
            self.table.index.insert(new_rid, columns[x-3], x)
            page_id = "b" + str(self.table.num_of_ranges) + "-" + str(curr_base_page) + "-" + str(x) + "-"
            curr_page = self.table.bufferpool.access(page_id, None)
            curr_page.write(columns[x-3])

            curr_page.pin_count -= 1

        # write 0 to schema encoding column, since initially inserted records have never been updated
        page_id = "b" + str(self.table.num_of_ranges) + "-" + str(curr_base_page) + "-" + str(2) + "-"
        curr_page = self.table.bufferpool.access(page_id, None)
        curr_page.write(0)
        curr_page.pin_count -= 1
        #curr_page.is_dirty = True

        # write -1 to indirection column for initially inserted records
        page_id = "b" + str(self.table.num_of_ranges) + "-" + str(curr_base_page) + "-" + str(1) + "-"
        curr_page = self.table.bufferpool.access(page_id, None)
        curr_page.write(-1)
        curr_page.pin_count -= 1
        #curr_page.is_dirty = True

        self.table.key_dict[columns[key_column]] = new_rid
        self.table.page_directory[new_rid] = [self.table.num_of_ranges, curr_base_page, curr_page.num_records-1]
        
        #add TPS = 0 for all base pages initially
        '''
        if self.table.num_of_ranges not in self.table.tps_data.keys():
            self.table.tps_data[self.table.num_of_ranges] = {}
            for i in range (1,17):
                # all base pages will have initial TPS value of 0
                self.table.tps_data[self.table.num_of_ranges][i] = 0
        '''
        self.table.rid += 1

        return True, new_rid - 1

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    # given a value from user (index_value), search for that value in the table in the specified column (index_column)
    # if index_value matches value in slot of index_column, append values in record at wanted columns (query_columns) to output

    # to make it faster, if index_column is just the table's primary key, we call a function called 
    def select(self, index_value, index_column, query_columns):
        # add 3 to column to add for indirection and schema encoding (3 not 2 b/c our columns start at 1 not 0)
        # output list
        out = []
        #continue execution if the index_column is not the primary key of the table
        col = index_column + 3 
        # change integer val into array of bytes
        val = index_value.to_bytes(8, 'big', signed=True)

        all_rids = self.table.index.locate(col, index_value)
        #index

        # go through all the records that have the needed value for a given column
        for a_rid in all_rids:
            page_dict = []
            [needed_range, needed_base, needed_slot] = self.table.page_directory[a_rid]
            indir_id = "b" + str(needed_range) + "-" + str(needed_base) + "-" + str(1) + "-"
            indirection = self.table.bufferpool.access(indir_id, None)
            rid = indirection.read(needed_slot)

            if -1 == rid or self.table.tps.get(a_rid) == rid:
                for col_new in range(3, len(query_columns)+3):  
                    if query_columns[col_new - 3] == 1:
                        query_id = "b" + str(needed_range) + "-" + str(needed_base) + "-" + str(col_new) + "-"
                        query_page = self.table.bufferpool.access(query_id, None)
                        page_dict.append(query_page)
                out.append(Record(0,0,self.select_read(needed_slot, page_dict)))
            else:
            # get tail page range, tail page, and tail slot from page directory using rid at indirection column of its base page
                tail_page_range, tail_base_page, tail_slot = self.table.page_directory[rid] # wont always be the same range
                page_dict = []
                for col_new in range(2, len(query_columns)+2):
                    if query_columns[col_new - 2] == 1:
                        query_id = "t" + str(tail_page_range) + "-" + str(tail_base_page) + "-" + str(col_new) + "-"
                        query_page = self.table.bufferpool.access(query_id, None)
                        page_dict.append(query_page)

                out.append(Record(0,0,self.select_read(tail_slot, page_dict)))
            indirection.pin_count -= 1

        return (out)

    # helper function to append values in wanted columns at specified slot number for output
    def select_read(self, slot, page_dict):
        out = []
        for col in range(len(page_dict)):
            out.append(page_dict[col].read(slot))
        return out

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    # schema encoding column or list?
    # updates a record with specified primary key and columns
    # tail pages are appended when necessary to update
    def update(self, primary_key, *columns):
        # if record at primary_key doesn't exist, return False
        key_column = self.table.key
        if self.table.key_dict.get(columns[key_column]) != None and primary_key != columns[key_column]: return False
        if self.table.key_dict.get(primary_key) == None:
           return False
        # set rid to value of key dict at primary_key value
        rid = self.table.key_dict[primary_key]
        # get base page indexes
        base_page_range, base_page, base_slot = self.table.page_directory[rid]
        binary_list = ['0'] * 8
        if (self.table.merged_range == base_page_range and rid in self.table.pra[base_page_range][1]) or (self.table.tps.get(rid) != None):
            if rid in self.table.se_tps:
                base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(2) + "-"
                curr_base_page = self.table.bufferpool.access(base_page_id, None)
                binary_list = list(bin(curr_base_page.read(base_slot, self.table.se_len))[2:].zfill(self.table.se_len * 8))     

            else:
                self.table.se_tps.add(rid)
        else:
            base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(2) + "-"
            curr_base_page = self.table.bufferpool.access(base_page_id, None)
            binary_list = list(bin(curr_base_page.read(base_slot, self.table.se_len))[2:].zfill(self.table.se_len * 8))     


        num_tail_pages = self.table.pra[base_page_range][0]
        # if number of tail pages is 0, add tail page
        if num_tail_pages == 0:
            for col in range(1, self.table.num_columns + 2):
                curr_page = Page(base_page_range, 1, col, 0, None)
                self.table.bufferpool.access(curr_page.page_id, curr_page)
                curr_page.pin_count -= 1
            self.table.pra[base_page_range][0] += 1
            num_tail_pages += 1
        # if tail page doesn't have capacity for one more record, add tail page
        else:
            tail_page_id = "t" + str(base_page_range) + "-" + str(num_tail_pages) + "-" + str(len(columns)) + "-"
            curr_tail_page = self.table.bufferpool.access(tail_page_id, None)
            
            if not curr_tail_page.has_capacity():
                for col in range(1, self.table.num_columns + 2):
                    curr_page = Page(base_page_range, num_tail_pages + 1, col, 0, None)
                    self.table.bufferpool.access(curr_page.page_id, curr_page)
                    curr_page.pin_count -= 1
                self.table.pra[base_page_range][0] += 1
                num_tail_pages += 1

            curr_tail_page.pin_count -= 1
        if columns[key_column] != None:
            # set value of key_dict at column of primary key column to the value of the rid at primary_key
            self.table.key_dict[columns[key_column]] = self.table.key_dict.pop(primary_key)
        # get tail page indirection from base_page indirection column

        base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(1) + "-"
        curr_base_page = self.table.bufferpool.access(base_page_id, None)
        tail_page_indirection = curr_base_page.read(base_slot)
        # change indirection column of current base record to point to the new tail page rid
        new_rid = self.table.rid
        curr_base_page.overwrite(base_slot, new_rid)

        curr_base_page.pin_count -= 1       
        if tail_page_indirection == -1:
            for x in range(3, len(columns) + 3):
                    base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(x) + "-"
                    curr_base_page = self.table.bufferpool.access(base_page_id, None)
                    val = curr_base_page.read(base_slot)
                    self.table.past_records[rid].append(val)
                    curr_base_page.pin_count -= 1
        # loop through columns, start at column 3 because columns 1-2 are for indirection and schema encoding
        for x in range(3, len(columns) + 3):
            # if column value at index is None, don't update that column (cumulative tail records)  
            if columns[x-3] == None:
                # if there is no current tail page corresponding to base page
                if tail_page_indirection == -1:
                    # get current value of base page at slot 
                    base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(x) + "-"
                    curr_base_page = self.table.bufferpool.access(base_page_id, None)
                    val = curr_base_page.read(base_slot)
                    curr_base_page.pin_count -= 1
                    # write value to tail page
                    tail_page_id = "t" + str(base_page_range) + "-" + str(num_tail_pages) + "-" + str(x - 1) + "-"
                    curr_tail_page = self.table.bufferpool.access(tail_page_id, None)
                    curr_tail_page.write(val)
                    curr_tail_page.pin_count -= 1
                # if there is a tail page associated with base page
                else:
                    # get tail page indexes
                    tail_page, tail_slot = self.table.page_directory[tail_page_indirection][1:]
                    tail_page_id = "t" + str(base_page_range) + "-" + str(tail_page) + "-" + str(x - 1) + "-"
                    curr_tail_page = self.table.bufferpool.access(tail_page_id, None)
                    # get current value of tail page at slot
                    val = curr_tail_page.read(tail_slot)
                    curr_tail_page.pin_count -= 1

                    tail_page_id = "t" + str(base_page_range) + "-" + str(num_tail_pages) + "-" + str(x - 1) + "-"
                    curr_tail_page = self.table.bufferpool.access(tail_page_id, None)
                    # write value to tail page
                    curr_tail_page.write(val)
                    curr_tail_page.pin_count -= 1
            # if value at index is not None, update column accordingly
            else:   
                tail_page_id = "t" + str(base_page_range) + "-" + str(num_tail_pages) + "-" + str(x - 1) + "-"
                curr_tail_page = self.table.bufferpool.access(tail_page_id, None)
                curr_tail_page.write(columns[x-3])
                curr_tail_page.pin_count -= 1
                if binary_list[x-3] == '0':
                    binary_list[x-3] = '1' 

        base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(2) + "-"
        curr_base_page = self.table.bufferpool.access(base_page_id, None)
        curr_base_page.overwrite(base_slot, (int(''.join(str(e) for e in binary_list), 2)), self.table.se_len)  
        curr_base_page.pin_count -= 1

                
        # update page directory
        #if curr_page_range.num_tail_pages > 16:
            #print (self.table.num_of_ranges, curr_page_range.num_tail_pages)
        tail_page_id = "t" + str(base_page_range) + "-" + str(num_tail_pages) + "-" + str(1) + "-"
        curr_tail_page = self.table.bufferpool.access(tail_page_id, None)
        if tail_page_indirection == -1:
            curr_tail_page.write(rid)
        else:
            curr_tail_page.write(tail_page_indirection)
        # curr_tail_page.write(tail_page_indirection) #first update will be -1 // not sure what we want
        self.table.page_directory[new_rid] = [base_page_range, num_tail_pages, curr_tail_page.num_records - 1]

        # increment rid
        if rid not in self.table.pra[base_page_range][1]:
            self.table.pra[base_page_range][1].add(rid)
            if len(self.table.pra[base_page_range][1]) > 800 and curr_tail_page.num_records == 512:
                updated_records = self.table.pra[base_page_range][1].copy()
                self.table.pra[base_page_range][1].clear()
                threading.Thread(target=self.queue_merge, args=(base_page_range, updated_records,), daemon=True).start()
        curr_tail_page.pin_count -= 1

        self.table.rid += 1
        return True

    def queue_merge(self, page_range, updated_records):
        self.table.merged_range = page_range
        for record in updated_records:
            self.table.merge(record)



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    # sum values at slot of selected columns in range of start and end range
    # start and end range are key values in key_dict, so sum the slot values at key indexes only within range
    def sum(self, start_range, end_range, aggregate_column_index):
        # add 3 to aggregate_column_index to add for indirection and schema encoding (3 not 2 b/c our columns start at 1 not 0)
        col = aggregate_column_index + 3
        # create array of key value pairs from key_dict
        sorted_keys = sorted(self.table.key_dict.items())
        sum = 0
        flag = 0
        # loop through each key value pair in array
        for key, rid in sorted_keys:
            # break if start_range greater than key
            if start_range > key: continue
            # break if key greater than end_range
            if end_range < key: break
            # get base page indexes
            base_page_range, base_page, base_slot = self.table.page_directory[rid]
            # value at indirection column
            base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(1) + "-"
            curr_base_page = self.table.bufferpool.access(base_page_id, None)
            indirection = curr_base_page.read(base_slot)
            curr_base_page.pin_count -= 1

            # if there is no tail page associated with base page (base page most recently updated page)
            if -1 == indirection or self.table.tps.get(rid) == indirection:
                # add value of base page at specified column to sum
                base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(col) + "-"
                curr_base_page = self.table.bufferpool.access(base_page_id, None)
                sum += curr_base_page.read(base_slot)
                flag = 1
                curr_base_page.pin_count -= 1
            # if there is a tail page associated with base page
            else:
                # get tail page indexes
                tail_page_range, tail_page, tail_slot = self.table.page_directory[indirection]
                # add value of tail page at specified column to sum
                tail_page_id = "t" + str(tail_page_range) + "-" + str(tail_page) + "-" + str(col - 1) + "-"
                curr_tail_page = self.table.bufferpool.access(tail_page_id, None)
                sum += curr_tail_page.read(tail_slot)
                flag = 1
                curr_tail_page.pin_count -= 1
                # print('3: ', curr_tail_page.pin_count)

        # if sum is 0 return False as nothing was summed (no records exist in given range)
        if flag == 0: return False

        return sum


    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False


    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select_version(self, index_value, index_column, query_columns, relative_version, sum = False):
        # add 3 to column to add for indirection and schema encoding (3 not 2 b/c our columns start at 1 not 0)
        # output list
        out = []
        version = abs(relative_version)

        #continue execution if the index_column is not the primary key of the table
        col = index_column + 3 
        # change integer val into array of bytes
        val = index_value.to_bytes(8, 'big', signed=True)
        #index
        all_rids = self.table.index.locate(col, index_value)

        # go through all the records that have the needed value for a given column
        for a_rid in all_rids:
            page_dict = []
            [needed_range, needed_base, needed_slot] = self.table.page_directory[a_rid]
            indir_id = "b" + str(needed_range) + "-" + str(needed_base) + "-" + str(1) + "-"
            indirection = self.table.bufferpool.access(indir_id, None)
            rid = indirection.read(needed_slot)

            # if indirection column empty, just use base page (version 0 since no other versions of record)
            if -1 == rid or self.table.tps.get(a_rid) == rid:
                for col_new in range(3, len(query_columns)+3):
                    if query_columns[col_new - 3] == 1:
                        query_id = "b" + str(needed_range) + "-" + str(needed_base) + "-" + str(col_new) + "-"
                        query_page = self.table.bufferpool.access(query_id, None)
                        page_dict.append(query_page)
                out.append(Record(0,0,self.select_read(needed_slot, page_dict)))
            else:
            # get tail page range, tail page, and tail slot from page directory using rid at indirection column of its base page
                tail_page_range, tail_base_page, tail_slot = self.table.page_directory[rid] # wont always be the same range
                tail_page_indirection = None
                for i in range(version):
                    # access value at tail pages indirection column (to get previous record)
                    tail_page_id = "t" + str(tail_page_range) + "-" + str(tail_base_page) + "-" + str(1) + "-"
                    curr_tail_page = self.table.bufferpool.access(tail_page_id, None)
                    tail_page_indirection = curr_tail_page.read(tail_slot)
                    curr_tail_page.pin_count -= 1
                    tail_page_range, tail_base_page, tail_slot = self.table.page_directory[tail_page_indirection]
                    if a_rid == tail_page_indirection:
                        break
                # version 0
                page_dict = []

                # if rid and tail_page_indirection are the same (aka tail page indirection points back to base page)
                if a_rid == tail_page_indirection:
                    columns = self.table.past_records[a_rid]
                    new_columns = []
                    for i in range(len(self.table.past_records[a_rid])):
                        if query_columns[i] == 1:
                            new_columns.append(columns[i])
                    out.append(Record(0,0,new_columns))
                    return out

                else:
                    for col_new in range(2, len(query_columns)+2):
                        if query_columns[col_new - 2] == 1:
                            query_id = "t" + str(tail_page_range) + "-" + str(tail_base_page) + "-" + str(col_new) + "-"
                            query_page = self.table.bufferpool.access(query_id, None)
                            page_dict.append(query_page)

                out.append(Record(0,0,self.select_read(tail_slot, page_dict)))
            indirection.pin_count -= 1

        return (out)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        # add 3 to aggregate_column_index to add for indirection and schema encoding (3 not 2 b/c our columns start at 1 not 0)
        col = aggregate_column_index + 3
        # create array of key value pairs from key_dict
        sorted_keys = sorted(self.table.key_dict.items())

        version = abs(relative_version)
        sum = 0
        # loop through each key value pair in array
        for key, rid in sorted_keys:
            # break if start_range greater than key
            if start_range > key: continue
            # break if key greater than end_range
            if end_range < key: break
            # get base page indexes
            base_page_range, base_page, base_slot = self.table.page_directory[rid]
            # value at indirection column
            base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(1) + "-"
            curr_base_page = self.table.bufferpool.access(base_page_id, None)
            indirection = curr_base_page.read(base_slot)
            curr_base_page.pin_count -= 1

            # if there is no tail page associated with base page (base page most recently updated page)
            if -1 == indirection or self.table.tps.get(rid) == indirection:
                # add value of base page at specified column to sum
                base_page_id = "b" + str(base_page_range) + "-" + str(base_page) + "-" + str(col) + "-"
                curr_base_page = self.table.bufferpool.access(base_page_id, None)
                sum += curr_base_page.read(base_slot)
                curr_base_page.pin_count -= 1
            # if there is a tail page associated with base page
            else:
                # get most recent (version 0) tail page indexes (since we using base page indirection)
                tail_page_range, tail_page, tail_slot = self.table.page_directory[indirection]
                tail_page_indirection = None

                for i in range(version):
                    # access value at tail pages indirection column (to get previous record)
                    tail_page_id = "t" + str(tail_page_range) + "-" + str(tail_page) + "-" + str(1) + "-"
                    curr_tail_page = self.table.bufferpool.access(tail_page_id, None)
                    tail_page_indirection = curr_tail_page.read(tail_slot)
                    curr_tail_page.pin_count -= 1
                    tail_page_range, tail_page, tail_slot = self.table.page_directory[tail_page_indirection]
                    if rid == tail_page_indirection:
                        break
                
                if rid == tail_page_indirection:
                    columns = self.table.past_records[rid]
                    new_column = columns[0]
                    sum += new_column
                else:
                    tail_page_id = "t" + str(tail_page_range) + "-" + str(tail_page) + "-" + str(col - 1) + "-"
                    curr_tail_page = self.table.bufferpool.access(tail_page_id, None)
                    sum += curr_tail_page.read(tail_slot)
                    curr_tail_page.pin_count -= 1

        # if sum is 0 return False as nothing was summed (no records exist in given range)
        if sum == 0: return False

        return sum