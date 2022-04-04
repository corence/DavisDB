MAX_RECORDS = 512
MAX_BASE_PAGES = 16
# BYTE ARRAY WILL BE 4096 BYTES
# EVERY COLUMN WILL STORE 8 BYTES FOR EACH ENTRY

class Page():

    def __init__(self, page_range_id, main_page_id, physical_page_id, is_base, data):
        #To keep track of number of records created
        #Initialize byte array
        if data == None:
            self.data = bytearray()
        # set data to previous data passed in from disk
        else:
            self.data = data
        # page is base page
        if is_base == 1:
            self.page_id = "b" + str(page_range_id) + "-" + str(main_page_id) + "-" + str(physical_page_id) + "-"
        # page is tail page
        else:
            self.page_id = "t" + str(page_range_id) + "-" + str(main_page_id) + "-" + str(physical_page_id) + "-"

        self.is_dirty = False
        self.pin_count = 0
        self.num_records = len(self.data)//8

        #size of each record is 8 bytes

    #To check if the page has enough space to add at least one more record
    def has_capacity(self):
        if self.num_records < MAX_RECORDS:
            return True
        else:
            return False

    # can make this function to return the slot number if needed, helpful for updating page directory
    def write(self, value):
        if not self.has_capacity():
            return False
        #If no records present yet, create the first record as byte array
        if self.num_records == 0:
            #convert the integer value to byte array
            self.data = bytearray((value).to_bytes(8, 'big', signed=True))
            #increment number of records
            self.num_records += 1
            self.is_dirty = True
            return True
        #If at least one record is present, just append new byte array value at the end to keep it contiguous
        else:
            converted_value = (value).to_bytes(8, 'big', signed=True)
            #appending new value to the already present byte array
            self.data += converted_value
            #increment number of records
            self.num_records += 1
            self.is_dirty = True
            return True

    # To read from a particular page, give a slot number and get the integer in return
    # slots start from 0
    def read(self, slot, se=None):
        #convert bytes to int value 
        if se != None:
            int_value = int.from_bytes(self.data[slot*8 : (slot*8+(se * 8))], byteorder='big', signed=True)
            return int_value

        int_value = int.from_bytes(self.data[slot*8 : (slot*8+8)], byteorder='big', signed=True)
        return int_value

    def read_data_in_bytes(self, slot):
        return self.data[slot*8 : (slot*8+8)]

    # To overwrite the value for a record's indirection column
    def overwrite(self, slot, value, se=None):

        # overwrite here
        self.is_dirty = True
        if se != None:
            converted_value = (value).to_bytes(se*8, 'big', signed=True)
            self.data = self.data[: slot * 8] + converted_value + self.data[(slot * 8) + (se * 8):]    
        else:
            converted_value = (value).to_bytes(8, 'big', signed=True)
            self.data = self.data[: slot * 8] + converted_value + self.data[(slot * 8) + 8:]

    def delete(self, slot):  
        # overwrite here
        self.is_dirty = True
        self.data = self.data[: slot * 8] + b'\x00\x00\x00\x00\x00\x00\x00\x00' + self.data[(slot * 8) + 8:]

