"""
A data strucutre holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""
class Index:

    def __init__(self, table):
        self.table = table
        self.all_columns_dicts = dict()
        #Create primary key column index by default
        self.create_index(0)

    #create index for a column
    def create_index(self, column_number):
        self.all_columns_dicts["dict" + str(column_number+3)] = dict()

    #create indexes for all columns
    def create_all_indexes_except_primary(self):
        for column_no in range(4, self.table.num_columns+3):
            self.all_columns_dicts["dict" + str(column_no)] = dict()

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.all_columns_dicts.get("dict" + str(column)) != None:
            a_column_dict = self.all_columns_dicts["dict" + str(column)]
            return a_column_dict[value]

    # Once an index is created functions to insert, delete and update an index are required

    def insert(self, rid, value, column):
        if "dict"+str(column) not in self.all_columns_dicts.keys():
            self.all_columns_dicts["dict" + str(column)] = dict()
        a_column_dict = self.all_columns_dicts["dict" + str(column)]
        if value not in a_column_dict.keys():
            a_column_dict[value] = [rid]
            #print(a_column_dict)
        else:
            a_column_dict[value].append(rid)
    
    def delete(self, rid, value, column):
        #TODO: Delete just the value if index is [] (blank)
        a_column_dict = self.all_columns_dicts["dict" + str(column)]
        if value in a_column_dict.keys():
            a_column_dict[value].remove(rid)
    
    def update(self, rid, column, prev_value, updated_value):
        self.delete(rid, prev_value, column)
        self.insert(rid, updated_value, column)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
