'''
Spark_Operations

Simple row wise operations to covert id into a string, provided by MongoDB.
Drop first row because initializing a new collection in MongoDB require a single row to be provided.
'''

def stringify_id(row):
    return str(row['_id'])

def drop_first_row(collection):
    altered_col = collection.iloc[1:]
    return altered_col 