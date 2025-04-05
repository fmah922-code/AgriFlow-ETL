def stringify_id(row):
    return str(row['_id'])

def drop_first_row(collection):
    altered_col = collection.iloc[1:]
    return altered_col 