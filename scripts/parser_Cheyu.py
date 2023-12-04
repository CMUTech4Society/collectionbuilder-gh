import pandas as pd

books_original = pd.read_csv('books.csv')
books_copy = books_original.copy()

cols_to_combine = [1, # location
                   2, # author
                   3, # title
                   4, # publisher
                   6, # isbn
                   7, # notes
                   8, # language
                   9, # resources
                   10, # related materials
                   11, # media
                   ]

def break_line(cur_row, col_to_combine): # if the last chr from the prev row is ";", we need to write data in a new line
    prev_row = cur_row - 1
    prev_value = books_copy.iloc[prev_row, col_to_combine]
    return str(prev_value)[-1] == ";"

def add_to_first_row_of_cur_item(new_data, first_row, cur_row, col_to_combine):
    old_data = books_copy.iloc[first_row, col_to_combine]
    if (str(old_data) != "nan"):
        if (break_line(cur_row, col_to_combine)):
            # print("cur col: ", cur_row, col_to_combine, "start at new line; old data: ", old_data, "new data: ", new_data)
            books_copy.iloc[first_row, col_to_combine] = old_data + "\n" + new_data # !!important: this is based on the assumption that all datatypes are strings. will not work if they are numbers.
        else:
            # print("cur col: ", cur_row, col_to_combine, "doesn't break line; old data: ", old_data, "new data: ", new_data)
            books_copy.iloc[first_row, col_to_combine] = old_data + " " + new_data # same !!important
    else:
        if (break_line(cur_row, col_to_combine)):
            # print("cur col: ", cur_row, col_to_combine, "start at new line; old data: ", old_data, "new data: ", new_data)
            books_copy.iloc[first_row, col_to_combine] = new_data # !!important: this is based on the assumption that all datatypes are strings. will not work if they are numbers.
        else:
            # print("cur col: ", cur_row, col_to_combine, "doesn't break line; old data: ", old_data, "new data: ", new_data)
            books_copy.iloc[first_row, col_to_combine] = new_data # same !!important
    # print("updated [%d, %d] = %s" % (first_row, col_to_combine, books_copy.iloc[first_row, col_to_combine]))

def add_isbn_to_first_row_of_cur_item(new_data, first_row, cur_row, col_to_combine):
    old_data = books_copy.iloc[first_row, col_to_combine]
    break_line_signals = ("ISBN-", "OCLC")
    if (str(old_data) != "nan"):
        if (new_data.startswith(break_line_signals) or break_line(cur_row, col_to_combine)):
            # print("cur col: ", cur_row, col_to_combine, "start at new line; old data: ", old_data, "new data: ", new_data)
            books_copy.iloc[first_row, col_to_combine] = old_data + "\n" + new_data # !!important: this is based on the assumption that all datatypes are strings. will not work if they are numbers.
        else:
            # print("cur col: ", cur_row, col_to_combine, "doesn't break line; old data: ", old_data, "new data: ", new_data)
            books_copy.iloc[first_row, col_to_combine] = old_data + " " + new_data # same !!important
    else:
        if (new_data.startswith(break_line_signals) or break_line(cur_row, col_to_combine)):
            # print("cur col: ", cur_row, col_to_combine, "start at new line; old data: ", old_data, "new data: ", new_data)
            books_copy.iloc[first_row, col_to_combine] = new_data # !!important: this is based on the assumption that all datatypes are strings. will not work if they are numbers.
        else:
            # print("cur col: ", cur_row, col_to_combine, "doesn't break line; old data: ", old_data, "new data: ", new_data)
            books_copy.iloc[first_row, col_to_combine] = new_data # same !!important
    # print("updated [%d, %d] = %s" % (first_row, col_to_combine, books_copy.iloc[first_row, col_to_combine]))

def combine(first_row, rows_to_combine):
    print("first row: ", first_row, " rows to combine: ", rows_to_combine)
    for i in range(1, len(rows_to_combine)): # combine data in each row except for the first row of this item
        row_to_combine = rows_to_combine[i]
        for col in cols_to_combine: # combine data column by column
            new_data = books_copy.iloc[row_to_combine, col]
            if (str(new_data) != "nan"):
                if (col != 6): # not isbn column
                    add_to_first_row_of_cur_item(new_data, first_row, row_to_combine, col)
                else: # isbn column
                    add_isbn_to_first_row_of_cur_item(new_data, first_row, row_to_combine, col)

def main():
    rows_to_combine = []
    first_row = 0 # the first row is the start of the first item
    for i in books_copy.index:
        # print("-- [%d, 0] = %s --" % (i, books_copy.iloc[i, 0])) # The "Number" column has col_index = 0
        if str(books_copy.iloc[i, 0]) != 'nan': # start of a new item
            # print("* this is THE start of a new item, and rows = ", rows_to_combine)
            if len(rows_to_combine) != 0: # not the first row i.e. there are some rows for the prev item to combine
                combine(first_row, rows_to_combine) # take in the row that is the start of the cur item
                first_row = i # after the prev rows are combined, update row
                rows_to_combine = []
            rows_to_combine.append(i) # store this row. it might need to be combined with some following rows
            # print("* updated rows = ", rows_to_combine)
        else: # this row need to be combined with some other rows
            rows_to_combine.append(i)
            # print("* this is not the start of a new item, and rows = ", rows_to_combine)
    books_copy.dropna(subset='Number', inplace = True)
    books_copy.to_csv('books_new.csv')
main()