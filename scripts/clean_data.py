import pandas
import copy
from string import ascii_letters

# Terribly ugly, but it works. Each file is handled separately because of their different formats


def book_entry(books, metadata, field_vars, previous_entry_start, row):
    #objectid, title, format, publisher
            field_vars[0] = (books.at[previous_entry_start, books.columns[0]].strip(ascii_letters)).strip()
            #print("row: ", row, " prev start: ", previous_entry_start, field_vars[0])
            field_vars[1] = " ".join([books.at[r, books.columns[3]] for r in range(previous_entry_start, row) if not pandas.isnull(books.at[r, books.columns[3]])])
            field_vars[3] = "book"
            field_vars[5] = " ".join([books.at[r, books.columns[4]] for r in range(previous_entry_start, row) if not pandas.isnull(books.at[r, books.columns[4]])])

            #update later for specific creators
            #creator, description, language, year, location
            field_vars[8] = " ".join([books.at[r, books.columns[2]] for r in range(previous_entry_start, row) if not pandas.isnull(books.at[r, books.columns[2]])])
            field_vars[9] = " ".join([books.at[r, books.columns[7]] for r in range(previous_entry_start, row) if not pandas.isnull(books.at[r, books.columns[7]])])
            field_vars[11] = " ".join([books.at[r, books.columns[8]] for r in range(previous_entry_start, row) if not pandas.isnull(books.at[r, books.columns[8]])])
            field_vars[12] = books.at[previous_entry_start, books.columns[5]]
            field_vars[14] = books.at[previous_entry_start, books.columns[1]]

            #split on ISBN, sort to get correct 10 13 order
            isbn = " ".join([books.at[r, books.columns[6]].replace("\n", "").replace("OCLC", "ISBN-") for r in range(previous_entry_start, row) if not (pandas.isnull(books.at[r, books.columns[6]]))]).split("ISBN-")
            isbn = [x.strip() for x in isbn if x.strip() != ""]
            #isbn.sort()
            #print(isbn)

            #isbn-10, isbn-13
            for num in isbn:
                if ("10:" in num):
                    field_vars[15] = "ISBN-" + num
                elif ("13:" in num):
                    field_vars[16] = "ISBN-" + num
                else:
                    field_vars[17] = "OCLC" + num

            #resources, related materials, media
            field_vars[18] = " ".join([books.at[r, books.columns[9]] for r in range(previous_entry_start, row) if not (pandas.isnull(books.at[r, books.columns[9]]))])
            field_vars[19] = " ".join([books.at[r, books.columns[10]] for r in range(previous_entry_start, row) if not (pandas.isnull(books.at[r, books.columns[10]]))])
            field_vars[20] = " ".join([books.at[r, books.columns[11]] for r in range(previous_entry_start, row) if not (pandas.isnull(books.at[r, books.columns[11]]))])

            #add new row with the information to table
            metadata.loc[len(metadata.index)] = field_vars

def add_books(books, metadata, vars):  

    previous_entry_start = 0

    for row in range(len(books.index) + 1):

        field_vars = copy.copy(vars)

        if row == len(books.index):
            book_entry(books, metadata, field_vars, previous_entry_start, row)
        elif not pandas.isnull(books.at[row, books.columns[0]]):
            object_id = (books.at[row, books.columns[0]].strip(ascii_letters)).strip()
            
            if object_id != '1':
                book_entry(books, metadata, field_vars, previous_entry_start, row)

            #start looking for new entry
            previous_entry_start = row


def journals_entry(journals, metadata, field_vars, previous_entry_start, row):
    #objectid, title, format, volume, publisher
            field_vars[0] = journals.at[previous_entry_start, journals.columns[0]]
            field_vars[1] = " ".join([journals.at[r, journals.columns[3]] for r in range(previous_entry_start, row) if not (pandas.isnull(journals.at[r, journals.columns[3]]))])
            field_vars[3] = "journal"
            field_vars[4] = " ".join([journals.at[r, journals.columns[5]] for r in range(previous_entry_start, row) if not (pandas.isnull(journals.at[r, journals.columns[5]]))])
            field_vars[5] = " ".join([journals.at[r, journals.columns[4]] for r in range(previous_entry_start, row) if not (pandas.isnull(journals.at[r, journals.columns[4]]))])

            #update later for specific creators
            #creator, description, language, year, location
            field_vars[8] = " ".join([journals.at[r, journals.columns[2]] for r in range(previous_entry_start, row) if not (pandas.isnull(journals.at[r, journals.columns[2]]))])
            field_vars[9] = " ".join([journals.at[r, journals.columns[7]] for r in range(previous_entry_start, row) if not (pandas.isnull(journals.at[r, journals.columns[7]]))])
            field_vars[11] = " ".join([journals.at[r, journals.columns[8]] for r in range(previous_entry_start, row) if not (pandas.isnull(journals.at[r, journals.columns[8]]))])
            field_vars[12] = journals.at[previous_entry_start, journals.columns[5]]
            field_vars[14] = journals.at[previous_entry_start, journals.columns[1]]

            #resources, related materials, media
            field_vars[18] = " ".join([journals.at[r, journals.columns[9]] for r in range(previous_entry_start, row) if not (pandas.isnull(journals.at[r, journals.columns[9]]))])
            field_vars[19] = " ".join([journals.at[r, journals.columns[10]] for r in range(previous_entry_start, row) if not (pandas.isnull(journals.at[r, journals.columns[10]]))])
            field_vars[20] = " ".join([journals.at[r, journals.columns[11]] for r in range(previous_entry_start, row) if not (pandas.isnull(journals.at[r, journals.columns[11]]))])

            #add new row with the information to table
            metadata.loc[len(metadata.index)] = field_vars

def add_journals(journals, metadata, vars):    

    previous_entry_start = 0

    for row in range(len(journals.index) + 1):

        field_vars = copy.copy(vars)

        if row == len(journals.index):
            journals_entry(journals, metadata, field_vars, previous_entry_start, row)
        elif not pandas.isnull(journals.at[row, journals.columns[0]]):
            object_id = journals.at[row, journals.columns[0]]
            
            if object_id != 1:
                journals_entry(journals, metadata, field_vars, previous_entry_start, row)

        #start looking for new entry
        previous_entry_start = row


def music_entry(music, metadata, field_vars, previous_entry_start, row):
    #objectid, title, format, record company
    field_vars[0] = music.at[previous_entry_start, music.columns[0]]
    field_vars[1] = " ".join([music.at[r, music.columns[3]] for r in range(previous_entry_start, row) if not (pandas.isnull(music.at[r, music.columns[3]]))])
    field_vars[3] = "music"
    field_vars[6] = " ".join([music.at[r, music.columns[4]] for r in range(previous_entry_start, row) if not (pandas.isnull(music.at[r, music.columns[4]]))])

    #update later for specific creators
    #creator, description, language, year, medium, location
    field_vars[8] = " ".join([music.at[r, music.columns[2]] for r in range(previous_entry_start, row) if not (pandas.isnull(music.at[r, music.columns[2]]))])
    field_vars[9] = " ".join([music.at[r, music.columns[9]] for r in range(previous_entry_start, row)  if not (pandas.isnull(music.at[r, music.columns[9]]))])
    field_vars[11] = " ".join([music.at[r, music.columns[8]] for r in range(previous_entry_start, row) if not (pandas.isnull(music.at[r, music.columns[8]]))])
    field_vars[12] = music.at[previous_entry_start, music.columns[5]]
    field_vars[13] = " ".join([music.at[r, music.columns[7]] for r in range(previous_entry_start, row) if not (pandas.isnull(music.at[r, music.columns[7]]))])
    field_vars[14] = music.at[previous_entry_start, music.columns[1]]

    #resources
    field_vars[18] = " ".join([music.at[r, music.columns[7]] for r in range(previous_entry_start, row) if not (pandas.isnull(music.at[r, music.columns[7]]))])

    #add new row with the information to table
    metadata.loc[len(metadata.index)] = field_vars

def add_music(music, metadata, vars):    

    previous_entry_start = 0

    for row in range(len(music.index) + 1):

        field_vars = copy.copy(vars)

        if row == len(music.index):
            music_entry(music, metadata, field_vars, previous_entry_start, row)
        elif not pandas.isnull(music.at[row, music.columns[0]]):
            object_id = music.at[row, music.columns[0]]
            
            if object_id != 1:
                music_entry(music, metadata, field_vars, previous_entry_start, row)
        
        #start looking for new entry
        previous_entry_start = row


def studio_entry(studio, metadata, field_vars, previous_entry_start, row):
    #objectid, title, format, publisher
    field_vars[0] = studio.at[previous_entry_start, studio.columns[0]]
    field_vars[1] = " ".join([studio.at[r, studio.columns[3]] for r in range(previous_entry_start, row) if not (pandas.isnull(studio.at[r, studio.columns[3]]))])
    field_vars[3] = "studio/museum"
    field_vars[5] = " ".join([studio.at[r, studio.columns[4]] for r in range(previous_entry_start, row) if not (pandas.isnull(studio.at[r, studio.columns[4]]))])

    #update later for specific creators
    #creator, description, language, year, location
    field_vars[8] = " ".join([studio.at[r, studio.columns[2]] for r in range(previous_entry_start, row) if not (pandas.isnull(studio.at[r, studio.columns[2]]))])
    field_vars[9] = " ".join([studio.at[r, studio.columns[6]] for r in range(previous_entry_start, row) if not (pandas.isnull(studio.at[r, studio.columns[6]]))])
    field_vars[11] = " ".join([studio.at[r, studio.columns[7]] for r in range(previous_entry_start, row) if not (pandas.isnull(studio.at[r, studio.columns[7]]))])
    field_vars[12] = studio.at[previous_entry_start, studio.columns[5]]
    field_vars[14] = studio.at[previous_entry_start, studio.columns[1]]

    #resources
    field_vars[18] = " ".join([studio.at[r, studio.columns[8]] for r in range(previous_entry_start, row) if not (pandas.isnull(studio.at[r, studio.columns[8]]))])

    #add new row with the information to table
    metadata.loc[len(metadata.index)] = field_vars

def add_studio(studio, metadata, vars):    

    previous_entry_start = 0

    for row in range(len(studio.index) + 1):

        field_vars = copy.copy(vars)

        if row == len(studio.index):
            studio_entry(studio, metadata, field_vars, previous_entry_start, row)
        elif not pandas.isnull(studio.at[row, studio.columns[0]]):
            object_id = studio.at[row, studio.columns[0]]
            
            if object_id != 1:
                studio_entry(studio, metadata, field_vars, previous_entry_start, row)

        #start looking for new entry
        previous_entry_start = row


def ephemera_entry(ephemera, metadata, field_vars, previous_entry_start, row):
    #objectid, title, format, publisher
    field_vars[0] = ephemera.at[previous_entry_start, ephemera.columns[0]]
    field_vars[1] = " ".join([ephemera.at[r, ephemera.columns[3]] for r in range(previous_entry_start, row) if not (pandas.isnull(ephemera.at[r, ephemera.columns[3]]))])
    field_vars[3] = "ephemera"
    field_vars[5] = " ".join([ephemera.at[r, ephemera.columns[4]] for r in range(previous_entry_start, row) if not (pandas.isnull(ephemera.at[r, ephemera.columns[4]]))])

    #update later for specific creators
    #creator, description, language, year, location
    field_vars[8] = " ".join([ephemera.at[r, ephemera.columns[2]] for r in range(previous_entry_start, row) if not (pandas.isnull(ephemera.at[r, ephemera.columns[2]]))])
    field_vars[9] = " ".join([ephemera.at[r, ephemera.columns[6]] for r in range(previous_entry_start, row) if not (pandas.isnull(ephemera.at[r, ephemera.columns[6]]))])
    field_vars[11] = " ".join([ephemera.at[r, ephemera.columns[7]] for r in range(previous_entry_start, row) if not (pandas.isnull(ephemera.at[r, ephemera.columns[7]]))])
    field_vars[12] = ephemera.at[previous_entry_start, ephemera.columns[5]]
    field_vars[14] = ephemera.at[previous_entry_start, ephemera.columns[1]]

    #resources
    field_vars[18] = " ".join([ephemera.at[r, ephemera.columns[8]] for r in range(previous_entry_start, row) if not (pandas.isnull(ephemera.at[r, ephemera.columns[8]]))])

    #add new row with the information to table
    metadata.loc[len(metadata.index)] = field_vars

def add_ephemera(ephemera, metadata, vars):    

    previous_entry_start = 0

    for row in range(len(ephemera.index) + 1):

        field_vars = copy.copy(vars)

        if row == len(ephemera.index):
            ephemera_entry(ephemera, metadata, field_vars, previous_entry_start, row)
        elif not pandas.isnull(ephemera.at[row, ephemera.columns[0]]):
            object_id = ephemera.at[row, ephemera.columns[0]]
            
            if object_id != 1:
                ephemera_entry(ephemera, metadata, field_vars, previous_entry_start, row)

        #start looking for new entry
        previous_entry_start = row



books = pandas.read_csv("CSV/books.csv")
journals = pandas.read_csv("CSV/journals_periodicals.csv")
music = pandas.read_csv("CSV/music.csv")
studio = pandas.read_csv("CSV/studio_museum.csv")
ephemera = pandas.read_csv("CSV/ephemera.csv")


fields = {"objectid" : [], "title" : [], "filename" : [], "kind" : [], "volume" : [], "publisher" : [], 
          "record company" : [], "youtubeid" : [], "creator" : [], "description" : [], "source" : [], 
          "language" : [], "year" : [], "medium" : [], "location" : [], "ISBN-10" : [], "ISBN-13" : [], 
          "OCLC" : [], "resources" : [], "related materials" : [], "media" : [], "format" : []}

field_vars = [pandas.NA] * len(fields)
field_vars[-1] = "image/jpeg"

metadata = pandas.DataFrame(fields)

add_books(books, metadata, copy.copy(field_vars))
add_journals(journals, metadata, copy.copy(field_vars))
add_music(music, metadata, copy.copy(field_vars))
add_studio(studio, metadata, copy.copy(field_vars))
add_ephemera(ephemera, metadata, copy.copy(field_vars))

# rearrange columns

metadata = metadata[["objectid", "location", "creator", "title", "year", "publisher", "volume", 
          "record company", "medium", "description", "ISBN-10", "ISBN-13", "OCLC", 
          "language", "resources", "related materials", "media", "format", "filename", 
          "source", "youtubeid"]]

metadata.to_csv("metadata.csv")
