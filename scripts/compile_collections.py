import argparse
import glob
import pandas as pd

p = argparse.ArgumentParser(description = 'compile CSV spreadsheets')
p.add_argument('--source',help = "folder to read csv files from")
p.add_argument('--destination')

args = p.parse_args()
columns = ["objectid",
            "title", 
            "journal", 
            "record",
            "creator", 
            "description", 
            "source", 
            "langugage", 
            "date", 
            "location", 
            "ISBN-10", 
            "ISBN-13",
            "resources", 
            "related_materials", 
            "media"]

fps = glob.glob(args.source[0]+"/*.csv")
compiled_df = pd.read_csv(fps[0]) 
compiled_df = [pd.concat(compiled_df,pd.read_csv(fp)) for fp in fps[1:]]
compiled_df.reset_index()

##need to deal with multiple formats – books, ephemra, etc. data they have

##TYPE: MUSIC, BOOKS, EPHEMRA, PERIODICALS&JOURNALS, STUDIOMUSEUM