#!/usr/bin/env python

import pandas as pd
import os
import sys
import csv
import argparse
from tqdm import tqdm
import re
import fastparquet
import sqlalchemy as sa 

import data_layout  # Import the data_layout module

# Function to find the most recent directory matching an 8-digit all-numeric name
def get_most_recent_dir(base_dir="data"):
    if not os.path.isdir(base_dir):
        return None
    dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d)) and re.match(r'^\d{8}$', d)]
    if not dirs:
        return None
    most_recent_dir = max(dirs, key=lambda d: os.path.getmtime(os.path.join(base_dir, d)))
    return os.path.join(base_dir, most_recent_dir)

def read_text_file(filename, chunk_size=100000, date_cols=[], date_format= "%m/%d/%Y", **kwargs):
    """Read a text file and return a DataFrame."""
    print(f"Reading text file: {filename}")

    # Initialize an empty list to store the chunks
    chunks = []
    print("Estimating size of file:", filename)
    # Count the total number of rows without loading the entire file into memory
    with open(filename, 'r', encoding="ISO-8859-1") as f:
        reader = csv.reader(f)
        total_rows = sum(1 for row in reader) - 1  # Subtract 1 for the header

    print("Reading file:", filename)
    # Read the file in chunks with a progress bar
    with tqdm(total=total_rows, unit='rows') as pbar:
        for chunk in pd.read_csv(filename, chunksize=chunk_size, **kwargs):
            # Convert date columns to datetime
            # for those columns in the intersection of date_cols and chunk.columns
            for col in set(date_cols).intersection(chunk.columns):
                chunk[col] = pd.to_datetime(chunk[col], format=date_format, errors='coerce')
            chunks.append(chunk)
            pbar.update(len(chunk))

    # Concatenate all chunks into a single DataFrame
    df = pd.concat(chunks, ignore_index=True)
    print(f"Finished reading file: {filename}")
    return df

def write_parquet(df, filename, chunks=True, chunk_size=1000000):
    """Write a DataFrame to a Parquet file, optionally in chunks with a progress bar."""
    print(f"Writing DataFrame to Parquet file: {filename}")
    if chunks:
        # Write the DataFrame in chunks with a progress bar
        total_rows = df.shape[0]
        with tqdm(total=total_rows, unit='rows') as pbar:
            for start in range(0, total_rows, chunk_size):
                end = min(start + chunk_size, total_rows)
                # df[start:end].to_parquet(filename, engine='fastparquet', append=(start != 0), object_encoding='utf8')
                fastparquet.write(filename, df[start:end], append=(start != 0), object_encoding='utf8')
                pbar.update(end - start)
    else:
        # Write the entire DataFrame to a Parquet file
        fastparquet.write(filename, df)
    print(f"Parquet file saved: {filename}")

def read_parquet(filename, **kwargs):
    """Read a Parquet file, with a progress bar if its in chunks."""
    print(f"Reading Parquet file: {filename}")
    # Open the Parquet file with fastparquet
    pf = fastparquet.ParquetFile(filename)
    if len(pf.row_groups) > 1:
        # Get the total number of rows in the Parquet file
        total_rows = sum(rg.num_rows for rg in pf.row_groups)
        
        # Initialize an empty list to store the chunks
        chunks = []

        # Read the Parquet file in chunks with a progress bar
        with tqdm(total=total_rows, unit='rows') as pbar:
            for chunk in pf.iter_row_groups(**kwargs):
                # Convert the Parquet file to a pandas DataFrame
                chunks.append(chunk)
                pbar.update(len(chunk))

        # Concatenate all chunks into a single DataFrame
        df = pd.concat(chunks, ignore_index=True)
    else:
        # Read the entire Parquet file in one shot
        df = pf.to_pandas(**kwargs)
    print(f"Parquet file read: {filename}")
    return df

def write_sqlite(df, table_name, dbcon, chunk_size=10000):
    """Write a DataFrame to a SQLite database table in chunks with a progress bar."""
    print(f"Writing to SQLite table: {table_name}")
    total_rows = df.shape[0]
    # drop the table if it exists
    # dbcon.execute(sa.text(f"DROP TABLE IF EXISTS {table_name}"))
        # Convert datetime columns to strings
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%d')
    # for col in df.columns:
    #     # if the column name ends in '_dt' or '_date', convert it to a string
    #     if (col.endswith('_dt') or col.endswith('_date')) and df[col].dtype == 'object':
    #         df[col] = pd.to_datetime(df[col], unit='ns').dt.strftime('%Y-%m-%d')

    with tqdm(total=total_rows, unit='rows', desc=table_name) as pbar:
        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            exists_action = 'append' if start != 0 else 'replace'
            df[start:end].to_sql(table_name, dbcon, if_exists=exists_action, index=False)
            pbar.update(end - start)
    print(f"SQLite table written: {table_name}")

def main():
    # Get the default directory
    default_datadir = get_most_recent_dir() or "datadir"

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process text files.')
    parser.add_argument('-d', '--datadir', type=str, default=default_datadir, help='Directory containing the data files')
    # -a, --absentee-file parses the absentee file, otherwise it is ignored
    # -v, --voter-file parses the voter file, otherwise it is ignored    
    # -s, --voterhist-file parses the voter history file, otherwise it is ignored
    parser.add_argument('-a', '--absentee-file', action='store_true', help='Parse the absentee file')
    parser.add_argument('-v', '--voter-file', action='store_true', help='Parse the voter file')
    parser.add_argument('-s', '--voterhist-file', action='store_true', help='Parse the voter history file')
    # -i, --init-db initializes the database, otherwise it is updated.
    parser.add_argument('-i', '--init-db', action='store_true', help='Initialize the database')
    # --no-sqlite skips writing to SQLite
    parser.add_argument('--no-sqlite', action='store_true', help='Skip writing to SQLite')
    # --no-parquet skips writing to Parquet
    parser.add_argument('--no-parquet', action='store_true', help='Skip writing to Parquet')

    args = parser.parse_args()

    DATADIR = args.datadir
    ABSENTEE_FILE = os.path.join(DATADIR, "absentee_20241105.csv")
    VOTER_FILE = os.path.join(DATADIR, "ncvoter_Statewide.txt")
    VOTERHIST_FILE = os.path.join(DATADIR, "ncvhis_Statewide.txt")
    DBFILE = "ncdata.db"
    TIMESTAMP = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")

    # If a database file already exists, save a back up of it with a timestamp
    if os.path.exists(DBFILE) and args.init_db and not args.no_sqlite:
        os.rename(DBFILE, f"{DBFILE}.{TIMESTAMP}.bak")
    
    print(f"Processing raw text files in directory: {DATADIR}")

    if not args.no_sqlite:
        e = sa.create_engine(f'sqlite:///{DBFILE}', echo=False)
        dbcon = e.connect()
    else:
        dbcon = None

    def process_absentee_file(absentee_file, dbcon):
        print(f"Processing absentee file: {absentee_file}")
        # Read the absentee file
        abs = read_text_file(absentee_file, encoding="ISO-8859-1", 
                        dtype=data_layout.abs_dtypes,
                        date_cols=data_layout.abs_dates,
                        date_format=data_layout.abs_dateformat)
        if not args.no_sqlite: write_sqlite(abs, 'absentee', dbcon)
        if not args.no_parquet: write_parquet(abs, 'absentee_20241105.parquet')
        del abs # shouldnt be needed, but just in case
        print("Finished processing absentee file.")

    def process_voter_file(voter_file, dbcon):
        print(f"Processing voter file: {voter_file}")
        # Read the voter file
        voter_reg = read_text_file(voter_file, sep='\t', encoding="ISO-8859-1",
                          dtype=data_layout.voter_dtypes,
                          date_cols=data_layout.voter_dates,
                          date_format=data_layout.voter_dateformat)
        if not args.no_sqlite: write_sqlite(voter_reg, 'voter_reg', dbcon)
        if not args.no_parquet: write_parquet(voter_reg, 'ncvoter_Statewide.parquet')
        del voter_reg
        print("Finished processing voter file.")

    def process_voterhist_file(voterhist_file, dbcon):
        print(f"Processing voter history file: {voterhist_file}")
        # Read the voter history file
        voter_hist = read_text_file(voterhist_file, sep='\t', encoding="ISO-8859-1",
                          dtype=data_layout.voterhist_dtypes,
                          date_cols=data_layout.voterhist_dates,
                          date_format=data_layout.voterhist_dateformat)
        if not args.no_sqlite: write_sqlite(voter_hist, 'voter_hist', dbcon)
        if not args.no_parquet: write_parquet(voter_hist, 'ncvhis_Statewide.parquet')
        del voter_hist
        print("Finished processing voter history file.")

    if args.absentee_file: process_absentee_file(ABSENTEE_FILE, dbcon)
    
    if args.voter_file: process_voter_file(VOTER_FILE, dbcon)

    if args.voterhist_file: process_voterhist_file(VOTERHIST_FILE, dbcon)

    print("Completed processing files.")

if __name__ == "__main__":
    main()
