"""
This file reads and uploads Tesla Demo data files to a database.
The existing plan is to use an AWS PostgreSQL database,
    then connect Graphana or Tableau Public to it.
We also want to run PyOD on the data to see if we can detect outliers.
"""
from pathlib import Path
import pandas as pd
from test_connection import define_conn


# scan directory
def scan_dir(data_dir: str=None):
    """
    Scan data_dir for .csv files.
    args:
        data_dir, a filepath (string or pathlib.Path object)
    returns:
        filepaths, a list of pathlib.Path() filepaths
    """
    if data_dir is None:
        data_dir = Path(r"/Users/evan/github/demo_20240205/data/")

    # no error handling here; if real, guard clauses!
    filepaths = [f for f in data_dir.glob('*.csv')]
    return filepaths


def parse_filename(f: Path):
    """
    Parse a filename and remove the tool ID and run_timestamp.
    args:
        f, a str or pathlib.Path object
    returns:
        tool: the tool name (str)
        run_timestamp: the run_timestamp of the measurement
    """
    filename = f.name
    filename_parts = filename.replace(".csv", "").split("_")
    tool = filename_parts[0]  # str
    run_timestamp = filename_parts[1] + " " + filename_parts[2]
    # print(tool, run_timestamp)

    return tool, run_timestamp


def make_df(fp: Path):
    """
    For a given file(path) f, use pd.read_csv() and create a dataframe.
    args:
        f, a filepath
    returns: 
        df, a pandas DataFrame
    """
    tool, run_timestamp = parse_filename(fp)
    df = pd.read_csv(fp)
    df.columns = ["time", "value"]  # manually assign
    df["tool"] = tool
    df["run_timestamp"] = pd.to_datetime(run_timestamp)
    df["filename"] = fp.name
    df = flag_outliers(df)
    # print(df.sample(2))
    return df


def make_df_list(file_list: list):
    """
    for each file in list file_list, make a df
    args:
        file_list, a list of filepaths
    returns:
        dfs, a list of pandas dataframes
    """
    dfs = []
    for fp in file_list:
        # check if file in database. if it is, move it to duplicate directory
        exists = check_if_exists(fp.name, conn)
        if exists:
            new_fp = fp.parent / "duplicate" / fp.name
            print(f"File already in database. Moving to {new_fp}")
            fp.rename(new_fp)

        if not exists:
            df = make_df(fp)
            dfs.append(df)

    # print(f"The df list is {len(dfs)} long.")
    return dfs


def check_if_exists(f, conn):
    """
    Check if the data has already been uploaded to the database.
    args:
        f: the run filename
    returns:
        True if the run is already in the database
        False if it is not
    """
    # get a count of rows in table with this filename
    num_rows = pd.read_sql_query(f"""
        SELECT COUNT(*)
        FROM meas_values
        WHERE filename = '{f}'
        """, conn).iloc[0, 0]

    if num_rows != 0:
        return True
    else:
        return False


def upload_dfs(dfs: list, engine):
    """
    For each df in the list dfs, upload to SQL db.
    args:
        dfs, a list of dfs
        conn, a psycopg2 database connection
    """
    for df in dfs:
        # be careful to use `engine` and not `conn` for SQLAlchemy!
        df.to_sql('meas_values', engine, if_exists="append", index=False)


def move_uploaded_files(file_l):
    """
    Move uploaded files to the ./data/uploaded folder
    args:
        file_l, a list of filepaths
    """

    for fp in file_l:
        fp.replace(fp.parent / "uploaded" / fp.name)


def flag_outliers(df):
    """
    Flag any points as outliers that are > 103 or < 97.
    args:
        df: a pandas DataFrame
    """
    df["outlier"] = False
    mask = (df["value"].gt(103) | df["value"].lt(97))
    df.loc[mask, "outlier"] = True
    return df


if __name__ == "__main__":
    engine, conn = define_conn()
    file_l = scan_dir()
    dfs = make_df_list(file_l)
    upload_dfs(dfs, engine)
    move_uploaded_files(file_l)
