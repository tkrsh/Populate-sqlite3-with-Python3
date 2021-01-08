"""
sqlite3 for read/write operations on the db
"""
import sqlite3
import pandas as pd

def get_cursor(file, table):
    """
    Returns cursor -> takes file,table prints satus.
    """
    try:
        con = sqlite3.connect(file)
        cur = con.cursor()
        cur.execute("SELECT * FROM {}".format(table))
        print("Successfully Connected to Databse", file)
    except SystemError:
        print("Connection Failed")
    return cur, con


def print_status(cur):
    """
    Prints status of the cursor
    """
    names = [description[0] for description in cur.description]
    fetch = cur.fetchall()
    lastrow_id = cur.lastrowid
    print("Fields-> {}\nData-> {}\nlastrow-> {}".format(names, fetch, lastrow_id))


def gen_data(dataframe):
    """
    Takes dataframe object and returns formated data ready for insertion
    """
    dataframe.fillna(value="not_found", inplace=True)
    return dataframe.values


def write_data(cur, inp_data, inc_connection):
    """
    Perform insertion takes cursor and and prints status.
    """
    action = '''insert into {} values ({})'''.format(TABLE, ("?," * 9)[:-1])
    for i in inp_data:
        cur.execute(action, i)
    inc_connection.commit()


if __name__ == "__main__":
    df = pd.read_csv("codeforces.csv")
    df = df[['Unnamed: 0', 'username', 'rating_codeforces', 'name', 'city',
             'country', 'organization', 'rank_codeforces', 'leaderboard_rank']]
    FILE = '/home/tkrsh/Desktop/db.sqlite3'
    TABLE = 'main_ratings'
    try:
        cursor, connection = get_cursor(FILE, TABLE)
        print_status(cursor)
        data = gen_data(df)
        write_data(cursor, data, connection)
        print("Successfull Insertions")
    except OverflowError:
        print("Error Occured")
