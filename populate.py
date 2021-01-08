import pandas as pd
import sqlite3

def get_cursor(file,table):
    """
    Returns cursor -> takes file,table prints satus.
    """
    try:
        
        connection = sqlite3.connect(file)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM {}".format(table))
        print("Successfully Connected to Databse",file)
        return cursor,connection
        
    except:
        
        print("Connection Failed")
        
    return cursor,connection

def print_status(cursor):
    """
    Prints status of the cursor 
    """
    names = [description[0] for description in cursor.description]
    fetch = cursor.fetchall()
    lastrow_id= cursor.lastrowid
    print("Fields-> {}\nData-> {}\nlastrow-> {}".format(names,fetch,lastrow_id))
    
def gen_data(dataframe):
    """
    Takes dataframe object and returns formated data ready for insertion
    """
    dataframe.fillna(value="not_found",inplace=True)
    return dataframe.values

def write_data(cursor,data,connection):
    """
    Perform insertion takes cursor and and prints status.
    
    """
    action = '''insert into {} values ({})'''.format(table,("?,"*9)[:-1])
    
    for i in data:
        cursor.execute(action, i)
    
    connection.commit()

if __name__ == "__main__":
    
     
    df=pd.read_csv("codeforces.csv")    
    df=df[['Unnamed: 0','username','rating_codeforces','name','city','country','organization','rank_codeforces','leaderboard_rank']]
    file='/home/tkrsh/Desktop/db.sqlite3'
    table='main_ratings'

    try:
        
        cursor,connection=get_cursor(file,table) 
        print_status(cursor)
        data=gen_data(df)
        write_data(cursor,data,connection)
        print("Successfull Insertions")
    
    except:
        print("Error Occured")
    
    