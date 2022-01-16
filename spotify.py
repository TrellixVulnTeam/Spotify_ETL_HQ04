from sqlite3.dbapi2 import Timestamp
from numpy.lib.function_base import append
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import date, datetime
import datetime
import sqlite3

DATABASE_LOCATION = 'sqlite:///my_played_tracks.sqlite'
USER_ID = 'jakub.uhrina'
TOKEN = 'BQAPH9YhOrq58HFrI3ehkQW76N9B0VoY35W3VAWp7Hhzpfp9DK7PFGGYGbEh08qr_O0beoilWwthSymy3W7A7QHT3ln4J1Klf0-iyGvp8LfExH51xPhbPxtlyq9qlYLTXMZq8b_eYVBhip1CHa0'

# Token -> https://developer.spotify.com/console/get-recently-played/?limit=10&after=1484811043508&before=

def check_if_valid_data(df: pd.DataFrame) -> bool:
    #Check if dateframe is empty
    if df.empty:
        print('No songs downloaded.')
        return False
    
    #Primary key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('Primary Key Check is violated')

    #Check for nulls
    if df.isnull().values.any():
        raise Exception

if __name__ == '__main__':

    headers = {
        'Accept' : 'application/json',
        'Content-Type' : 'application/json',
        'Authorization' : 'Bearer {token}'.format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=60)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix_timestamp),headers=headers)

    data = r.json()

    song_names = []
    artist_names = []
    player_at_list = []
    timestamp = []

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        player_at_list.append(song['played_at'])
        timestamp.append(song['played_at'][0:10])

    song_dict = {
        'song_name' : song_names,
        'artist_name' : artist_names,
        'played_at' : player_at_list,
        'timestamp' : timestamp
    }

    song_df = pd.DataFrame(song_dict,columns= ['song_name', 'artist_name', 'played_at', 'timestamp'])
    print(song_df)

    #Validate
    if check_if_valid_data(song_df):
        print('Data valid, proceed to Load stage')

    #Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = '''
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)

        )'''
    cursor.execute(sql_query)

    try:
        song_df.to_sql('my_played_tracks', engine, index=False, if_exists='append')
    except:
        print('Data already exists in the database')
    conn.close()
    print('Close database successfully ')