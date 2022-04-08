# Importing built-in packages
from datetime import datetime
import pytz

# Importing third-party packages
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import sqlalchemy
# import psycopg2
import sqlite3


# Importing Global Variable from horario.py
from horario import HORARIO

# Defining the Log Function
def log(message):
    timestamp_format = '%d-%m-%Y %H:%M:%S:%f' # day-month-year hour:minute:second:microsecond
    now = datetime.now(tz=pytz.timezone('America/Sao_Paulo')) # get current timestamp in São Paulo timezone
    timestamp = now.strftime(timestamp_format)
    with open('spotify_logfile.txt', 'a') as f:
      f.write(timestamp + ', ' + message + '\n')

# Re-writing the horario.py file
def set_last_song(message):
    with open('horario.py', 'w') as f:
      f.write(f'HORARIO = "{message}"')

# Definenin the backup function  
def backup():
    today = datetime.now()
    date_for_backup = today.strftime('%d-%m-%Y_%H-%M')
    album_df.to_csv(f'backup\spotify_albuns\s_albuns_{date_for_backup}.csv')
    artist_df.to_csv(f'backup\spotify_artists\s_artists_{date_for_backup}.csv')
    song_df.to_csv(f'backup\songs\songs_{date_for_backup}.csv')
    artist_song_df.to_csv(f'backup\songs_artists\songs_artists_{date_for_backup}.csv')

# Validating the Dataset  
def check_if_valid_data(df: pd.DataFrame) -> bool:
# Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        log("No songs downloaded. Finishing execution")
        return False
    return True


log("Starting ETL Process for Spotify Recente Played Tracks")

# We are doing this so our backup files wont have repeated data
# Calling the global variable to get the time for the last song retrieved!
time = HORARIO

# transforming the time retrieved to unix_timestamp
new = f'{time[:10]} {time[11:-1]}'
hour = datetime.strptime(new, '%Y-%m-%d %H:%M:%S.%f')
# Since spotify returns the time in UTC timezone, we need to convert it to São Paulo timezone, by subtracting 3 hours,
# we also add 1 ,minute to the time, so we guarantee the last song won't be retrieved again 
unix_timestemp = int(hour.timestamp()) * 1000 - 10740000

# You will need a spotify account, and you will need to create an APP on this link: https://developer.spotify.com/dashboard/
# You can get the step-by-step on the README.md file
SPOTIPY_CLIENT_ID = 'Your_Client_ID'
SPOTIPY_CLIENT_SECRET = 'Your_Client_Secret'
# Using Local Host with a port enables the terminal to get the redirect url automatically, so you don't have to copy manually!
# That's essential for airflow be able to retrieve the redirect url
# Remember you have to add the URL at yours project settings.
SPOTIPY_REDIRECT_URI = 'http://localhost:8081'
# This tell spotify what end point we want the token to have access, if you want to get access for other endpoints, you have to add them here
# You can take a look in all endpoints, and what kind autorization they need on: https://developer.spotify.com/documentation/general/guides/authorization/scopes/
SCOPE = "user-read-recently-played"

log("Connecting to server and downloading the data!")
# Defining the token object, in this case, will need to pass the the following parameters: client_id, client_secret, redirect_uri, scope
token = SpotifyOAuth(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, redirect_uri=SPOTIPY_REDIRECT_URI, scope=SCOPE)

# Generating the token 
spotify = spotipy.Spotify(auth_manager=token)

# Calling the endpoint to get the data
data = spotify.current_user_recently_played(limit=50, after=unix_timestemp)
# print(data)
log("Data Downloaded Successfully")

# The artists info
artist_id = []
artist_name = []
artist_uri = []
artist_url = []
# We are gonna need this info for another table
song_id_rep = []

# The albuns info
album_id = []
artist_id_album = []
album_name = []
album_release_date = []
album_total_tracks = []
album_uri = []
album_url = []
album_cover_url = []

# Songs info
song_id = []
song_name = []
song_duration_ms = []
song_popularity = []
song_explicit = []
song_url = []
played_at = []
date = []

log("Start Extraction Process")
# Filling the list previously defined
for song in data["items"]:
    song_id_1 = song["track"]["id"]
    song_id.append(song["track"]["id"])
    song_name.append(song["track"]["name"])
    song_duration_ms.append(song["track"]["duration_ms"])
    song_popularity.append(song["track"]["popularity"])
    song_explicit.append(song["track"]["explicit"])
    song_url.append(song["track"]["external_urls"]["spotify"])
    played_at.append(song["played_at"])
    date.append(song["played_at"][:10])
    album_id.append(song["track"]["album"]["id"])
    artist_id_album.append(song["track"]["album"]["artists"][0]["id"])
    album_name.append(song["track"]["album"]["name"])
    album_release_date.append(song["track"]["album"]["release_date"])
    album_total_tracks.append(song["track"]["album"]["total_tracks"])
    album_uri.append(song["track"]["album"]["uri"])
    album_url.append(song["track"]["album"]["external_urls"]["spotify"])
    album_cover_url.append(song["track"]["album"]["images"][0]["url"])
    for artist in song["track"]["artists"]:
        artist_id.append(artist["id"])
        artist_name.append(artist["name"])
        artist_uri.append(artist["uri"])
        artist_url.append(artist["external_urls"]["spotify"])
        song_id_rep.append(song_id_1)
log("Finish extraction Process")

# Definining a dictionary for the songs  
song_dict = {
    'song_id': song_id,
    'song_name': song_name,
    'song_duration': song_duration_ms,
    'song_popularity': song_popularity,
    'song_explicit': song_explicit,
    'song_url': song_url,
    'played_at': played_at,
    'date': date,
    'album_id': album_id
}

# Definining a dictionary for the albuns
album_dict = {
    'album_id': album_id,
    'album_name': album_name,
    'album_release_date': album_release_date,
    'album_total_tracks': album_total_tracks,
    'album_uri': album_uri,
    'album_url': album_url,
    'album_cover_url': album_cover_url,
    'artist_id': artist_id_album
}

# Definining a dictionary for the artists
artists_dict = {
    'artist_id': artist_id,
    'artist_name': artist_name,
    'artist_uri': artist_uri,
    'artist_url': artist_url
}

# Defining a dictionary for the songs_artists
songs_artists_dict = {
    'song_id': song_id_rep,
    'artist_id': artist_id
}

log("Start Transformation Process")
# Defining the dataframes and making the necessary transformations
# Dataframe for the songs
song_df = pd.DataFrame(song_dict, columns= ['song_id',
                                            'song_name',
                                            'song_duration',
                                            'song_popularity',
                                            'song_explicit',
                                            'song_url',
                                            'played_at',
                                            'date',
                                            'album_id'])
song_df['date'] = pd.to_datetime(song_df['date'], format="%Y-%m-%d")

# Dataframe for the albuns
album_df = pd.DataFrame(album_dict, columns = ['album_id',
                                                'album_name',
                                                'album_release_date',
                                                'album_total_tracks',
                                                'album_uri',
                                                'album_url',
                                                'album_cover_url',
                                                'artist_id'])
album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'], format="%Y-%m-%d")
album_df = album_df.drop_duplicates(subset=['album_id'])

# DataFrame for the artists
artist_df = pd.DataFrame(artists_dict, columns = ['artist_id',
                                                  'artist_name',
                                                  'artist_uri',
                                                  'artist_url'])
artist_df = artist_df.drop_duplicates(subset=['artist_id'])

# DataFrame for the artist_songs
# Since a music can have more then one artist, and a artist have more than on song, we need to create a table to relate the songs to all its artists, and the artist
# to all its songs
artist_song_df = pd.DataFrame(songs_artists_dict, columns= ['song_id', 
                                                     'artist_id'])
# We need to concatenate the song_id and artist_id to create a unique id for each row
artist_song_df['unique_id'] = artist_song_df['song_id'] + '-' + artist_song_df['artist_id']
artist_song_df = artist_song_df.drop_duplicates(subset=['unique_id'])
log("Finish Transformation Process")

print(song_df)
print('\n\n')
print(album_df)
print('\n\n')
print(artist_df)
print('\n\n')
print(artist_song_df)

log("Start Loading Process")
# Validating the Data
# In this stage we are only going to check our principal table, since we only want to validate tha the dataset isn't empty
if check_if_valid_data(song_df):
    log("Data valid, proceed to Load stage")


    """ To use Postgres as your RDBMS, you can use the following code, just create the database and the tables with the sql code provided on the  
    create_database_tables.sql file and uncoment the code below. And comment the code for the sqlite database. You also need to install the psycopg2 library
    and import it in the code."""
    # Change the username to your database username, password to your database password and database_name to your database name
    # DATABASE = 'postgresql://username:password@localhost:5432/database_name'

    # engine = sqlalchemy.create_engine(DATABASE)
    # conn = psycopg2.connect(database="database_name", user="user_name", password="password")
    # cur = conn.cursor()
        
    # conn_eng = engine.raw_connection()
    # cur_eng = conn_eng.cursor()

    """If you aren't familiar with RDBMSs, you can use the sqlite database provided on this repository, it has all the tables you're gonna need.
    Alredy set up"""

    DATABASE = 'sqlite:///my_spotify_data.sqlite'

    engine = sqlalchemy.create_engine(DATABASE)
    conn = sqlite3.connect('my_spotify_data.sqlite')
    cur = conn.cursor()
        
    conn_eng = engine.raw_connection()
    cur_eng = conn_eng.cursor()

    log('Database connected')

    log("Start Loading Process")
    #TRACKS: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_tracks AS SELECT * FROM spotify_tracks LIMIT 0
    """)
    song_df.to_sql("tmp_track", con = engine, if_exists='append', index = False)
    conn.commit()
    #Moving data from temp table to production table
    cur.execute(
    """
    INSERT INTO spotify_tracks
    SELECT tmp_track.*
    FROM   tmp_track
    LEFT   JOIN spotify_tracks USING (played_at)
    WHERE  spotify_tracks.played_at IS NULL;

    """)
    cur.execute(
    """
    DROP TABLE tmp_track;

    """)
    conn.commit()
    log("Data Loaded to spotify_tracks")

    #ALBUM: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_album AS SELECT * FROM spotify_albuns LIMIT 0
    """)
    album_df.to_sql("tmp_album", con = engine, if_exists='append', index = False)
    conn_eng.commit()
    #Moving from Temp Table to Production Table
    cur.execute(
    """
    INSERT INTO spotify_albuns
    SELECT tmp_album.*
    FROM   tmp_album
    LEFT   JOIN spotify_albuns USING (album_id)
    WHERE  spotify_albuns.album_id IS NULL;

    """)
    cur.execute(
    """
    DROP TABLE tmp_album;

    """)
    conn.commit()
    log("Data Loaded to spotify_albuns")

    #Artist: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_artist AS SELECT * FROM spotify_artists LIMIT 0
    """)
    artist_df.to_sql("tmp_artist", con = engine, if_exists='append', index = False)
    conn_eng.commit()
    #Moving data from temp table to production table
    cur.execute(
    """
    INSERT INTO spotify_artists
    SELECT tmp_artist.*
    FROM   tmp_artist
    LEFT   JOIN spotify_artists USING (artist_id)
    WHERE  spotify_artists.artist_id IS NULL;

    """)
    cur.execute(
    """
    DROP TABLE tmp_artist;

    """)
    conn.commit()
    log("Data Loaded to spotify_artists")

    #Artists_songs: Temp Table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_artists_songs AS SELECT * FROM artists_song LIMIT 0
    """)
    artist_song_df.to_sql("tmp_artists_songs", con = engine, if_exists='append', index = False)
    conn_eng.commit()
    #Moving data from temp table to production table
    cur.execute(
    """
    INSERT INTO artists_song
    SELECT tmp_artists_songs.*
    FROM   tmp_artists_songs
    LEFT   JOIN artists_song USING (unique_id)
    WHERE  artists_song.unique_id IS NULL;

    """)
    cur.execute(
    """
    DROP TABLE tmp_artists_songs;

    """)
    conn.commit()
    log("Data Loaded to artists_songs")
    log("Loading Process Finished")
    
    conn.close()
    log("Connection closed")
    
    # Updating the file horario.py with the time of the last song retrieved, and calling the backup function
    if played_at:
        last_song = played_at[0]
        # set_last_song(last_song)
        log(f"Horario updated successfully - Set to {last_song}")
        backup()
        log("Successful Backup")

log('ETL job finished successfully!\n')


