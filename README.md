# My recent played tracks on Spotify

This project was based on a similar project by Karolina Sowinska from the youtube channel [Karolina Sowinska](https://www.youtube.com/c/KarolinaSowinska). Her project was developed on a series of 4 videos that are listed below.

- [Data Engineering Course for Beginners - #1 EXTRACT](https://www.youtube.com/watch?v=dvviIUKwH7o)
- [Data Engineering Course for Beginners - #2 TRANSFORM](https://www.youtube.com/watch?v=X-phMpEp6Gs)
- [Data Engineering Course for Beginners - #3 LOAD](https://www.youtube.com/watch?v=rvPtpOjzVTQ)
- [Airflow for Beginners - Run Spotify ETL Job in 15 minutes!](https://www.youtube.com/watch?v=i25ttd32-eo)

## Requirements

- Spotify Account
- Spotify App on the Spotify for developeders website

### Libraries

- [Spotipy](https://spotipy.readthedocs.io/en/2.19.0/)
- [Pandas](https://pandas.pydata.org/pandas-docs/stable/)
- [SqlAlchemy](https://www.sqlalchemy.org/docs/index.html)
- [Psycopg2](https://www.postgresql.org/docs/9.5/static/libpq-connect.html) (You only need this if you choose to use PostgreSQL as your RDBMS)

## Setup

1. Clone the repository

```bash
git clone https://github.com/lilacostaro/spotify_spotipy.git
```

2. Create a virtual environment

```bash
python -m venv venv
```

3. Activate the virtual environment

```bash
./venv/scripts/activate
```

4. Install the requirements

```bash
pip install -r requirements.txt
```

5. Create a Spotify Account if you don't have one

- [Spotify](https://www.spotify.com/us/)

6. Create a Spotify App on Spotify for Developers

    6.1 - Access the link below and log in
    - [Spotify for Developers](https://developer.spotify.com/dashboard/) 

    6.2 - Create a new app
    - Click on CREATE AN APP
    ![Create a new app](/images/dashboard.png)
    - Enter a name and a description for your app, and click on CREATE
    ![Enter a name and a description for your app](/images/create_app_2.png)

    6.3 - Get the App Credentials
    - Click on SHOW CLIENT SECRET
    ![Show Client Secret](/images/client_id.png)
    - Copy the Client ID and Client Secret
    - Click on ADD SETTINGS
    ![Add Settings](/images/client_secret.png)
    - Go to Redirect URIs, and pass a uri, I suggest you use: 'http://localhost:8081' so your terminal can retrieve the url automatically, if you want to use this on airflow for example, this is essential.
    - Click on Save
    ![Redirect Uri](/images/redirect_uri.png)
    - Copy the Client ID, Client Secret and Redirect URI


7. Modify the code to use the credentials

- Go to the file named my_recent_played_tracks.py and change the lines below:

```python
SPOTIPY_CLIENT_ID = 'YOUR_CLIENT_ID' # line 64
SPOTIPY_CLIENT_SECRET = 'YOUR_CLIENT_SECRET' # line 65
SPOTIPY_REDIRECT_URI = 'YOUR_REDIRECT_URI' # line 69
```

8. Create the Directories for the backup files

- Create a directory named 'backup'
- Inside the 'backup' directory create 4 directories, a directory named 'songs', a directory named 'spotify_artists', a directory named 'spotify_albums' and a directory named 'songs_artists'

## Usage

1. Run the code

```bash	
python my_recent_played_tracks.py
```

Obs.: In the repository you're going to find a sqlite database named 'my_spotify_data.sqlite' that alredy have all the tables created. And the connection to this database it's configured on the code. If you want to use another RDBMS, the sql code used to create the tables are available in this [file](/sql_statements/create_database_tables.sql). And the code to connect to postgresql is available in the my_recent_played_tracks.py, you only need to uncomment the lines(243, 245, 246, 247, 249, 250)and pass your credentials. And comment the lines(255, 257, 258, 259, 261, 262).

## 