# importando bibliotecas biult-in
from datetime import datetime
import pytz

# importando bibliotecas de terceiros
import spotipy
from spotipy.oauth2 import SpotifyOAuth

import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3

import pandas as pd

# importando meu modulo
from horario import Horario

# Definindo a função log
def log(message):
    timestamp_format = '%d-%m-%Y %H:%M:%S:%f' # day-month-year hour:minute:second:microsecond
    now = datetime.now(tz=pytz.timezone('America/Sao_Paulo')) # get current timestamp in São Paulo timezone
    timestamp = now.strftime(timestamp_format)
    with open('logfile.txt', 'a') as f:
      f.write(timestamp + ', ' + message + '\n')

# Reescreve o arquivo horario.py
def set_last_song(message):
    with open('horario.py', 'w') as f:
      f.write('def Horario():\n')
      f.write(f'    return "{message}"')

# Cria um arquivo.csv de backup das informaçoes obtidas     
def backup():
    today = datetime.now()
    date_for_backup = today.strftime('%d-%m-%Y_%H-%M')
    song_df.to_csv(f'backup\songs_{date_for_backup}.csv')

# Validação do dataset    
def check_if_valid_data(df: pd.DataFrame) -> bool:
# Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        log("No songs downloaded. Finishing execution")
        return False

# Primary key check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        log("Primary Key Check is violated")
        raise Exception('Primary Key Check is violated')

# Check for nulls
    if df.isnull().values.any():
        log("Null value found")
        raise Exception("Null value found")
    

    
# setando as variaveis globais
SPOTIPY_CLIENT_ID='Your client_id'
SPOTIPY_CLIENT_SECRET='Your Client_Secret'
SPOTIPY_REDIRECT_URI='Your Redirect URI'
SCOPE = "user-read-recently-played"
DATABASE = 'sqlite:///my_played_tracks.sqlite'


if __name__ == '__main__':

    log("ETL Job Started")
        
    # Chamando o horario da ultima musica baixada
    time = Horario()

    # trasformando o horario recebido para unix_timestamp
    new = f'{time[:10]} {time[11:-1]}'
    hora = datetime.strptime(new, '%Y-%m-%d %H:%M:%S.%f')
    # Como o spotify retorna um horario no fusorario UTC, precisa ser feita uma subtração de 3h no horario da ultima musica, 
    # e adicionar 1 minuto ao horario para garantir que a ultima musica não estaja no novo dataset
    unix_timestemp = int(hora.timestamp()) * 1000 - 10740000

    # Definindo os parametros para o token
    token = SpotifyOAuth(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, redirect_uri=SPOTIPY_REDIRECT_URI, scope=SCOPE)

    # Gerando o token
    spotify = spotipy.Spotify(auth_manager=token)
    print(f'{type(spotify)} \n\n')

    # chamando a url com os parametros definidos e armazenando o resultado na variavel data
    data = spotify.current_user_recently_played(limit=50, after=unix_timestemp)
    #print(data)
        
    log("Data Loaded")
        
    # Definindo listas vazias que receberam os dados obtidos na requisição
    song_id = []
    song_names = []
    artist_names = []
    song_popularity = []
    played_at_list = []
    timestamps = []

    # Buscando as informações desejadas no dataset adquirido
    for song in data["items"]:
            song_id.append(song["track"]["id"])
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            song_popularity.append(song["track"]["popularity"])
            played_at_list.append(song["played_at"])
            timestamps.append(song["played_at"][0:10])
                

    # Definindo o dicionario
    song_dict = {
        "song_id" : song_id,
        "song_name" : song_names,
        "artist_name" : artist_names,
        "song_popularity" : song_popularity,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    # Definindo o dataframe
    song_df = pd.DataFrame(song_dict, columns= ["song_id", "song_name", "artist_name", "song_popularity", "played_at", "timestamp"])

    # imprimindo o dataframe
    print(song_df)

    # Validando os dados obtidos
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")
        log("Data valid, proceed to Load stage")

    # Criando o cursor para conectar com o banco de dados
    engine = sqlalchemy.create_engine(DATABASE)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    # Definindo os parametros para a criação da tabela
    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_id VARCHAR(25),
        song_name VARCHAR(100),
        artist_name VARCHAR(100),
        song_popularity VARCHAR(10),
        played_at VARCHAR(100),
        timestamp VARCHAR(25),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    # Criando a tabela caso ela não exista
    cursor.execute(sql_query) # I made a typo, should be "cursor"
    print("Opened database successfully")
    log("Opened database successfully")

    # Inserindo os dados na tabela ou não, se eles já existirem
    try:
        song_df.to_sql('my_played_tracks', engine, index=False, if_exists="append")
    except:
        print('Data already exists in the database')
        log('Data already exists in the database')

    # Fechando a conexão com o banco de dados
    conn.close()
    print("Close database successfully")
    log("Close database successfully")

    # Atualizando o horario da ultima musica ouvida, o o arquivo de backup se o dataset não estiver vazio
    if played_at_list:
        last_song = played_at_list[0]
        set_last_song(last_song)
        log(f"Horario updated successfully - Set to {last_song}")
        backup()
        log("Successful Backup")
            
    log("ETL Job Finished Successfully\n")
        

