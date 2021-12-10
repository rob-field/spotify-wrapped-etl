import pandas as pd
import numpy as np
import json
from time import strftime
from datetime import datetime, timedelta
import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
import psycopg2
from sqlalchemy import create_engine

def spotify_etl():
    # Setting up spotify webscraping with spotipy
    SPOTIFY_CLIENT_ID = ""
    SPORTIFY_CLIENT_SECRET = ""
    SPORTIFY_REDIRECT_URI = ""


    auth_manager = SpotifyOAuth(client_id=SPOTIFY_CLIENT_ID,
                                client_secret=SPORTIFY_CLIENT_SECRET,
                                redirect_uri=SPORTIFY_REDIRECT_URI,
                                scope="user-read-recently-played")

    sp = spotipy.Spotify(auth_manager=auth_manager)
    
    last_hour_date_time = datetime.now() - timedelta(hours = 1)
    last_hour_unix = last_hour_date_time.strftime("%s")
    # Retrieve listening history from the last hour
    listening_history = sp.current_user_recently_played(limit=50,
                                                       after=last_hour_unix)

    if len(listening_history) == 0:
        print("error - no data collected")

    # Ablbum Info
    album_info = []

    for x in listening_history["items"]:
        album_id = x["track"]["album"]["id"]
        album_name = x["track"]["album"]["name"]
        release_date = x["track"]["album"]["release_date"]
        total_tracks = x["track"]["album"]["total_tracks"]
        album_uri = x["track"]["album"]["uri"]

        album_dict = {'album_id':album_id, 'album_name':album_name, 'release_date':release_date, 'total_tracks':total_tracks, 'album_uri':album_uri}
        album_info.append(album_dict)

    # Artist Info
    artist_info = []

    for x in listening_history["items"]:
        artist_id = x["track"]["artists"][0]["id"]
        artist_name = x["track"]["artists"][0]["name"]
        artist_uri = x["track"]["artists"][0]["uri"]

        aritst_dict = {'artist_id':artist_id, 'artist_name':artist_name, 'artist_uri':artist_uri}
        artist_info.append(aritst_dict)

    # Song info
    song_info = []

    for x in listening_history["items"]:
        song_id = x["track"]["id"]
        song_name = x["track"]["name"]
        song_artist = x["track"]["artists"][0]["name"]
        song_duration_ms = x["track"]["duration_ms"]
        song_explicit = x["track"]["explicit"]
        song_track_number = x["track"]["track_number"]
        song_popularity = x["track"]["popularity"]
        played_at = x["played_at"]
        song_uri = x["track"]["uri"]
        album_id = x["track"]["album"]["id"]
        artist_id = x["track"]["artists"][0]["id"]

        song_dict = {'song_id':song_id, 'song_name':song_name, 'song_artist':song_artist, 'song_duration_ms':song_duration_ms, 'song_explicit':song_explicit,
                     'song_track_number':song_track_number, 'song_popularity':song_popularity, 'played_at':played_at, 'song_uri':song_uri,
                    'album_id':album_id, 'artist_id':artist_id}
        song_info.append(song_dict)

    # Create dataframes for collected album/artist info list
    # Remove duplicates
    album_df = pd.DataFrame(album_info)
    album_df = album_df.drop_duplicates(subset=["album_id"])

    artist_df = pd.DataFrame(artist_info)
    artist_df = artist_df.drop_duplicates(subset=["artist_id"])

    # Create dataframe for song info list
    song_df = pd.DataFrame(song_info)

    # Feature engineering - albums
    # Reduce album release date to just year
    album_df["release_year"] = (album_df["release_date"].str[:4]).astype(int)
    album_df["total_tracks"] = (album_df["total_tracks"]).astype(int)
    
    # Rearrange column order
    album_df = album_df[['album_id', 'album_name', 'release_year', 'total_tracks', 'album_uri']]
        
    # Feature Engineering - songs
    # Convert boolean values to bit (int)
    song_df["song_explicit"] = song_df["song_explicit"].astype(int)

    # Convert time played at to datetime and GMT
    song_df["played_at"] = pd.to_datetime(song_df["played_at"], format="%Y-%m-%d %H:%M:%S")
    song_df["played_at"] = song_df["played_at"].dt.tz_convert('GMT')
    song_df["played_at"] = song_df["played_at"].dt.strftime('%Y-%m-%d %H:%M:%S')
    song_df["played_at"] = pd.to_datetime(song_df["played_at"])
    song_df["unix_timestamp"] = (pd.to_datetime(song_df["played_at"]).view(int) / 10**9)
    song_df["unique_id"] = song_df["song_id"] + "-" + song_df["unix_timestamp"].astype(str)

    # Rearrange column order
    song_df = song_df[['unique_id', 'song_id', 'song_name', 'song_artist','song_duration_ms', 'song_explicit', 'song_track_number', 'song_popularity', 'played_at', 'unix_timestamp', 'song_uri', 'album_id', 'artist_id']]
    
    conn = psycopg2.connect(host="localhost", port="5432", user="postgres", password="abcd1234", dbname="spotifydb")
    cur = conn.cursor()
    
    engine = create_engine("postgresql+psycopg2://postgres:abcd1234@localhost:5432/spotifydb")
    conn_eng = engine.raw_connection()
    cur_eng = conn_eng.cursor()

    
    cur_eng.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_albums AS SELECT * FROM albums LIMIT 0")
    album_df.to_sql("tmp_albums", con = engine, if_exists="append", index=False)

    cur.execute(
        """
        INSERT INTO albums
        SELECT tmp_albums.*
        FROM tmp_albums
        LEFT JOIN albums USING (album_id)
        WHERE albums.album_id IS NULL;

        DROP TABLE tmp_albums""")

    conn.commit()

    cur_eng.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_artists AS SELECT * FROM artists LIMIT 0")
    artist_df.to_sql("tmp_artists", con = engine, if_exists="append", index=False)

    cur.execute(
        """
        INSERT INTO artists
        SELECT tmp_artists.*
        FROM tmp_artists
        LEFT JOIN artists USING (artist_id)
        WHERE artists.artist_id IS NULL;

        DROP TABLE tmp_artists""")

    conn.commit()

    cur_eng.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_songs AS SELECT * FROM songs")
    song_df.to_sql("tmp_songs", con = engine, if_exists="append", index=False)

    cur.execute(
        """
        INSERT INTO songs
        SELECT tmp_songs.*
        FROM tmp_songs
        LEFT JOIN songs USING (unique_id)
        WHERE songs.unique_id IS NULL;

        DROP TABLE tmp_songs""")

    conn.commit()
    
    return "Complete"


if __name__ == "__main__":
    spotify_etl()