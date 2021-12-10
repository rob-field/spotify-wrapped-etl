-- Create Song Table
CREATE TABLE IF NOT EXISTS songs(
    unique_id TEXT PRIMARY KEY NOT NULL,
    song_id TEXT NOT NULL,
    song_name TEXT,
    song_artist TEXT,
    song_duration_ms INT,
    song_explicit INT,
    song_track_number INT,
    song_popularity INT,
    played_at TEXT,
    unix_timestamp FLOAT,
    song_uri TEXT,
    album_id TEXT,
    artist_id TEXT
    );

-- Create Album Table
CREATE TABLE IF NOT EXISTS albums(
    album_id TEXT PRIMARY KEY NOT NULL,
    album_name TEXT,
    release_date TEXT,
    release_year SMALLINT,
    total_tracks SMALLINT,
    album_uri TEXT
    );

-- Create Artist Table
CREATE TABLE IF NOT EXISTS artists(
    artist_id TEXT PRIMARY KEY NOT NULL,
    artist_name TEXT,
    artist_uri TEXT
    );
    