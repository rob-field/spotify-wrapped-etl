import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import func

from operator import itemgetter
import numpy as np

import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import seaborn as sns

Base = automap_base()

engine = create_engine("")

# reflect the tables
Base.prepare(engine, reflect=True)

db_session = Session(engine)

# mapped classes are now created with names by default, matching that of the table name.
albums = Base.classes.albums
artists = Base.classes.artists
songs = Base.classes.songs


# Total playback time
def total_playback_func():
    return db_session.query(func.sum(songs.song_duration_ms)).all()[0][0]

# Total Number of Artists
def total_artists_func():
    return db_session.query(func.count(artists.artist_id)).all()[0][0]

# Most Common Artist
def most_common_artist_func():
    top_artist_id = sorted(db_session.query(songs.artist_id, func.count(songs.artist_id)).group_by(songs.artist_id).all(), key=itemgetter(1), reverse=True)[0][0]
    
    return [x.artist_name for x in db_session.query(artists).filter(artists.artist_id==top_artist_id).all()][0]

# Top 3 Artists
def top_three_artists_func():
    top_three_artist_ids = sorted(db_session.query(songs.artist_id, func.count(songs.artist_id)).group_by(songs.artist_id).all(), key=itemgetter(1), reverse=True)[:3]
    top_artist_list = []
    for i in top_three_artist_ids:
        top_artist_list.append(db_session.query(artists.artist_name).filter(artists.artist_id==i[0]).all()[0][0])
    top_artist_df = pd.DataFrame(top_artist_list, columns=["Artist"])
    
    return top_artist_df

# Most Common Album
def most_common_album_func():
    top_album_id = sorted(db_session.query(songs.album_id, func.count(songs.album_id)).group_by(songs.album_id).all(), key=itemgetter(1), reverse=True)[0][0]

    top_album_artist = list(db_session.query(albums.album_name, artists.artist_name
                ).select_from(songs
                             ).join(albums, songs.album_id == albums.album_id
                                   ).join(artists, songs.artist_id == artists.artist_id
                                               ).filter(albums.album_id==top_album_id).all()[0])
    return top_album_artist

# Top 5 songs
# To Improve
def top_songs_func():
    top_five_song_ids = sorted(db_session.query(songs.song_id, func.count(songs.song_id)).group_by(songs.song_id).all(), key=itemgetter(1), reverse=True)[:5]
    top_five_song_ids = [x[0] for x in top_five_song_ids]
    song_list = []
    artist_list = []
    for i in top_five_song_ids:
        song_list.append(db_session.query(songs.song_name).filter(songs.song_id==i).all()[0])
        artist_list.append(db_session.query(songs.song_artist).filter(songs.song_id==i).all()[0][0])

    song_df = pd.DataFrame(song_list, columns=["Songs"])
    song_df["Artists"] = artist_list
    song_df.index = np.arange(1, len(song_df)+1)
    return song_df

# Top 3 decades - based on release date
def top_decades_func():
    decade_2020s = db_session.query(albums.album_name).filter(albums.release_year>=2020, albums.release_year<=2029).all()
    decade_2010s = db_session.query(albums.album_name).filter(albums.release_year>=2010, albums.release_year<=2019).all()
    decade_2000s = db_session.query(albums.album_name).filter(albums.release_year>=2000, albums.release_year<=2009).all()
    decade_1990s = db_session.query(albums.album_name).filter(albums.release_year>=1990, albums.release_year<=1999).all()
    decade_1980s = db_session.query(albums.album_name).filter(albums.release_year>=1980, albums.release_year<=1989).all()
    decade_1970s = db_session.query(albums.album_name).filter(albums.release_year>=1970, albums.release_year<=1979).all()
    decade_1960s = db_session.query(albums.album_name).filter(albums.release_year>=1960, albums.release_year<=1969).all()
    decade_1950s = db_session.query(albums.album_name).filter(albums.release_year>=1950, albums.release_year<=1959).all()
    decade_1940s = db_session.query(albums.album_name).filter(albums.release_year>=1940, albums.release_year<=1949).all()
    
    decade_dict = {'2020s':len(decade_2020s), '2010s':len(decade_2010s), '2000s':len(decade_2000s), '1990s':len(decade_1990s), 
                         '1980s':len(decade_1980s), '1970s':len(decade_1970s), '1960s':len(decade_1960s), '1950s':len(decade_1950s), '1940s':len(decade_1940s)}
    
    decade_df = pd.DataFrame(decade_dict.items(), columns=["Decade", "Number of Albums"])
    decade_df = decade_df[decade_df["Number of Albums"] != 0]

    return decade_df


# Song Popularity - Songs over certain popularity threshold
# Popularity based on total number of plays the track has had and how recent those plays are
def most_popular_songs_func():
    popular_df = pd.DataFrame([x for x in sorted(set(db_session.query(songs.song_name, songs.song_artist, songs.song_popularity).filter(songs.song_popularity>50).all()), key=itemgetter(1), reverse=True)],
                              columns=["Song", "Artist", "Popularity"])
    popular_df = popular_df.sort_values(by=['Popularity'], ascending=False)

    return popular_df[:10]


# Number of explicity vs non-explicity
def explicit_songs_func():
    non_explicit = len(db_session.query(songs.song_explicit).filter(songs.song_explicit==0).all())
    explicit = len(db_session.query(songs.song_explicit).filter(songs.song_explicit==1).all())
    return f"Non-explicit songs: {non_explicit}, Explicit songs: {explicit}"



# Plottting when songs were listened to
def listening_timeline_func():
    date_list = db_session.query(songs.played_at).all()

    dates = []
    count = []
    for x in date_list:
        dates.append(datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S'))
        count.append(1)

    fig = plt.subplots(figsize=(50,20))
    plt.axhline(y=1, color='r', linestyle='-')
    plt.scatter(dates, count, s=500)
    plt.tick_params(
        axis='y',
        left=False,
        labelleft=False)
    plt.title("Timeline of Playback")
    plt.savefig('listening_plot.png')
    plt.close()
    
    return fig

def email_function():
    
    sender_email = ""
    receiver_email = ""
    password = ""

    message = MIMEMultipart("alternative")
    message["Subject"] = "Spotify ETL Weekly Wrapped"
    message["From"] = sender_email
    message["To"] = receiver_email
    
    total_play_time = total_playback_func()
    total_artists = total_artists_func()
    most_common_artist = most_common_artist_func()
    top_three_artists = top_three_artists_func()
    most_common_album = most_common_album_func()
    top_five_songs = top_songs_func()
    top_decades = top_decades_func()
    most_popular_songs = most_popular_songs_func()
    listening_timeline_func()
    
    today = datetime.today().date()
    six_days_ago = today - timedelta(days=6)
    
    # Create the plain-text and HTML version of your message
    text = f"""\
    Weekly Summary for Spotify
    
    Includes Listening Data from {six_days_ago} - {today}
    
    Total Listening Time: 
    {total_play_time/1000:.2f} seconds
    {total_play_time/60000:.2f} minutes
    {total_play_time/3600000:.3f} hours
    
    Total Artists Listend To: 
    {total_artists}
    
    Most Frequently Listened to Artist: 
    {most_common_artist} 
    
    Top Three Artists:
    {top_three_artists}
    
    Most Listened To Album:
    {most_common_album[0]} by {most_common_album[1]}
    
    Top 5 Songs:
    (Based on number of plays)
    {top_five_songs}
    
    Top Decades:
    {top_decades}
    
    Favourite Songs:
    {"listening_plot.png"}
    
    """
    
    html = f"""\
    <html>

      <body>
          <h1>
          Weekly Summary for Spotify
          </h1>
          <h3>
          Includes Listening Data from {six_days_ago} - {today}
          </h3>
        <p>
           <b>Total Listening Time:</b> <br>
           {total_play_time/1000:.2f} seconds <br>
           {total_play_time/60000:.2f} minutes <br>
           {total_play_time/3600000:.3f} hours <br>
           <br>
           
           <b>Total Artists Listened To:</b>  <br>
           {total_artists} <br>
           <br>
           
           <b>Most Frequently Listened to Artist:</b>  <br>
           {most_common_artist} <br>
           <br>
           
           <b>Top Three Artists:</b>  <br>
           {top_three_artists.to_html(index=False)} <br>
           <br>
           
           <b>Most Listened To Album:</b> <br>
           {most_common_album[0]} by {most_common_album[1]} <br>
           <br>
           
           <b>Top 5 Songs:</b>  <br>
           (Based on number of plays) <br>
           {top_five_songs.to_html()} <br>
           <br>
           
           <b>Top Decades:</b>  <br>
           {top_decades.to_html(index=False)} <br>
           <br>
           
           <b>Favourite Songs:</b>  <br>
           {most_popular_songs.to_html(index=False)} <br>
           <br>
           
           <b>Listening Timeline:</b> <br>
           <img src="cid:listening_plot" alt="Logo" style="width:1000px;height:500px;"><br>
        </p>
      </body>
    </html>
    """

    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)
    
    # This example assumes the image is in the current directory
    fp = open('listening_plot.png', 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()
    
    msgImage.add_header('Content-ID', '<listening_plot>')
    message.attach(msgImage)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(
            sender_email, receiver_email, message.as_string()
        )
        
    return "Email Sent"

if __name__ == "__main__":
    email_function()