import requests
import streamlit as st
from streamlit_lottie import st_lottie
from PIL import Image
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import pymongo
import datetime
import mysql.connector
import re

def load_lottieurl(url):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()


st.set_page_config(page_title='YouTUBE', page_icon=":movie_camera:", layout="wide")

lottie_coding = load_lottieurl("https://lottie.host/8ea4186a-eb65-4c3c-b0e5-cd01ab315d33/jTyCSPOFYz.json")
lottie_coding1 = load_lottieurl("https://lottie.host/ae8a527f-44bb-4f37-ad21-9a72af6ad798/pyiIYEHL0E.json")
lottie_coding2 = load_lottieurl("https://lottie.host/70633239-d7e1-4966-a24b-9833f933a9fb/T8htUrHlii.json")
data_migration = Image.open(r"C:\Users\admin\PycharmProjects\pythonProject\images\sql.png")

with st.container():
    st.lottie(lottie_coding1, height=300, key="coding1")
    st.header("Welcome to YouTUBE :movie_camera:")
    st.title('YouTUBE Data Harvesting Project')
    st.write(" Data Harvesting and Warehousing using SQL, MongoDB and Streamlit ")
    st.write("[learn more>]('https://www.youtube.com/')")

API_KEY = 'AIzaSyBLTidzTSC69Y-CDVbKoTV2iRCRQ6HU_Kc'
Youtube =build('youtube','v3', developerKey=API_KEY)

def get_channel_data(channel_id):
    all_data = []
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    channel_response = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    ).execute()

    for item in channel_response['items']:
        data = {
            'Channel Name': item['snippet']['title'],
            'Channel Description': item['snippet']['description'],
            'Subscriber Count': item['statistics']['subscriberCount'],
            'View Count': item['statistics']['viewCount'],
            'Video Count': item['statistics']['videoCount']
        }

        if 'contentDetails' in item:
            data['Playlist ID'] = item['contentDetails'].get('relatedPlaylists', {}).get('uploads')
            data['Playlist NAME'] = item['contentDetails'].get('relatedPlaylists', {}).get('uploads')

        all_data.append(data)

    return all_data


def get_video_ids(Youtube, playlist_id):
    request = Youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    video_ids = []
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = Youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

                next_page_token = response.get('nextPageToken')

    return video_ids


def get_video_details(Youtube, video_id):
    all_video_stats = []

    for i in range(0, len(video_id), 50):
        request = Youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=','.join(video_id[i:i + 50])
        )
        response = request.execute()

        for video in response['items']:
            video_stats = dict(
                Video_Id= video['id'],
                Video_Title=video['snippet']['title'],
                Video_Description= video['snippet']['description'],
                Title=video['snippet']['title'],
                Published=video['snippet']['publishedAt'],
                Views=video['statistics']['viewCount'],
                Likes=video['statistics'].get('likeCount',0),
                Dislikes=video['statistics'].get('dislikeCount',0),
                Favorite_Count=video['statistics'].get('favoriteCount',0),
                Comment_Count= video['statistics'].get('commentCount',0),
                Duration=video['contentDetails']['duration'],
                Thumbnail= video['snippet']['thumbnails']['default']['url'],
                Caption_Status=video['contentDetails']['caption']
            )
            all_video_stats.append(video_stats)

    return all_video_stats

def get_comment_in_video(youtube, video_ids):
    all_comments = []

    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=20
            )
            response = request.execute()

            for comment in response['items']:
                get_comments_in_videos = {
                    "comment_id": comment["snippet"]["topLevelComment"]["id"],
                    "comment_text": comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "comment_author": comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "comment_publishedAt": comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                    "video_id": comment["snippet"]["videoId"]
                }
                all_comments.append(get_comments_in_videos)

        except HttpError as e:
            if e.resp.status == 404:
                print(f"Video not found for video ID: {video_ids}")
            else:
                pass

    return all_comments


Channel_ids = ['UCQYO2p7JMcCp-9xIZxGP2Sg', 'UCXhbCCZAG4GlaBLm80ZL-iA', 'UC9trsD1jCTXXtN3xIOIU8gg',
              'UCoCG7wgXEBOfyTfeFylzIaw', 'UCniI-BQk7qAtXNmmz40LSdg', 'UCzee67JnEcuvjErRyWP3GpQ',
              'UCYXVX9Iwupv0M-aSrxUHCXQ', 'UCa9c6LDV43ZfD89zsxkEyzg', 'UCtcTNRfGeHcHxEXKSUu84Fw',
              'UCBnxEdpoZwstJqC1yZpOjRA']

selected_channel_id = st.selectbox('Please select channel id from below list:', Channel_ids)

with st.container():
    st.write("------")
    left_column, right_column = st.columns(2)
    with left_column:
        st.header("Mongo DB")
        st.write("Data Extracting from YouTUBE to MongoDB")
        transfer = st.button('Extract')
    with right_column:
        st_lottie(lottie_coding, height=300, key="coding")


if transfer:
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    channel_data = get_channel_data(selected_channel_id)
    st.write(channel_data)

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['youtube']
    collection = db['channel_data']
    collection.insert_many(channel_data)

    playlist_id = channel_data[0].get('Playlist ID')
    if playlist_id:
        video_ids = get_video_ids(youtube, playlist_id)
        vid_details = get_video_details(youtube, video_ids)
        collection = db['video_details']
        collection.insert_many(vid_details)
        st.write(vid_details)

        # for video_id in video_ids:
        comment_data = get_comment_in_video(youtube, video_ids)

        if comment_data:  # Check if comment_data is not empty
            collection = db['comment_data']
            collection.insert_many(comment_data)
            st.write(comment_data)
        else:
            st.write("No comments found for video:", video_ids)


with st.container():
    st.write("------")
    left_column, right_column = st.columns(2)
    with right_column:
        st.header("SQL")
        st.write("Data Migrating from MongoDB to SQL")
        load = st.button('Load Data')
    with left_column:
        st_lottie(lottie_coding2 , height=300, key="coding2")

mysql_db = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Vickee@27993",
        database="youtube"
        )
mysqlcursor = mysql_db.cursor()


if load:
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client['youtube']
    mongo_collection = mongo_db['channel_data']
    mongo_data = mongo_collection.find({})

    for data in mongo_data:
        # Assuming you have fields 'field1', 'field2', and 'field3' in your MongoDB collection
        channel_id = selected_channel_id
        channel_name = data["Channel Name"]
        channel_view = data["View Count"]
        channel_description = data["Channel Description"]



        # Assuming you have corresponding columns 'field1', 'field2', and 'field3' in your MySQL table
        sql = "INSERT INTO channel (channel_id, channel_name, channel_view, channel_description) VALUES (%s, %s, %s, %s)"
        values = (channel_id, channel_name, channel_view, channel_description)

        # Execute the SQL statement
        mysqlcursor.execute(sql,values)
        # mysql_cursor.execute(values)

    mongo_video=mongo_db['video_details']
    mongo_video_data=mongo_video.find({})
    for videodata in mongo_video_data:
        video_id = videodata.get("Video_Id")
        playlist_id = data["Playlist ID"]
        video_name = videodata.get("Video_Title")
        video_descrption = videodata.get("Video_Description")
        published_date = videodata.get("Published")
        published_date = datetime.datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ')
        formatted_published_date = published_date.strftime('%Y-%m-%d %H:%M:%S')
        view_count = videodata.get("Views")
        like_count = videodata.get("Likes")
        dislike_count = videodata.get("Dislikes")
        favorite_count = videodata.get("Favorite_Count")
        comment_count = videodata.get("Comment_Count")
        duration = videodata.get("Duration")
        minutes_match = re.search(r'(\d+)M', duration)
        seconds_match = re.search(r'(\d+)S', duration)

        minutes = int(minutes_match.group(1)) if minutes_match else 0
        seconds = int(seconds_match.group(1)) if seconds_match else 0

        duration = minutes * 60 + seconds


        thumbnail = videodata.get("Thumbnail")
        caption_status = videodata.get( "Caption_Status")

        sql = "INSERT INTO video (video_id, playlist_id, video_name, video_descrption, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (video_id , playlist_id , video_name , video_descrption, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status)

        mysqlcursor.execute(sql, values)

    mongo_comment = mongo_db['comment_data']
    mongo_comment_data = mongo_comment.find({})
    for commentdata in mongo_comment_data:
        comment_id = commentdata["comment_id"]
        video_id = commentdata["video_id"]
        comment_text = commentdata["comment_text"]
        comment_author = commentdata["comment_author"]
        comment_published_date = commentdata["comment_publishedAt"]
        comment_published_date = datetime.datetime.strptime(comment_published_date, '%Y-%m-%dT%H:%M:%SZ')
        comment_published_date = comment_published_date.strftime('%Y-%m-%d %H:%M:%S')

        sql = "INSERT INTO comment(comment_id, video_id, comment_text, comment_author, comment_published_date) values ( %s, %s, %s, %s, %s)"
        values = (comment_id, video_id, comment_text, comment_author, comment_published_date)

        mysqlcursor.execute(sql, values)

        playlist_name = data["Playlist NAME"],
        playlist_id = data["Playlist ID"],
        channel_id = selected_channel_id

        sql = "INSERT INTO playlist(playlist_id, channel_id, playlist_name) values (%s, %s, %s)"
        values = (playlist_id[0], channel_id, playlist_name[0])
        st.write(values)
        mysqlcursor.execute(sql, values)




    # Commit the changes to MySQL
mysql_db.commit()


    # Close the connections
# mongo_client.close()
# mysql_cursor.close()
# mysql_db.close()

Q1=st.button('What are the names of all the videos and their corresponding channels?')
Q2=st.button('Which channels have the most number of videos, and how many videos do they have?')
Q3=st.button('What are the top 10 most viewed videos and their respective channels?')
Q4=st.button('How many comments were made on each video, and what are their corresponding video names?')
Q5=st.button('Which videos have the highest number of likes, and what are their corresponding channel names?')
Q6=st.button('What is the total number of likes and dislikes for each video, and what are their corresponding video names?')
Q7=st.button('What is the total number of views for each channel, and what are their corresponding channel names?')
Q8=st.button('What are the names of all the channels that have published videos in the year 2022?')
Q9=st.button('What is the average duration of all videos in each channel, and what are their corresponding channel names?')
Q10=st.button('Which videos have the highest number of comments, and what are their corresponding channel names?')

if Q1:
    load_data = mysql_cursor.execute('SELECT a.video_name, b.channel_name FROM video a JOIN playlist c ON a.playlist_id = c.playlist_id JOIN channel b ON c.channel_id = b.channel_id;')
    result = mysql_cursor.fetchall()
    st.write(result)
    df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
    st.write(df)
if Q2:
    load_data = mysql_cursor.execute('SELECT channel_name, COUNT(*) AS video_count FROM video JOIN channel ON video.playlist_id = channel.channel_id GROUP BY channel_name ORDER BY video_count DESC;')
    result = mysql_cursor.fetchall()
    st.write(result)
    df = pd.DataFrame(result, columns=["Channel Name", "Count"])
    st.write(df)
if Q3:
    load_data = mysql_cursor.execute('SELECT video.video_name, channel.channel_name, video.view_count FROM video JOIN channel ON video.playlist_id = channel.channel_id ORDER BY video.view_count DESC LIMIT 10;')
    result = mysql_cursor.fetchall()
    st.write(result)
    df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
    st.write(df)
if Q4:
    load_data = mysql_cursor.execute('SELECT video.video_name, COUNT(comment.comment_id) AS comment_count FROM video LEFT JOIN comment ON video.video_id = comment.video_id GROUP BY video.video_id, video.video_name;')
    result = mysql_cursor.fetchall()
    st.write(result)
    df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
    st.write(df)
if Q5:
    load_data = mysql_cursor.execute('SELECT video.video_name, channel.channel_name, video.like_count FROM video INNER JOIN channel ON video.playlist_id = channel.channel_id WHERE video.like_count = (SELECT MAX(like_count) FROM video);')
    result = mysql_cursor.fetchall()
    st.write(result)
    df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
    st.write(df)
if Q6:
    load_data = mysql_cursor.execute('SELECT video.video_name, SUM(video.like_count) AS total_likes, SUM(video.dislike_count) AS total_dislikes FROM video GROUP BY video.video_name;')
    result = mysql_cursor.fetchall()
    st.write(result)
    df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
    st.write(df)
if Q7:
    load_data = mysql_cursor.execute('SELECT channel.channel_name, SUM(video.view_count) AS total_views FROM channel JOIN video ON channel.channel_id = video.playlist_id GROUP BY channel.channel_name;')
    result = mysql_cursor.fetchall()
    st.write(result)
    df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
    st.write(df)
if Q8:
    load_data = mysql_cursor.execute('SELECT DISTINCT channel.channel_name FROM channel JOIN video ON channel.channel_id = video.playlist_id WHERE YEAR(video.publish_date) = 2022;')
    result = mysql_cursor.fetchall()
    st.write(result)
    df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
    st.write(df)
if Q9:
    load_data = mysql_cursor.execute('SELECT channel.channel_name, AVG(video.duration) AS average_duration FROM channel JOIN video ON channel.channel_id = video.playlist_id GROUP BY channel.channel_name;')
    result = mysql_cursor.fetchall()
    st.write(result)
    df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
    st.write(df)
if Q10:
    load_data = mysql_cursor.execute('SELECT video.video_name, channel.channel_name, COUNT(comment.comment_id) AS comment_count FROM video JOIN channel ON video.playlist_id = channel.channel_id JOIN comment ON video.video_id = comment.video_id GROUP BY video.video_id ORDER BY comment_count DESC LIMIT 10;')
    result = mysql_cursor.fetchall()
    st.write(result)
    df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
    st.write(df)
