import requests
import streamlit as st
from streamlit_lottie import st_lottie
from streamlit_option_menu import option_menu
from PIL import Image
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pymongo
import mysql.connector
import datetime
import re
import pandas as pd

# Function to load Lottie animation from a URL
def load_lottieurl(url):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()

# Load Lottie animations
lottie_coding = load_lottieurl("https://lottie.host/8ea4186a-eb65-4c3c-b0e5-cd01ab315d33/jTyCSPOFYz.json")
lottie_coding1 = load_lottieurl("https://lottie.host/ae8a527f-44bb-4f37-ad21-9a72af6ad798/pyiIYEHL0E.json")
lottie_coding2 = load_lottieurl("https://lottie.host/70633239-d7e1-4966-a24b-9833f933a9fb/T8htUrHlii.json")
data_migration = Image.open(r"C:\Users\admin\PycharmProjects\pythonProject\images\sql.png")

# Set up Streamlit page configuration
st.set_page_config(page_title='YouTUBE', page_icon=":movie_camera:", layout="wide")

# Sidebar menu options
with st.sidebar:
    selected = option_menu(
        menu_title="GUVI",
        options=["Project Overview", "MongoDB", "MySQL"],
        icons=["house", "database", "filetype-sql"],
        menu_icon="cast",
        default_index=0
    )

# YouTube API key
API_KEY = 'AIzaSyBLTidzTSC69Y-CDVbKoTV2iRCRQ6HU_Kc'
Youtube = build('youtube', 'v3', developerKey=API_KEY)

# Project Overview section
if selected == "Project Overview":
    with st.container():
        st.write("")
        left_column, right_column = st.columns(2)
        with left_column:
            st.header("CAPSTONE PROJECT")
            st.title('Youtube Data Harvesting Project')
            st.write(" Data Harvesting and Warehousing using SQL, MongoDB, and Streamlit ")
            st.write("[learn more>]('https://docs.google.com/document/d/1WrMDf4KnzprK37EJLr3QW0wRUB3few-1Yujv6wnYhZw/edit?pli=1/')")
        with right_column:
            st.lottie(lottie_coding1, height=300, key="coding1")

# MongoDB section
if selected == "MongoDB":
    Channel_ids = ['UCduIoIMfD8tT3KoU0-zBRgQ', 'UCwr-evhuzGZgDFrq_1pLt_A', 'UCnz-ZXXER4jOvuED5trXfEA',
                   'UCMSI1Ck1mJOaxxwJ0bzrYhQ', 'UCZjRcM1ukeciMZ7_fvzsezQ', 'UCCktnahuRFYIBtNnKT5IYyg',
                   'UCJQJAI7IjbLcpsjWdSzYz0Q', 'UCJtZrWo0j49s3IyxgZWTJDw', 'UCQ3ZwLf92CtRYqJw0QcMa4Q',
                   'UCuI5XcJYynHa5k_lqDzAgwQ']
    selected_channel_id = st.selectbox('select channel id:', Channel_ids)

    with st.container():
        st.write("")
        left_column, right_column = st.columns(2)
        with left_column:
            st.header("Mongo DB")
            st.write("Data inserting from YoutubeAPI to MongoDB")
            transfer = st.button('Insert into MongoDB')
        with right_column:
            st_lottie(lottie_coding, height=300, key="coding")

        # Function to get channel data from YouTube API
        def get_channel_data(Youtube, Channel_ids):
            all_data = []
            request = Youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=Channel_ids
            )
            response = request.execute()

            for item in response.get('items', []):
                data = dict(
                    channel_name=item['snippet']['title'],
                    channel_description=item['snippet']['description'],
                    channel_views=item['statistics']['viewCount'],
                    subscription_count=item['statistics']['subscriberCount'],
                    Video_count=item['statistics']['videoCount'],
                    playlist_id=item['contentDetails']['relatedPlaylists']['uploads'],
                    playlist_name=item['contentDetails']['relatedPlaylists']['uploads']
                )
                all_data.append(data)
            return all_data

        # Function to get video IDs from a playlist
        def get_video_ids(Youtube, playlist_id):
            video_ids = []
            request = Youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50
            )
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            nextpagetoken = response.get('nextPageToken')
            more_pages = True

            while more_pages:
                if nextpagetoken is None:
                    more_pages = False
                else:
                    request = Youtube.playlistItems().list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken=nextpagetoken)

                    response = request.execute()

                    for i in range(len(response['items'])):
                        video_ids.append(response['items'][i]['contentDetails']['videoId'])

                    nextpagetoken = response.get('nextPageToken')

            return video_ids


        # Function to get video details from YouTube API
        def get_video_details(Youtube, video_ids):
            all_video_stats = []

            for i in range(0, len(video_ids), 50):
                request = Youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=','.join(video_ids[i:i + 50])
                )
                response = request.execute()

                for video in response.get('items', []):
                    video_stats = dict(
                        Title=video['snippet']['title'],
                        PublishedDate=video['snippet']['publishedAt'],
                        viewcount=video['statistics']['viewCount'],
                        likeCount=video['statistics']['likeCount'],
                        favoriteCount=video['statistics']['favoriteCount'],
                        commentCount=video['statistics'].get('commentCount', 0),
                        Duration=video['contentDetails']['duration'],
                        Thumbnail=video['snippet']['thumbnails']['default']['url'],
                        Caption_Status=video['contentDetails']['caption']
                    )
                    all_video_stats.append(video_stats)

            return all_video_stats

        # Function to get comments from YouTube API
        def get_comment_in_video(Youtube, video_ids):
            all_comments = []

            for video_id in video_ids:
                try:
                    request = Youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=video_id,
                        maxResults=20
                    )
                    response = request.execute()

                    for comment in response.get('items', []):
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

        # Insert data into MongoDB if the "Insert into MongoDB" button is clicked
        if transfer:
            channel_data = get_channel_data(Youtube, selected_channel_id)
            st.write(channel_data)

            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client['youtube']
            collection = db['channel_data']
            collection.insert_many(channel_data)

            if channel_data and len(channel_data) > 0:
                playlist_id = channel_data[0].get('playlist_id')
                if playlist_id:
                    video_ids = get_video_ids(Youtube, playlist_id)
                    vid_details = get_video_details(Youtube, video_ids)
                    collection = db['video_details']
                    collection.insert_many(vid_details)
                    st.write(vid_details)

                    comment_data = get_comment_in_video(Youtube, video_ids)

                    if comment_data:
                        collection = db['comment_data']
                        collection.insert_many(comment_data)
                        st.write(comment_data)
                    else:
                        st.write("No comments found for video:", video_ids)

# MySQL section
if selected == "MySQL":
    with st.container():
        st.write("")
        left_column, right_column = st.columns(2)
        with right_column:
            st.header("MySQL")
            st.write("Data Migrating from MongoDB to MySQL")
            load = st.button('Load Data')
        with left_column:
            st_lottie(lottie_coding2, height=300, key="coding2")

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
            channel_id = data["playlist_id"]
            channel_name = data["channel_name"]
            channel_view = data["channel_views"]
            channel_description = data["channel_description"]

            # Assuming you have corresponding columns 'field1', 'field2', and 'field3' in your MySQL table
            sql = "INSERT INTO channel (channel_id, channel_name, channel_view, channel_description) VALUES (%s, %s, %s, %s)"
            values = (channel_id, channel_name, channel_view, channel_description)

            # Execute the SQL statement
            mysqlcursor.execute(sql, values)
            # mysql_cursor.execute(values)

        mongo_video = mongo_db['video_details']
        mongo_video_data = mongo_video.find({})
        for videodata in mongo_video_data:
            video_id = videodata.get("Video_Id")
            playlist_id = data["playlist_id"]
            video_name = videodata.get("Title")
            video_descrption = videodata.get("Video_Description")
            published_date = videodata.get("PublishedDate")
            published_date = datetime.datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ')
            formatted_published_date = published_date.strftime('%Y-%m-%d %H:%M:%S')
            view_count = videodata.get("viewcount")
            like_count = videodata.get("likeCount")
            dislike_count = videodata.get("Dislikes")
            favorite_count = videodata.get("favoriteCount")
            comment_count = videodata.get("commentCount")
            duration = videodata.get("Duration")
            minutes_match = re.search(r'(\d+)M', duration)
            seconds_match = re.search(r'(\d+)S', duration)

            minutes = int(minutes_match.group(1)) if minutes_match else 0
            seconds = int(seconds_match.group(1)) if seconds_match else 0

            duration = minutes * 60 + seconds

            thumbnail = videodata.get("Thumbnail")
            caption_status = videodata.get("Caption_Status")

            sql = "INSERT INTO video (video_id, playlist_id, video_name, video_descrption, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (
            video_id, playlist_id, video_name, video_descrption, published_date, view_count, like_count, dislike_count,
            favorite_count, comment_count, duration, thumbnail, caption_status)

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

            playlist_name = data["playlist_name"],
            playlist_id = data["playlist_id"],
            channel_id = data["playlist_id"]

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

    with st.container():
        st.write("---SQL Query Output---")
        left_column, right_column = st.columns(2)
        with left_column:
            Q1 = st.button('Q1.What are the names of all the videos and their corresponding channels?')
            Q2 = st.button('Q2.Which channels have the most number of videos, and how many videos do they have?')
            Q3 = st.button('Q3.What are the top 10 most viewed videos and their respective channels?')
            Q4 = st.button('Q4.How many comments were made on each video, and what are their corresponding video names?')
            Q5 = st.button('Q5.What is the total number of views for each channel, and what are their corresponding channel names?')
        with right_column:
            Q6 = st.button('Q6.What is the total number of likes and dislikes for each video & their corresponding video names?')
            Q7 = st.button('Q7.Which videos have the highest number of likes, and what are their corresponding channel names?')
            Q8 = st.button('Q8.What are the names of all the channels that have published videos in the year 2022?')
            Q9 = st.button('Q9.What is the average duration of all videos in each channel & their corresponding channel names?')
            Q10 = st.button('Q10.Which videos have the highest number of comments, and what are their corresponding channel names?')

    if Q1:
        load_data = mysqlcursor.execute('SELECT video.video_name, channel.channel_name FROM video JOIN channel ON video.channel_id = channel.id;')
        result = mysqlcursor.fetchall()
        st.write(result)
        df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
        st.write(df)
    if Q2:
        load_data = mysqlcursor.execute('SELECT channel.channel_name, COUNT(video.id) AS video_count FROM channel JOIN video ON channel.id = video.channel_id GROUP BY channel.channel_name ORDER BY video_count DESC;')
        result = mysqlcursor.fetchall()
        st.write(result)
        df = pd.DataFrame(result, columns=["Channel Name", "Count"])
        st.write(df)
    if Q3:
        load_data = mysqlcursor.execute('SELECT video.video_name, channel.channel_name, video.view_count FROM video JOIN channel ON video.channel_id = channel.id ORDER BY video.view_count DESC LIMIT 10;')
        result = mysqlcursor.fetchall()
        st.write(result)
        df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
        st.write(df)
    if Q4:
        load_data = mysqlcursor.execute('SELECT video.video_name, COUNT(comment.id) AS comment_count FROM video JOIN comment ON video.id = comment.video_id GROUP BY video.video_name;')
        result = mysqlcursor.fetchall()
        st.write(result)
        df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
        st.write(df)
    if Q5:
        load_data = mysqlcursor.execute('SELECT channel.channel_name, SUM(video.view_count) AS total_views FROM channel JOIN video ON channel.id = video.channel_id GROUP BY channel.channel_name;')
        result = mysqlcursor.fetchall()
        st.write(result)
        df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
        st.write(df)
    if Q6:
        load_data = mysqlcursor.execute('SELECT video.video_name, SUM(video.like_count) AS total_likes, SUM(video.dislike_count) AS total_dislikes FROM video GROUP BY video.video_name;')
        result = mysqlcursor.fetchall()
        st.write(result)
        df = pd.DataFrame(result, columns=["Video Name", "Channel Name","like_count"])
        st.write(df)
    if Q7:
        load_data = mysqlcursor.execute('SELECT video.video_name, channel.channel_name, video.like_count FROM video JOIN channel ON video.channel_id = channel.id ORDER BY video.like_count DESC LIMIT 10;')
        result = mysqlcursor.fetchall()
        st.write(result)
        df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
        st.write(df)
    if Q8:
        load_data = mysqlcursor.execute('SELECT DISTINCT channel.channel_name FROM channel JOIN video ON channel.id = video.channel_id WHERE YEAR(video.published_date) = 2022;')
        result = mysqlcursor.fetchall()
        st.write(result)
        df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
        st.write(df)
    if Q9:
        load_data = mysqlcursor.execute('SELECT channel.channel_name, AVG(video.duration) AS average_duration FROM channel JOIN video ON channel.id = video.channel_id GROUP BY channel.channel_name;')
        result = mysqlcursor.fetchall()
        st.write(result)
        df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
        st.write(df)
    if Q10:
        load_data = mysqlcursor.execute('SELECT video.video_name, channel.channel_name, COUNT(comment.id) AS comment_count FROM video JOIN channel ON video.channel_id = channel.id JOIN comment ON video.id = comment.video_id GROUP BY video.video_name ORDER BY comment_count DESC LIMIT 10;')
        result = mysqlcursor.fetchall()
        st.write(result)
        df = pd.DataFrame(result, columns=["Video Name", "Channel Name"])
        st.write(df)
