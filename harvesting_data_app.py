from datetime import datetime
import mysql.connector
import pandas as pd
import pymongo
import streamlit as st
from PIL import Image
from googleapiclient.discovery import build

# setting page configurations
icon = Image.open('y.png')
st.set_page_config(page_title='Youtube Data Harvesting & Warehousing',
                   page_icon=icon,
                   layout="wide",
                   initial_sidebar_state="expanded")
st.header(':blue[Youtube Data Harvesting & Warehousing]',divider = 'rainbow')
st.write("### :blue[Enter YouTube Channel ID :-]")
ch_id = st.text_input('')

# Constructing the conection with Youtube API
api_key = 'ENTER YOUR API KEY'
youtube = build('youtube', 'v3', developerKey=api_key)

# creating mongoD conection
mongo_connection = 'mongodb://localhost:27017/'
mongodb_name = 'youtube_ds'
mongo_channel_collection_name = 'channel_data'
mongo_playlist_collection_name = 'playlist_data'
mongo_video_collection_name = 'video_data'
mongo_comment_collection_name = 'comment_data'

client = pymongo.MongoClient(mongo_connection)
db = client[mongodb_name]

# creating SQL connection
mysql_host = 'localhost'
mysql_port = 3306
mysql_db = 'data_pool'
mysql_user = 'root'
mysql_password = 'Test@1234'

connection = mysql.connector.Connect(
    host = mysql_host,
    port = mysql_port,
    database = mysql_db,
    user = mysql_user,
    password = mysql_password
)
cursor = connection.cursor()

# function to get Channel details
def get_channel_stats(youtube, channel_id):
    request = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id)
    response = request.execute()
    data = dict(channel_id=channel_id,
                channel_name=response['items'][0]['snippet']['title'],
                Subscribers=response['items'][0]['statistics']['subscriberCount'],
                Total_videos=response['items'][0]['statistics']['videoCount'],
                channel_description=response['items'][0]['snippet']['description'],
                channel_view_count = response['items'][0]['statistics']['viewCount']
                )
    return data


# function to get playlist details
def get_playlist_stats(youtube, channel_id):
    request = youtube.playlists().list(part='snippet', channelId=channel_id)
    response = request.execute()
    all_data = []
    for i in range(len(response['items'])):
        data = dict(playlist_id=response['items'][i]['id'],
                    playlist_name=response['items'][i]['snippet']['title'],
                    channel_id=response['items'][i]['snippet']['channelId'])
        all_data.append(data)
    return all_data

# function to get playlist id's
def get_playlist_ids(youtube, channel_id):
    request = youtube.playlists().list(part='snippet', channelId=channel_id)
    response = request.execute()
    play_list_ids = []
    for i in range(len(response['items'])):
        play_list_ids.append(response['items'][i]['id'])
    return play_list_ids


# function to get video id's
def get_video_ids(play_list_id):
    video_ids = []
    for j in range(len(play_list_id)):
        request = youtube.playlistItems().list(part='snippet', playlistId=play_list_id[j])
        response = request.execute()
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
    return video_ids


# function to get video response from API
def get_video_details_res(v_ids):
    dt = ','.join(v_ids)
    for i in range(0, len(v_ids)):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=dt).execute()
    return response


# function to get video details
def get_req_video_details(res):
    req_video_details = []
    for i in range(len(res['items'])):
        data = dict(Channel_id=res['items'][i]['snippet']['channelId'],
                    Video_id=res['items'][i]['id'],
                    Video_name=res['items'][i]['snippet']['title'],
                    Video_des=res['items'][i]['snippet']['description'],
                    Publish_date=res['items'][i]['snippet']['publishedAt'],
                    View_count=res['items'][i]['statistics']['viewCount'],
                    Like_count=res['items'][i]['statistics']['likeCount'],
                    Fav_count=res['items'][i]['statistics']['favoriteCount'],
                    Comment_count=res['items'][i]['statistics']['commentCount'],
                    Duration=res['items'][i]['contentDetails']['duration'],
                    Thumb_nail=res['items'][i]['snippet']['thumbnails']['default']['url'],
                    Caption_status=res['items'][i]['contentDetails']['caption'])
        req_video_details.append(data)
    return req_video_details


# function to get comments details (comment id,comment text, comment author and published date)
def get_comment_details_res(v_ids):
    cmt_res = []
    for i in range(0, len(v_ids)):
        # print(i)
        request = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=v_ids[i])
        response = request.execute()
        cmt_res.append(response)
    cmt_req_details = []
    for j in range(0, len(cmt_res)):
        for k in range(0, len(cmt_res[j]['items'])):
            data = dict(Comment_id=cmt_res[j]['items'][k]['id'],
                        Video_id=cmt_res[j]['items'][k]['snippet']['videoId'],
                        Comment_text=cmt_res[j]['items'][k]['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author=cmt_res[j]['items'][k]['snippet']['topLevelComment']['snippet'][
                            'authorDisplayName'],
                        Comment_published_date=cmt_res[j]['items'][k]['snippet']['topLevelComment']['snippet'][
                            'publishedAt']
                        )
            cmt_req_details.append(data)
    return cmt_req_details

# function to fetch channel names from mongoDB
def channel_names():
    names = []
    for i in db[mongo_channel_collection_name].find():
        names.append(i['channel_name'])
    return names

# constructing streamlit UI
tab1, tab2, tab3 = st.tabs(['$\huge Extract$', "$\huge Migrate$","$$\huge Visualization $$"])

# setting up extract tab
with tab1:
    if st.button('Extract Channel Data'):
        if ch_id:
            ch_details = get_channel_stats(youtube,ch_id)
            st.write(f'### Extracted data from :violet["{ch_details["channel_name"]}"] Channel')
            st.table(ch_details)

    if st.button("Upload to MongoDb"):
        with st.spinner('Uploading...'):
            ch_details = get_channel_stats(youtube,ch_id)
            pl_details = get_playlist_stats(youtube,ch_id)
            all_playlist_ids = get_playlist_ids(youtube, ch_id)
            all_video_ids = get_video_ids(all_playlist_ids)
            videoDetailResponse = get_video_details_res(all_video_ids)
            video_stats_data = get_req_video_details(videoDetailResponse)
            vd_details = video_stats_data
            cmt_details = get_comment_details_res(all_video_ids)

            collection = db[mongo_channel_collection_name]
            collection1 = db[mongo_playlist_collection_name]
            collection2 = db[mongo_video_collection_name]
            collection3 = db[mongo_comment_collection_name]
            data_exist = collection.find_one(ch_details)
            if data_exist is None:
                collection.insert_one(ch_details)
                collection1.insert_many(pl_details)
                collection2.insert_many(vd_details)
                collection3.insert_many(cmt_details)
                st.success("## Upload successful to mongoDb!!!")

            else:
                st.error('## Channel details already exist in the mongoDB!!!')

# setting up Migrate tab
with tab2:
    st.markdown('#    ')
    st.markdown("## Select a Channel to migrate to SQL")

    ch_names = channel_names()
    user_input = st.selectbox('Select the Channel',options=ch_names)


    # function to get channel Id
    def get_user_input_channel_Id(channelName):
        collection = db[mongo_channel_collection_name]
        documents = collection.find()
        channelId = ''
        for i in documents:
            if i.get('channel_name') == channelName:
                channelId = i.get('channel_id')
        return channelId

    user_selected_channelId = get_user_input_channel_Id(user_input)

    # function to insert Channel data from mongoDB to MySQL
    def insert_channel_data():
        collection = db[mongo_channel_collection_name]
        insert_channel_query = "INSERT INTO channels (channel_id, channel_name,Subscribers,Total_videos, channel_description,channel_view_count) VALUES (%s, %s, %s, %s, %s, %s)"
        for i in collection.find({'channel_name':user_input},{'_id':0}):
            cursor.execute(insert_channel_query,tuple(i.values()))
            connection.commit()

    # function to insert Playlist data from mongoDB to MySQL
    def insert_playlist_data():
        collection1 = db[mongo_playlist_collection_name]
        insert_playlist_query = "INSERT INTO playlist (playlist_id, playlist_name, channel_id) VALUES (%s, %s, %s)"
        for i in collection1.find({'channel_id':user_selected_channelId},{'_id':0}):
            cursor.execute(insert_playlist_query,tuple(i.values()))
            connection.commit()

    # function to insert Video data from mongoDB to MySQL
    def insert_video_data():
        collection2 = db[mongo_video_collection_name]
        insert_video_query = "INSERT INTO video(Channel_id, Video_id, Video_name, Video_des, Publish_date, View_count, Like_count, Fav_count, Comment_count, Duration, Thumb_nail, Caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        for i in collection2.find({'Channel_id': user_selected_channelId}, {'_id': 0}):
            # Convert ISO 8601 datetime to MySQL datetime format
            publish_date_iso = i['Publish_date']
            publish_date_mysql = datetime.strptime(publish_date_iso, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

            # Create a tuple for insertion, including the converted datetime
            insert_data = (
                i['Channel_id'], i['Video_id'], i['Video_name'], i['Video_des'], publish_date_mysql, i['View_count'],
                i['Like_count'], i['Fav_count'], i['Comment_count'], i['Duration'], i['Thumb_nail'],
                i['Caption_status'])

            cursor.execute(insert_video_query, insert_data)
            connection.commit()

    # function to insert Comment data from mongoDB to MySQL
    def insert_comment_data():
        collection3 = db[mongo_comment_collection_name]
        collection4 = db[mongo_video_collection_name]
        insert_comment_query = "INSERT INTO comments (Comment_id, Video_id, Comment_text, Comment_author, Comment_published_date) VALUES (%s, %s, %s, %s, %s)"

        for i in collection4.find({'Channel_id': user_selected_channelId}, {'_id': 0}):
            for j in collection3.find({'Video_id': i['Video_id']}, {'_id': 0}):
                # Convert ISO 8601 datetime to MySQL datetime format
                comment_published_date_iso = j['Comment_published_date']
                comment_published_date_mysql = datetime.strptime(comment_published_date_iso,
                                                                 '%Y-%m-%dT%H:%M:%SZ').strftime(
                    '%Y-%m-%d %H:%M:%S')

                # Create a tuple for insertion, including the converted datetime
                insert_data = (
                    j['Comment_id'], j['Video_id'], j['Comment_text'], j['Comment_author'],
                    comment_published_date_mysql)

                cursor.execute(insert_comment_query, insert_data)
                connection.commit()


    if st.button('Submit'):
        cursor.execute(f"SELECT * FROM channels WHERE channel_name = %s", (user_input,))
        existing_data = cursor.fetchone()
        if existing_data:
            st.error('# :red["Channel Data already migrated!!!"]')
        else:
            insert_channel_data()
            insert_playlist_data()
            insert_video_data()
            insert_comment_data()
            st.success('## :green["Data Migration to MySQL is Successful!!!"]')

# Setting up visualization tab
with tab3:
    st.write('## :blue[Select any question to get Insights]')
    questions = st.selectbox('$\huge Questions $',
                             ['Click the question that you would like to query',
                              "1.What are the names of all the videos and their corresponding channels?",
                              "2.Which channels have the most number of videos, and how many videos do they have?",
                              "3. What are the top 10 most viewed videos and their respective channels?",
                              "4.How many comments were made on each video, and what are their corresponding video names?",
                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                              "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                              "8.What are the names of all the channels that have published videos in the year2022?",
                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"]
                             )
    if questions == "1.What are the names of all the videos and their corresponding channels?":
        query1 = "Select Video_name as Video_Name, channel_name as Channel_Name from video join channels on channels.channel_id = video.Channel_id "
        cursor.execute(query1)
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)

    elif questions == "2.Which channels have the most number of videos, and how many videos do they have?":
        query2 = "Select channel_name as Channel_Name , Total_videos as Total_Videos from channels order by Total_videos desc LIMIT 1"
        cursor.execute(query2)
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)

    elif questions == "3. What are the top 10 most viewed videos and their respective channels?":
        query3 = "select Video_name as Video_Name,View_count as View_Count,channel_name as Channel_Name from channels join video on channels.channel_id = video.Channel_id order by View_count desc limit 10"
        cursor.execute(query3)
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions == "4.How many comments were made on each video, and what are their corresponding video names?":
        query4 = "Select Video_name as Video_Name, Comment_count as Comment_Count from video "
        cursor.execute(query4)
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5 = "Select Video_name as Video_Name, Like_count as Like_Count, channel_name as Channel_Name from channels join video on channels.channel_id = video.Channel_id order by Like_count desc limit 5"
        cursor.execute(query5)
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query6 = "Select Video_name as Video_Name, Like_count as Like_count from video"
        cursor.execute(query6)
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
        query7 = "Select channel_name as Channel_Name , channel_view_count as Channelviewcount from channels"
        cursor.execute(query7)
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions == "8.What are the names of all the channels that have published videos in the year2022?":
        query8 = "Select distinct channel_name as Channel_Name from channels join video on channels.channel_id = video.Channel_id where Publish_date like '2022%'"
        cursor.execute(query8)
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9 = " SELECT channel_name, AVG((CONVERT(SUBSTRING(Duration, LOCATE('T', Duration) + 1, LOCATE('M', Duration) - LOCATE('T', Duration) - 1), UNSIGNED) * 60) + CONVERT(SUBSTRING(Duration, LOCATE('M', Duration) + 1, LOCATE('S', Duration) - LOCATE('M', Duration) - 1), UNSIGNED) ) / 60 AS average_duration_minutes FROM video join channels on channels.channel_id = video.Channel_id group by channel_name"
        cursor.execute(query9)
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

    elif questions =="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10 = "Select channel_name as Channel_Name , Video_name as Video_Name, Comment_count as Comment_Count from channels join video on channels.channel_id = video.Channel_id order by Comment_count desc limit 10"
        cursor.execute(query10)
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
        st.write(df)

cursor.close()
connection.close()
client.close()