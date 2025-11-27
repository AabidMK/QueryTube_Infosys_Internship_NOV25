import os
import time
import pandas as pd
from googleapiclient.discovery import build
# from youtube_transcript_api import YouTubeTranscriptApi
# from youtube_transcript_api.proxies import GenericProxyConfig
#t_api = YouTubeTranscriptApi()
# proxies=[
#     {"http":'http://IP1:PORT','https':'https://IP1:PORT'},
#     {"http":'http://IP2:PORT','https':'https://IP2:PORT'},
#     {"http":'http://IP3:PORT','https':'https://IP3:PORT'}
# ]
from dotenv import load_dotenv
load_dotenv()
api_Key = os.getenv('api_key')
youtube = build('youtube', 'v3', developerKey=api_Key)

channel_handle = '@freecodecamp'

# get channel id
ch = youtube.channels().list(part='snippet,statistics,contentDetails', forHandle=channel_handle).execute()
channel_id = ch['items'][0]['id']
channel_title = ch['items'][0]['snippet']['title']
channel_description = ch['items'][0]['snippet']['description']
channel_country = ch['items'][0]['snippet'].get('country')
channel_thumbnail = ch['items'][0]['snippet']['thumbnails']['default']['url']
channel_subscriberCount = ch['items'][0]['statistics'].get('subscriberCount')
channel_videoCount = ch['items'][0]['statistics'].get('videoCount')
upload_playlist = ch['items'][0]['contentDetails']['relatedPlaylists']['uploads']

# get videos
vidreq = youtube.playlistItems().list(
    part='contentDetails',
    playlistId=upload_playlist,
    maxResults=50
)
videos = vidreq.execute()

finaldata = []

for v in videos['items']:
    vid = v['contentDetails']['videoId']

    content = youtube.videos().list(
        part='snippet,statistics,contentDetails,status',
        id=vid
    ).execute()
    
    for item in content['items']:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        details = item.get("contentDetails", {})
        status = item.get("status", {})

        row = {
            'id': vid,
            'title': snippet.get("title"),
            'description': snippet.get("description"),
            'publishedAt': snippet.get("publishedAt"),
            'tags': ",".join(snippet.get("tags", [])) if snippet.get("tags") else None,
            'categoryId': snippet.get("categoryId"),
            'defaultLanguage': snippet.get("defaultLanguage"),
            'defaultAudioLanguage': snippet.get("defaultAudioLanguage"),
            'thumbnail_default': snippet.get("thumbnails", {}).get("default", {}).get("url"),
            'thumbnail_high': snippet.get("thumbnails", {}).get("high", {}).get("url"),
            'duration': details.get("duration"),
            'viewCount': stats.get("viewCount"),
            'likeCount': stats.get("likeCount"),
            'commentCount': stats.get("commentCount"),
            'privacyStatus': status.get("privacyStatus"),

            'channel_id': channel_id,
            'channel_title': channel_title,
            'channel_description': channel_description,
            'channel_country': channel_country,
            'channel_thumbnail': channel_thumbnail,
            'channel_subscriberCount': channel_subscriberCount,
            'channel_videoCount': channel_videoCount,
            
        }
      #yeah we
        finaldata.append(row)
        
df = pd.DataFrame(finaldata)
df.to_csv('freecodecamp.csv', index=False)
