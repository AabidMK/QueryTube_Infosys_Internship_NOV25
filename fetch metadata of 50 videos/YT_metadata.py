import os
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

api_Key = os.environ.get('YT_API')
youtube = build('youtube', 'v3', developerKey=api_Key)

channel_handle = '@AutoFocus'

ch = youtube.channels().list(
    part='snippet,statistics,contentDetails',
    forHandle=channel_handle
).execute()

channel_data = ch['items'][0]

channel_id = channel_data['id']
channel_title = channel_data['snippet']['title']
channel_description = channel_data['snippet']['description']
channel_country = channel_data['snippet'].get('country')
channel_thumbnail = channel_data['snippet']['thumbnails']['default']['url']
channel_subscriberCount = channel_data['statistics'].get('subscriberCount')
channel_videoCount = channel_data['statistics'].get('videoCount')
upload_playlist = channel_data['contentDetails']['relatedPlaylists']['uploads']

videos = youtube.playlistItems().list(
    part="contentDetails",
    playlistId=upload_playlist,
    maxResults=50
).execute()

finaldata = []

for v in videos['items']:
    vid = v['contentDetails']['videoId']

    
    content = youtube.videos().list(
        part='snippet,statistics,contentDetails,status',
        id=vid
    ).execute()

    item = content["items"][0]
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
        'channel_videoCount': channel_videoCount
    }

    finaldata.append(row)

df = pd.DataFrame(finaldata)
df.to_csv('final_output.csv', index=False)

print("CSV created with selected fields!")
