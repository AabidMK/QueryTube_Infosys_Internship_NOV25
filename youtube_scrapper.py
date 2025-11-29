
import os
import time
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound, CouldNotRetrieveTranscript

# load .env
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    raise SystemExit("Missing YOUTUBE_API_KEY in .env")

youtube = build('youtube', 'v3', developerKey=API_KEY)

# change channel handle if you want
channel_handle = '@programmingwithmosh'

# STEP 1 — Channel Details
ch = youtube.channels().list(
    part='snippet,statistics,contentDetails',
    forHandle=channel_handle
).execute()

channel = ch['items'][0]
channel_id = channel['id']
channel_title = channel['snippet']['title']
channel_description = channel['snippet']['description']
channel_country = channel['snippet'].get('country')
channel_thumbnail = channel['snippet']['thumbnails']['default']['url']
channel_subscriberCount = channel['statistics'].get('subscriberCount')
channel_videoCount = channel['statistics'].get('videoCount')
upload_playlist = channel['contentDetails']['relatedPlaylists']['uploads']

# STEP 2 — Fetch 50 Videos
videos = []
next_page_token = None

while len(videos) < 50:
    pl = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=upload_playlist,
        maxResults=50,
        pageToken=next_page_token
    ).execute()

    videos.extend(pl.get('items', []))
    next_page_token = pl.get('nextPageToken')

    if not next_page_token:
        break

videos = videos[:50]

# STEP 3 — Get Metadata of Each Video
finaldata = []

for v in videos:
    vid = v['contentDetails']['videoId']

    details = youtube.videos().list(
        part='snippet,statistics,contentDetails,status',
        id=vid
    ).execute()

    if not details['items']:
        continue

    item = details['items'][0]
    snippet = item.get("snippet", {})
    stats = item.get("statistics", {})
    info = item.get("contentDetails", {})
    status = item.get("status", {})

    row = {
        'id': vid,
        'title': snippet.get("title"),
        'description': snippet.get("description"),
        'publishedAt': snippet.get("publishedAt"),
        'tags': "|".join(snippet.get("tags", [])) if snippet.get("tags") else None,
        'categoryId': snippet.get("categoryId"),
        'defaultLanguage': snippet.get("defaultLanguage"),
        'defaultAudioLanguage': snippet.get("defaultAudioLanguage"),
        'thumbnail_default': snippet.get("thumbnails", {}).get("default", {}).get("url"),
        'thumbnail_high': snippet.get("thumbnails", {}).get("high", {}).get("url"),
        'duration': info.get("duration"),
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

# STEP 4 — Save CSV
df = pd.DataFrame(finaldata)
df.to_csv("videos_metadata.csv", index=False)

print("✔ Successfully saved 50 video records to videos_metadata.csv")