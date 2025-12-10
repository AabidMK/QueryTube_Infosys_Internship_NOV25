# fetch_videos.py

import os
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build

# Load API key from .env file
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")

# Replace with your channel ID
# bbc
# CHANNEL_ID = "UCHaHD477h-FeBbVh9Sh7syA" 
# easy eng
# CHANNEL_ID = "UCTRHegh7UqWuKRymXoqzbzA" 
# min physics
# CHANNEL_ID = "UCeiYXex_fwgYDonaTcSIk6w"
# bright side
CHANNEL_ID = "UC4rlAVgAK0SGk-yTfe48Qpw"

# Build YouTube API client
youtube = build("youtube", "v3", developerKey=API_KEY)

# Step 1a: Get the channel's Uploads playlist ID
channel_response = youtube.channels().list(
    id=CHANNEL_ID,
    part="contentDetails"
).execute()

uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


# Step 1b: Get up to 50 video IDs from the Uploads playlist
video_ids = []
next_page_token = None

while len(video_ids) < 50:
    playlist_response = youtube.playlistItems().list(
        playlistId=uploads_playlist_id,
        part="contentDetails",
        maxResults=50,
        pageToken=next_page_token
    ).execute()

    for item in playlist_response["items"]:
        video_ids.append(item["contentDetails"]["videoId"])
        if len(video_ids) >= 50:
            break

    next_page_token = playlist_response.get("nextPageToken")
    if not next_page_token:
        break

print(f"Fetched {len(video_ids)} video IDs")



# Step 2: Get video details
video_response = youtube.videos().list(
    id=",".join(video_ids),
    part="snippet,contentDetails,statistics,status"
).execute()

video_data = []
for item in video_response["items"]:
    snippet = item["snippet"]
    content = item["contentDetails"]
    stats = item.get("statistics", {})
    status = item["status"]

    video_data.append({
        "id": item["id"],
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "publishedAt": snippet.get("publishedAt"),
        "tags": snippet.get("tags"),
        "categoryId": snippet.get("categoryId"),
        "defaultLanguage": snippet.get("defaultLanguage"),
        "defaultAudioLanguage": snippet.get("defaultAudioLanguage"),
        "thumbnail_default": snippet["thumbnails"]["default"]["url"],
        "thumbnail_high": snippet["thumbnails"]["high"]["url"],
        "duration": content.get("duration"),
        "viewCount": stats.get("viewCount"),
        "likeCount": stats.get("likeCount"),
        "commentCount": stats.get("commentCount"),
        "privacyStatus": status.get("privacyStatus")
    })

# Step 3: Get channel details
channel_response = youtube.channels().list(
    id=CHANNEL_ID,
    part="snippet,statistics"
).execute()

channel_info = channel_response["items"][0]
channel_snippet = channel_info["snippet"]
channel_stats = channel_info["statistics"]

for v in video_data:
    v.update({
        "channel_id": CHANNEL_ID,
        "channel_title": channel_snippet.get("title"),
        "channel_description": channel_snippet.get("description"),
        "channel_country": channel_snippet.get("country"),
        "channel_thumbnail": channel_snippet["thumbnails"]["default"]["url"],
        "channel_subscriberCount": channel_stats.get("subscriberCount"),
        "channel_videoCount": channel_stats.get("videoCount")
    })

# Step 4: Save to DataFrame
df = pd.DataFrame(video_data)
print(df.head())

# Save to CSV
df.to_csv("utube_vids.csv", index=False)
print("Saved 50 videos to youtube_videos.csv")
