from googleapiclient.discovery import build
import os
import pandas as pd
import isodate
from dotenv import load_dotenv

API_KEY = os.getenv(API_KEY)
CHANNEL_ID = os.getenv(CHANNEL_ID)

youtube = build("youtube", "v3", developerKey=API_KEY)

# -----------------------------
# STEP 1: Fetch CHANNEL Details
# -----------------------------
channel_res = youtube.channels().list(
    part="snippet,statistics,contentDetails",
    id=CHANNEL_ID
).execute()

channel_data = channel_res["items"][0]

channel_info = {
    "channel_id": CHANNEL_ID,
    "channel_title": channel_data["snippet"]["title"],
    "channel_description": channel_data["snippet"].get("description"),
    "channel_country": channel_data["snippet"].get("country"),
    "channel_thumbnail": channel_data["snippet"]["thumbnails"]["default"]["url"],
    "channel_subscriber": channel_data["statistics"].get("subscriberCount"),
    "channel_videoCount": channel_data["statistics"].get("videoCount")
}

uploads_playlist_id = channel_data["contentDetails"]["relatedPlaylists"]["uploads"]

# ------------------------------------
# STEP 2: Fetch 50 VIDEOS from PLAYLIST
# ------------------------------------
playlist_res = youtube.playlistItems().list(
    part="snippet",
    playlistId=uploads_playlist_id,
    maxResults=50
).execute()

video_ids = []
for item in playlist_res["items"]:
    video_ids.append(item["snippet"]["resourceId"]["videoId"])

# ------------------------------------
# STEP 3: Fetch VIDEO DETAILS + STATS
# ------------------------------------
videos_res = youtube.videos().list(
    part="snippet,contentDetails,statistics,status",
    id=",".join(video_ids)
).execute()

final_data = []

for v in videos_res["items"]:
    snippet = v["snippet"]
    stats = v.get("statistics", {})
    content = v["contentDetails"]
    status = v.get("status", {})

    # Parse duration
    duration_iso = content.get("duration")
    duration_seconds = None
    if duration_iso:
        try:
            duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds())
        except:
            duration_seconds = None

    video_record = {
        "id": v["id"],
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "publishedAt": snippet.get("publishedAt"),
        "tags": ",".join(snippet.get("tags", [])),
        "categoryId": snippet.get("categoryId"),
        "defaultLanguage": snippet.get("defaultLanguage"),
        "defaultAudioLanguage": snippet.get("defaultAudioLanguage"),

        "thumbnail_default": snippet["thumbnails"]["default"]["url"],
        "thumbnail_high": snippet["thumbnails"]["high"]["url"],

        "duration": duration_seconds,
        "privacyStatus": status.get("privacyStatus"),

        "viewCount": stats.get("viewCount"),
        "likeCount": stats.get("likeCount"),
        "commentCount": stats.get("commentCount"),

        # Add channel info
        "channel_id": channel_info["channel_id"],
        "channel_title": channel_info["channel_title"],
        "channel_description": channel_info["channel_description"],
        "channel_country": channel_info["channel_country"],
        "channel_thumbnail": channel_info["channel_thumbnail"],
        "channel_subscriber": channel_info["channel_subscriber"],
        "channel_videoCount": channel_info["channel_videoCount"]
    }

    final_data.append(video_record)

# ------------------------------------
# STEP 4: Save CSV
# ------------------------------------
df = pd.DataFrame(final_data)
df.to_csv("youtube_50_videos.csv", index=False, encoding="utf-8")

print("CSV saved successfully: youtube_50_videos.csv")  

