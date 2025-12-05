# get_kaggle_videos.py
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
import pandas as pd
import time

API_KEY = "Put your key here"  
CHANNEL_SEARCH_NAME = "Kaggle"  # we want Kaggle channel
NUM_VIDEOS = 50

youtube = build("youtube", "v3", developerKey=API_KEY)

def find_channel_id_by_name(name):
    # search for a channel named "Kaggle" and take the first match
    req = youtube.search().list(part="snippet", q=name, type="channel", maxResults=5)
    res = req.execute()
    items = res.get("items", [])
    if not items:
        raise RuntimeError("Channel not found via search.")
    # best result (first) -> channelId
    return items[0]["snippet"]["channelId"]

def get_channel_info(channel_id):
    resp = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id).execute()
    items = resp.get("items", [])
    if not items:
        raise RuntimeError("Channel not found by ID.")
    return items[0]

def get_first_n_video_ids_from_uploads(uploads_playlist_id, n=50):
    video_ids = []
    nextPageToken = None
    while len(video_ids) < n:
        resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=min(50, n - len(video_ids)),
            pageToken=nextPageToken
        ).execute()
        for it in resp.get("items", []):
            vid = it["contentDetails"]["videoId"]
            video_ids.append(vid)
            if len(video_ids) >= n:
                break
        nextPageToken = resp.get("nextPageToken")
        if not nextPageToken:
            break
        time.sleep(0.1)
    return video_ids

def fetch_videos_metadata(video_ids):
    # videos.list supports up to 50 ids at once
    vids = ",".join(video_ids)
    resp = youtube.videos().list(part="snippet,contentDetails,statistics,status", id=vids, maxResults=50).execute()
    return resp.get("items", [])

def get_transcript_text(video_id):
    # optional: fetch transcript (auto-generated or uploaded)
    try:
        segs = YouTubeTranscriptApi.get_transcript(video_id)
        text = " ".join([s.get("text","") for s in segs])
        return text
    except (TranscriptsDisabled, NoTranscriptFound):
        return ""
    except Exception:
        return ""

def main():
    print("Finding channel ID for:", CHANNEL_SEARCH_NAME)
    channel_id = find_channel_id_by_name(CHANNEL_SEARCH_NAME)
    print("Channel ID:", channel_id)

    ch = get_channel_info(channel_id)
    uploads_pl = ch["contentDetails"]["relatedPlaylists"]["uploads"]
    print("Uploads playlist:", uploads_pl)

    video_ids = get_first_n_video_ids_from_uploads(uploads_pl, n=NUM_VIDEOS)
    print("Collected", len(video_ids), "video IDs")

    videos_meta = fetch_videos_metadata(video_ids)
    print("Fetched metadata for", len(videos_meta), "videos")

    rows = []
    for v in videos_meta:
        snip = v.get("snippet", {})
        stats = v.get("statistics", {})
        content = v.get("contentDetails", {})
        status = v.get("status", {})

        vid = v.get("id")
        transcript_text = get_transcript_text(vid)  # optional; remove if slow

        row = {
            "id": vid,
            "title": snip.get("title",""),
            "description": snip.get("description",""),
            "publishedAt": snip.get("publishedAt",""),
            "tags": "|".join(snip.get("tags", [])) if snip.get("tags") else "",
            "categoryId": snip.get("categoryId",""),
            "defaultLanguage": snip.get("defaultLanguage",""),
            "defaultAudioLanguage": snip.get("defaultAudioLanguage",""),
            "thumbnail_default": snip.get("thumbnails",{}).get("default",{}).get("url",""),
            "thumbnail_high": snip.get("thumbnails",{}).get("high",{}).get("url",""),
            "duration": content.get("duration",""),
            "viewCount": stats.get("viewCount",""),
            "likeCount": stats.get("likeCount",""),
            "commentCount": stats.get("commentCount",""),
            "privacyStatus": status.get("privacyStatus",""),
            "channel_id": ch.get("id",""),
            "channel_title": ch.get("snippet",{}).get("title",""),
            "channel_description": ch.get("snippet",{}).get("description",""),
            "channel_country": ch.get("snippet",{}).get("country",""),
            "channel_thumbnail": ch.get("snippet",{}).get("thumbnails",{}).get("default",{}).get("url",""),
            "channel_subscriberCount": ch.get("statistics",{}).get("subscriberCount",""),
            "channel_videoCount": ch.get("statistics",{}).get("videoCount",""),
            "transcript": transcript_text
        }
        rows.append(row)
        time.sleep(0.1)

    df = pd.DataFrame(rows)
    df.to_csv("kaggle_50_videos.csv", index=False, encoding="utf-8-sig")
    print("Saved kaggle_50_videos.csv with", len(df), "rows")

if __name__ == "__main__":

    main()
