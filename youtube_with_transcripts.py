import pandas as pd
import isodate
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
import time

class YouTubeDataFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=api_key)
    
    def get_transcript(self, video_id):
        """Fetch transcript for a video"""
        try:
            # Try to get transcript in English first
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            
            # Try to get manually created transcript first
            try:
                transcript = transcript_list.find_manually_created_transcript(['en'])
            except:
                # If no manual transcript, try auto-generated
                transcript = transcript_list.find_generated_transcript(['en'])
            
            # Fetch the actual transcript data
            transcript_data = transcript.fetch()
            
            # Combine all text into a single string
            full_transcript = " ".join([entry['text'] for entry in transcript_data])
            return full_transcript
            
        except TranscriptsDisabled:
            return "Transcripts disabled for this video"
        except NoTranscriptFound:
            return "No transcript found"
        except Exception as e:
            return f"Error fetching transcript: {str(e)}"
    
    def get_channel_details(self, channel_id):
        """Fetch channel details"""
        try:
            request = self.youtube.channels().list(
                part='snippet,statistics',
                id=channel_id
            )
            response = request.execute()
            
            if response['items']:
                channel = response['items'][0]
                snippet = channel['snippet']
                stats = channel['statistics']
                
                return {
                    'channel_id': channel_id,
                    'channel_title': snippet.get('title', ''),
                    'channel_description': snippet.get('description', ''),
                    'channel_country': snippet.get('country', ''),
                    'channel_thumbnail': snippet['thumbnails']['default']['url'],
                    'channel_subscriberCount': stats.get('subscriberCount', '0'),
                    'channel_videoCount': stats.get('videoCount', '0')
                }
            return None
        except HttpError as e:
            print(f"Error fetching channel details: {e}")
            return None
    
    def get_video_ids(self, channel_id, max_results=50):
        """Fetch video IDs from a channel"""
        try:
            video_ids = []
            next_page_token = None
            
            while len(video_ids) < max_results:
                request = self.youtube.search().list(
                    part='id',
                    channelId=channel_id,
                    maxResults=min(50, max_results - len(video_ids)),
                    order='date',
                    type='video',
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response['items']:
                    if item['id']['kind'] == 'youtube#video':
                        video_ids.append(item['id']['videoId'])
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            
            return video_ids[:max_results]
        except HttpError as e:
            print(f"Error fetching video IDs: {e}")
            return []
    
    def get_video_details(self, video_ids):
        """Fetch detailed information for multiple videos"""
        try:
            all_videos_data = []
            
            for i in range(0, len(video_ids), 50):
                batch_ids = video_ids[i:i+50]
                
                request = self.youtube.videos().list(
                    part='snippet,contentDetails,statistics,status',
                    id=','.join(batch_ids)
                )
                response = request.execute()
                
                for item in response['items']:
                    video_data = self._parse_video_data(item)
                    
                    # Get transcript for this video
                    print(f"  📝 Fetching transcript for video: {item['id'][:10]}...")
                    transcript = self.get_transcript(item['id'])
                    video_data['transcript'] = transcript
                    
                    all_videos_data.append(video_data)
                    time.sleep(0.5)  # Avoid being blocked
                
                time.sleep(1)
            
            return all_videos_data
        except HttpError as e:
            print(f"Error fetching video details: {e}")
            return []
    
    def _parse_video_data(self, item):
        """Parse individual video data"""
        snippet = item['snippet']
        content_details = item['contentDetails']
        statistics = item.get('statistics', {})
        status = item.get('status', {})
        
        # Parse duration from ISO 8601 format
        duration = content_details.get('duration', 'PT0S')
        try:
            duration_parsed = isodate.parse_duration(duration)
            duration_str = str(duration_parsed)
        except:
            duration_str = duration
        
        # Get thumbnails
        thumbnails = snippet.get('thumbnails', {})
        thumbnail_default = thumbnails.get('default', {}).get('url', '')
        thumbnail_high = thumbnails.get('high', {}).get('url', '')
        
        return {
            'id': item['id'],
            'title': snippet.get('title', ''),
            'description': snippet.get('description', ''),
            'publishedAt': snippet.get('publishedAt', ''),
            'tags': ', '.join(snippet.get('tags', [])),
            'categoryId': snippet.get('categoryId', ''),
            'defaultLanguage': snippet.get('defaultLanguage', ''),
            'defaultAudioLanguage': snippet.get('defaultAudioLanguage', ''),
            'thumbnail_default': thumbnail_default,
            'thumbnail_high': thumbnail_high,
            'duration': duration_str,
            'viewCount': statistics.get('viewCount', '0'),
            'likeCount': statistics.get('likeCount', '0'),
            'commentCount': statistics.get('commentCount', '0'),
            'privacyStatus': status.get('privacyStatus', ''),
            'transcript': ''  # Will be filled later
        }
    
    def fetch_channel_videos_data(self, channel_id, max_videos=50):
        """Main function to fetch all data"""
        print("🔍 Fetching channel details...")
        channel_data = self.get_channel_details(channel_id)
        
        if not channel_data:
            print("❌ Failed to fetch channel details")
            return None
        
        print(f"📺 Fetching video IDs from channel: {channel_data['channel_title']}")
        video_ids = self.get_video_ids(channel_id, max_videos)
        
        if not video_ids:
            print("❌ No videos found or failed to fetch video IDs")
            return None
        
        print(f"✅ Found {len(video_ids)} videos. Fetching details and transcripts...")
        videos_data = self.get_video_details(video_ids)
        
        # Combine channel data with each video
        for video in videos_data:
            video.update(channel_data)
        
        return videos_data

def save_to_csv(data, filename='youtube_videos_with_transcripts.csv'):
    """Save data to CSV file"""
    if not data:
        print("❌ No data to save")
        return
    
    df = pd.DataFrame(data)
    
    # Define column order - adding transcript column
    columns = [
        'id', 'title', 'description', 'publishedAt', 'tags', 'categoryId',
        'defaultLanguage', 'defaultAudioLanguage', 'thumbnail_default',
        'thumbnail_high', 'duration', 'viewCount', 'likeCount', 'commentCount',
        'privacyStatus', 'transcript', 'channel_id', 'channel_title', 'channel_description',
        'channel_country', 'channel_thumbnail', 'channel_subscriberCount',
        'channel_videoCount'
    ]
    
    # Reorder columns and save
    df = df[columns]
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"✅ Data saved to {filename}")
    print(f"📊 Total videos: {len(data)}")
    
    # Show transcript availability
    transcripts_available = df['transcript'].apply(lambda x: len(str(x)) > 50).sum()
    print(f"📝 Transcripts available for: {transcripts_available} videos")
    
    return df

# Main execution
if __name__ == "__main__":
    # Replace with your API key and channel ID
    API_KEY = "AIzaSyC-HobSbcVAUr4bFxm3ODc_HblmRwrMTmQ"
    CHANNEL_ID = "UC7qfffOXz7AYlnJlcfkMmZw"  # Google Developers
    
    # Check if youtube-transcript-api is installed
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        print("✅ youtube-transcript-api is installed")
    except ImportError:
        print("❌ youtube-transcript-api not found!")
        print("Please run: pip install youtube-transcript-api")
        exit()
    
    # Initialize fetcher
    fetcher = YouTubeDataFetcher(API_KEY)
    
    # Fetch data
    videos_data = fetcher.fetch_channel_videos_data(CHANNEL_ID, max_videos=50)
    
    # Save to CSV
    if videos_data:
        df = save_to_csv(videos_data, 'youtube_videos_with_transcripts.csv')
        
        # Display summary
        print("\n📋 Summary:")
        print(f"Channel: {videos_data[0]['channel_title']}")
        print(f"Subscribers: {videos_data[0]['channel_subscriberCount']}")
        print(f"Total Videos: {videos_data[0]['channel_videoCount']}")
        print(f"Videos fetched: {len(videos_data)}")
        
        # Show first few rows with transcript preview
        print("\n📄 Sample transcript (first 200 chars):")
        if len(videos_data) > 0 and videos_data[0]['transcript']:
            print(videos_data[0]['transcript'][:200] + "...")
        else:
            print("No transcript available for first video")
    else:
        print("❌ Failed to fetch data")