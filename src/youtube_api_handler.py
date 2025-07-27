import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st

# Function to create the YouTube service object (cached as a resource)
@st.cache_resource
def get_youtube_service(api_key):
    """Caches the YouTube API service object to avoid re-creation on reruns."""
    return build('youtube', 'v3', developerKey=api_key)

class YouTubeAPIHandler:
    def __init__(self, api_key):
        # Use the cached service object
        self.youtube = get_youtube_service(api_key)

    @st.cache_data(ttl=3600) # Cache API responses for 1 hour
    # CHANGE self TO _self HERE:
    def get_channel_details(_self, channel_id): # <--- MODIFIED
        """Fetches details for a given YouTube channel ID."""
        try:
            # Use _self.youtube inside the method, as that's the parameter name now
            request = _self.youtube.channels().list(
                part="snippet,statistics,contentDetails",
                id=channel_id
            )
            response = request.execute()

            if not response['items']:
                st.error(f"No channel found for ID: {channel_id}")
                return None

            channel_data = response['items'][0]
            snippet = channel_data['snippet']
            statistics = channel_data['statistics']
            content_details = channel_data['contentDetails']

            channel_info = {
                'channel_id': channel_data['id'],
                'channel_name': snippet['title'],
                'subscribers': int(statistics.get('subscriberCount', 0)),
                'total_videos': int(statistics.get('videoCount', 0)),
                'uploads_playlist_id': content_details['relatedPlaylists']['uploads']
            }
            return channel_info
        except HttpError as e:
            st.error(f"YouTube API Error (Channel Details): {e}")
            return None
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
            return None

    @st.cache_data(ttl=3600)
    # CHANGE self TO _self HERE:
    def get_playlist_video_ids(_self, playlist_id): # <--- MODIFIED
        """Fetches video IDs from a given playlist ID."""
        video_ids = []
        next_page_token = None

        try:
            # Use _self.youtube inside the method
            while True:
                request = _self.youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist_id,
                    maxResults=50, # Max allowed per request
                    pageToken=next_page_token
                )
                response = request.execute()

                for item in response['items']:
                    video_ids.append(item['contentDetails']['videoId'])

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            return video_ids
        except HttpError as e:
            st.error(f"YouTube API Error (Playlist Items): {e}")
            return []
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
            return []

    @st.cache_data(ttl=3600)
    # CHANGE self TO _self HERE:
    def get_video_details(_self, video_ids): # <--- MODIFIED
        """Fetches details for a list of video IDs."""
        if not video_ids:
            return []

        # YouTube API allows max 50 video IDs per request
        video_details_list = []
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            try:
                # Use _self.youtube inside the method
                request = _self.youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=",".join(batch_ids)
                )
                response = request.execute()

                for item in response['items']:
                    snippet = item['snippet']
                    statistics = item['statistics']
                    content_details = item['contentDetails']

                    video_details = {
                        'video_id': item['id'],
                        'channel_id': snippet['channelId'],
                        'title': snippet['title'],
                        'published_date': snippet['publishedAt'],
                        'views': int(statistics.get('viewCount', 0)),
                        'likes': int(statistics.get('likeCount', 0)),
                        'comments_count': int(statistics.get('commentCount', 0)),
                        'duration': content_details.get('duration') # ISO 8601 duration
                    }
                    video_details_list.append(video_details)
            except HttpError as e:
                st.error(f"YouTube API Error (Video Details batch {i}-{i+50}): {e}")
                continue # Try next batch
            except Exception as e:
                st.error(f"An unexpected error occurred during video details fetch: {e}")
                continue
        return video_details_list

    @st.cache_data(ttl=3600)
    # CHANGE self TO _self HERE:
    def get_comments_of_video(_self, video_id, max_results=100): # <--- MODIFIED
        """Fetches top-level comments for a given video ID."""
        comments_list = []
        next_page_token = None
        collected_comments = 0

        try:
            # Use _self.youtube inside the method
            while collected_comments < max_results:
                request = _self.youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=min(100, max_results - collected_comments), # Max 100 per request
                    pageToken=next_page_token
                )
                response = request.execute()

                for item in response['items']:
                    snippet = item['snippet']['topLevelComment']['snippet']
                    comment_info = {
                        'comment_id': item['id'],
                        'video_id': video_id,
                        'author': snippet['authorDisplayName'],
                        'comment_text': snippet['textDisplay'],
                        'published_date': snippet['publishedAt']
                    }
                    comments_list.append(comment_info)
                    collected_comments += 1
                    if collected_comments >= max_results:
                        break

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            return comments_list
        except HttpError as e:
            if e.resp.status == 403 and "commentsDisabled" in str(e):
                st.warning(f"Comments are disabled for video ID: {video_id}")
            else:
                st.error(f"YouTube API Error (Comments): {e}")
            return []
        except Exception as e:
            st.error(f"An unexpected error occurred during comments fetch: {e}")
            return []