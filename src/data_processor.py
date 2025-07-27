import pandas as pd
from datetime import datetime
import re

def parse_duration(duration_str):
    """
    Parses ISO 8601 duration string (e.g., 'PT1H2M3S') to seconds.
    """
    if not isinstance(duration_str, str):
        return 0

    duration_re = re.compile(r'P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = duration_re.match(duration_str)
    if not match:
        return 0

    days, hours, minutes, seconds = [int(x) if x else 0 for x in match.groups()]
    total_seconds = (days * 24 * 3600) + (hours * 3600) + (minutes * 60) + seconds
    return total_seconds

def process_channel_data(channel_details):
    """Processes raw channel details into a DataFrame row."""
    if not channel_details:
        return pd.DataFrame()
    return pd.DataFrame([channel_details])

def process_video_data(video_details_list):
    """Processes raw video details into a DataFrame."""
    if not video_details_list:
        return pd.DataFrame()

    df = pd.DataFrame(video_details_list)
    df['published_date'] = pd.to_datetime(df['published_date']).dt.date
    df['duration_seconds'] = df['duration'].apply(parse_duration)
    # Drop the original ISO duration string if not needed
    df = df.drop(columns=['duration'])
    return df

def process_comment_data(comments_list):
    """Processes raw comment details into a DataFrame."""
    if not comments_list:
        return pd.DataFrame()

    df = pd.DataFrame(comments_list)
    df['published_date'] = pd.to_datetime(df['published_date']).dt.date
    return df