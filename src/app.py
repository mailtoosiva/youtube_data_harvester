import streamlit as st
import pandas as pd
from src.youtube_api_handler import YouTubeAPIHandler
from src.database_manager import DatabaseManager
from src.data_processor import process_channel_data, process_video_data, process_comment_data

# Initialize API and DB handlers
try:
    api_key = st.secrets["YOUTUBE_API_KEY"]
    youtube_handler = YouTubeAPIHandler(api_key)
    db_manager = DatabaseManager(db_type='mysql') # Or 'postgresql'
except KeyError:
    st.error("API Key or Database credentials not found in .streamlit/secrets.toml. Please configure them.")
    st.stop()
except Exception as e:
    st.error(f"Failed to initialize. Check your configuration: {e}")
    st.stop()

# --- Streamlit App Setup ---
st.set_page_config(layout="wide", page_title="YouTube Data Harvester")

st.title("YouTube Data Harvesting and Warehousing")
st.subheader("Collect, Store, and Analyze YouTube Channel Data")

# Session state to store collected channel data temporarily
if 'collected_channels_data' not in st.session_state:
    st.session_state['collected_channels_data'] = {} # {channel_id: {'channel_info': {}, 'videos': [], 'comments': []}}

# --- Sidebar for Navigation ---
st.sidebar.header("Navigation")
menu_options = ["Data Collection", "Data Warehousing", "Data Analysis"]
choice = st.sidebar.radio("Go to", menu_options)

# --- Data Collection Section ---
if choice == "Data Collection":
    st.header("Collect YouTube Channel Data")
    channel_id_input = st.text_input("Enter YouTube Channel ID:")

    if st.button("Fetch Channel Data"):
        if channel_id_input:
            with st.spinner(f"Fetching data for channel ID: {channel_id_input}..."):
                channel_info = youtube_handler.get_channel_details(channel_id_input)
                if channel_info:
                    st.success(f"Fetched channel: {channel_info['channel_name']}")
                    st.write("### Channel Details:")
                    st.json(channel_info)

                    # Store in session state for potential warehousing
                    if channel_id_input not in st.session_state['collected_channels_data']:
                        st.session_state['collected_channels_data'][channel_id_input] = {
                            'channel_info': channel_info,
                            'videos': [],
                            'comments': []
                        }
                    else:
                        st.info("Channel already in temporary collection. Updating details.")
                        st.session_state['collected_channels_data'][channel_id_input]['channel_info'] = channel_info

                    # Fetch videos and comments (optional, can be done during warehousing)
                    if st.checkbox(f"Fetch all videos and comments for {channel_info['channel_name']} now? (Can be time-consuming)"):
                        with st.spinner(f"Fetching videos for {channel_info['channel_name']}..."):
                            video_ids = youtube_handler.get_playlist_video_ids(channel_info['uploads_playlist_id'])
                            st.info(f"Found {len(video_ids)} videos for {channel_info['channel_name']}.")
                            if video_ids:
                                video_details = youtube_handler.get_video_details(video_ids)
                                st.session_state['collected_channels_data'][channel_id_input]['videos'] = video_details
                                st.success(f"Fetched details for {len(video_details)} videos.")
                                # Fetch comments for each video (can be very slow/quota heavy)
                                if st.checkbox("Fetch comments for all fetched videos? (Extremely quota intensive)"):
                                    all_comments = []
                                    progress_text = "Operation in progress. Please wait."
                                    comments_bar = st.progress(0, text=progress_text)
                                    for i, vid_det in enumerate(video_details):
                                        comments = youtube_handler.get_comments_of_video(vid_det['video_id'])
                                        all_comments.extend(comments)
                                        comments_bar.progress((i + 1) / len(video_details), text=f"Fetched comments for {i+1}/{len(video_details)} videos.")
                                    st.session_state['collected_channels_data'][channel_id_input]['comments'] = all_comments
                                    st.success(f"Fetched {len(all_comments)} comments in total.")
                                    comments_bar.empty()
                            else:
                                st.warning("No videos found for this channel's uploads playlist.")
                else:
                    st.error("Could not fetch channel details. Check ID or API key.")
        else:
            st.warning("Please enter a YouTube Channel ID.")

    st.markdown("---")
    st.subheader("Channels Collected for Warehousing:")
    if st.session_state['collected_channels_data']:
        df_channels_temp = pd.DataFrame([ch['channel_info'] for ch in st.session_state['collected_channels_data'].values()])
        st.dataframe(df_channels_temp[['channel_name', 'subscribers', 'total_videos']])
        if len(st.session_state['collected_channels_data']) >= 10:
            st.warning("You have collected data for 10 channels. Consider warehousing them.")
    else:
        st.info("No channels collected yet. Enter a Channel ID above.")

# --- Data Warehousing Section ---
elif choice == "Data Warehousing":
    st.header("Migrate Collected Data to SQL Data Warehouse")
    db_manager.create_tables() # Ensure tables exist

    if st.session_state['collected_channels_data']:
        st.info(f"You have {len(st.session_state['collected_channels_data'])} channels in temporary storage ready for migration.")

        if st.button("Migrate All Collected Data to SQL Database"):
            total_channels_migrated = 0
            total_videos_migrated = 0
            total_comments_migrated = 0

            with st.spinner("Migrating data... This may take a while depending on video/comment count."):
                for channel_id, data in st.session_state['collected_channels_data'].items():
                    # Process and insert channel data
                    df_channel = process_channel_data(data['channel_info'])
                    db_manager.insert_channel_data(df_channel)
                    total_channels_migrated += 1

                    # Fetch and insert video data for this channel
                    # If not already fetched, fetch now during warehousing
                    current_videos_data = data['videos']
                    if not current_videos_data and data['channel_info']:
                        st.info(f"Fetching videos for {data['channel_info']['channel_name']} during migration...")
                        video_ids = youtube_handler.get_playlist_video_ids(data['channel_info']['uploads_playlist_id'])
                        current_videos_data = youtube_handler.get_video_details(video_ids)

                    df_videos = process_video_data(current_videos_data)
                    db_manager.insert_video_data(df_videos)
                    total_videos_migrated += len(df_videos)

                    # Fetch and insert comments for videos of this channel
                    # If not already fetched, fetch now during warehousing
                    current_comments_data = data['comments']
                    if not current_comments_data and current_videos_data:
                        st.info(f"Fetching comments for videos of {data['channel_info']['channel_name']} during migration (quota intensive)...")
                        for video_det in current_videos_data:
                             comments_for_video = youtube_handler.get_comments_of_video(video_det['video_id'])
                             current_comments_data.extend(comments_for_video)

                    df_comments = process_comment_data(current_comments_data)
                    db_manager.insert_comment_data(df_comments)
                    total_comments_migrated += len(df_comments)

                st.success(f"Migration complete!")
                st.info(f"Channels migrated: {total_channels_migrated}")
                st.info(f"Videos migrated: {total_videos_migrated}")
                st.info(f"Comments migrated: {total_comments_migrated}")

                # Clear session state after successful migration
                st.session_state['collected_channels_data'] = {}
                st.info("Temporary collected data cleared from session.")
    else:
        st.info("No channels to migrate. Please collect data in the 'Data Collection' section first.")

# --- Data Analysis Section ---
elif choice == "Data Analysis":
    st.header("Analyze YouTube Data from SQL Database")

    all_channels = db_manager.get_all_channel_names()
    if all_channels:
        channel_names_map = {ch['name']: ch['id'] for ch in all_channels}
        selected_channel_name = st.selectbox(
            "Select a Channel to Filter Analysis (Optional):",
            ["All Channels"] + list(channel_names_map.keys())
        )
        selected_channel_id = channel_names_map.get(selected_channel_name)
    else:
        st.warning("No channels found in the database. Please migrate data first.")
        selected_channel_name = "All Channels"
        selected_channel_id = None

    analysis_queries = {
        "Names of all videos and their corresponding channels":
            """
            SELECT V.title AS video_title, C.channel_name
            FROM Videos V
            JOIN Channels C ON V.channel_id = C.channel_id
            """,
        "Channels with the most videos":
            """
            SELECT C.channel_name, C.total_videos
            FROM Channels C
            ORDER BY C.total_videos DESC
            """,
        "Top 10 most viewed videos and their channels":
            """
            SELECT V.title AS video_title, C.channel_name, V.views
            FROM Videos V
            JOIN Channels C ON V.channel_id = C.channel_id
            ORDER BY V.views DESC
            LIMIT 10
            """,
        "Comments count per video and video names":
            """
            SELECT V.title AS video_title, V.comments_count
            FROM Videos V
            """,
        "Videos with the highest likes and their channels":
            """
            SELECT V.title AS video_title, C.channel_name, V.likes
            FROM Videos V
            JOIN Channels C ON V.channel_id = C.channel_id
            ORDER BY V.likes DESC
            """,
        "Total likes and dislikes for each video and video names":
            """
            SELECT title AS video_title, likes, 0 AS dislikes_placeholder -- Dislikes not available from YouTube API
            FROM Videos
            """,
        "Total views per channel and channel names":
            """
            SELECT C.channel_name, SUM(V.views) AS total_channel_views
            FROM Channels C
            JOIN Videos V ON C.channel_id = V.channel_id
            GROUP BY C.channel_name
            ORDER BY total_channel_views DESC
            """,
        "Channels with videos published in 2022":
            """
            SELECT DISTINCT C.channel_name
            FROM Channels C
            JOIN Videos V ON C.channel_id = V.channel_id
            WHERE YEAR(V.published_date) = 2022 -- Use EXTRACT(YEAR FROM V.published_date) for PostgreSQL
            """,
        "Average duration of all videos in each channel":
            """
            SELECT C.channel_name, AVG(V.duration_seconds) AS average_video_duration_seconds
            FROM Channels C
            JOIN Videos V ON C.channel_id = V.channel_id
            GROUP BY C.channel_name
            """,
        "Videos with the highest comments and their channels":
            """
            SELECT V.title AS video_title, C.channel_name, V.comments_count
            FROM Videos V
            JOIN Channels C ON V.channel_id = C.channel_id
            ORDER BY V.comments_count DESC
            """
    }

    selected_query_description = st.selectbox(
        "Select an Analysis Query:",
        list(analysis_queries.keys())
    )

    full_query = analysis_queries[selected_query_description]

    # Add channel filter to queries if a specific channel is selected
    if selected_channel_name != "All Channels" and selected_channel_id:
        if "WHERE" in full_query:
            full_query += f" AND C.channel_id = '{selected_channel_id}'"
        else:
            # Need to carefully insert WHERE clause if it's a simple SELECT or JOIN
            # This is a simplified approach, for complex queries, you might need more logic
            if "FROM Videos V" in full_query and "JOIN Channels C" in full_query:
                full_query += f" WHERE C.channel_id = '{selected_channel_id}'"
            elif "FROM Channels C" in full_query: # For queries only on Channels table
                 full_query += f" WHERE C.channel_id = '{selected_channel_id}'"


    st.code(full_query, language="sql")

    if st.button("Run Analysis Query"):
        with st.spinner("Executing query..."):
            result_df = db_manager.execute_query(full_query)
            if not result_df.empty:
                st.dataframe(result_df)
                st.download_button(
                    label="Download data as CSV",
                    data=result_df.to_csv(index=False).encode('utf-8'),
                    file_name=f"{selected_query_description.replace(' ', '_').lower()}.csv",
                    mime="text/csv",
                )
            else:
                st.info("No results found for this query.")