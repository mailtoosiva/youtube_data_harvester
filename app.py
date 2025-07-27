import streamlit as st
import pandas as pd
# Import both the handler class and the cached service function for clarity
from src.youtube_api_handler import YouTubeAPIHandler, get_youtube_service
from src.database_manager import DatabaseManager
from src.data_processor import process_channel_data, process_video_data, process_comment_data

# --- Streamlit App Setup (THIS MUST BE THE ABSOLUTE FIRST STREAMLIT COMMAND) ---
st.set_page_config(layout="wide", page_title="YouTube Data Harvester")

# Initialize API and DB handlers.
# These operations must come AFTER st.set_page_config() because they use st.secrets and st.error.
try:
    api_key = st.secrets["YOUTUBE_API_KEY"]
    youtube_handler = YouTubeAPIHandler(api_key)
    # Instantiate DatabaseManager. Set db_type according to your setup (e.g., 'mysql' or 'postgresql')
    db_manager = DatabaseManager(db_type='mysql')
except KeyError as e:
    st.error(f"Configuration error: Missing secret '{e}'. Please ensure .streamlit/secrets.toml is correctly configured with your API key and database credentials.")
    st.stop() # Stop the app execution if essential secrets are missing
except Exception as e:
    st.error(f"Failed to initialize. Check your API key or database configuration: {e}")
    st.stop() # Stop on other critical initialization errors

# Main app title and subtitle
st.title("YouTube Data Harvesting and Warehousing")
st.subheader("Collect, Store, and Analyze YouTube Channel Data")

# Session state to store collected channel data temporarily across Streamlit reruns.
# This prevents loss of data when users navigate between sections or interact with widgets.
if 'collected_channels_data' not in st.session_state:
    # Structure: {channel_id: {'channel_info': {}, 'videos': [], 'comments': []}}
    st.session_state['collected_channels_data'] = {}

# --- Sidebar for Navigation ---
st.sidebar.header("Navigation")
menu_options = ["Data Collection", "Data Warehousing", "Data Analysis"]
choice = st.sidebar.radio("Go to", menu_options)

# --- Section: Data Collection ---
if choice == "Data Collection":
    st.header("Collect YouTube Channel Data")
    channel_id_input = st.text_input("Enter YouTube Channel ID:")

    if st.button("Fetch Channel Data"):
        if channel_id_input:
            with st.spinner(f"Fetching data for channel ID: {channel_id_input}..."):
                # Call the API handler to get channel details
                channel_info = youtube_handler.get_channel_details(channel_id_input)
                if channel_info:
                    st.success(f"Fetched channel: {channel_info['channel_name']}")
                    st.write("### Channel Details:")
                    st.json(channel_info) # Display the raw JSON response for the channel

                    # Store the fetched channel information in session state
                    if channel_id_input not in st.session_state['collected_channels_data']:
                        st.session_state['collected_channels_data'][channel_id_input] = {
                            'channel_info': channel_info,
                            'videos': [], # Initialize videos list (will be populated if chosen)
                            'comments': [] # Initialize comments list (will be populated if chosen)
                        }
                    else:
                        st.info("Channel already in temporary collection. Updating details.")
                        # Update just the channel_info part if already present
                        st.session_state['collected_channels_data'][channel_id_input]['channel_info'] = channel_info

                    # Optional: Allow user to fetch all videos immediately. This can be time/quota intensive.
                    if st.checkbox(f"Fetch all videos for {channel_info['channel_name']} now? (Can be time-consuming for large channels)"):
                        with st.spinner(f"Fetching videos for {channel_info['channel_name']}..."):
                            video_ids = youtube_handler.get_playlist_video_ids(channel_info['uploads_playlist_id'])
                            st.info(f"Found {len(video_ids)} videos for {channel_info['channel_name']}.")
                            if video_ids:
                                video_details = youtube_handler.get_video_details(video_ids)
                                st.session_state['collected_channels_data'][channel_id_input]['videos'] = video_details
                                st.success(f"Fetched details for {len(video_details)} videos.")

                                # Optional: Allow user to fetch comments for all fetched videos. This is EXTREMELY quota intensive.
                                if st.checkbox("Fetch comments for all fetched videos? (Extremely quota intensive - use with caution!)"):
                                    all_comments = []
                                    progress_text = "Fetching comments..."
                                    comments_bar = st.progress(0, text=progress_text)
                                    for i, vid_det in enumerate(video_details):
                                        comments = youtube_handler.get_comments_of_video(vid_det['video_id'])
                                        all_comments.extend(comments)
                                        # Update progress bar
                                        comments_bar.progress((i + 1) / len(video_details), text=f"Fetched comments for {i+1}/{len(video_details)} videos.")
                                    st.session_state['collected_channels_data'][channel_id_input]['comments'] = all_comments
                                    st.success(f"Fetched {len(all_comments)} comments in total.")
                                    comments_bar.empty() # Clear the progress bar after completion
                            else:
                                st.warning("No videos found for this channel's uploads playlist.")
                else:
                    st.error("Could not fetch channel details. Please double-check the Channel ID or your API key, and ensure the channel is public.")
        else:
            st.warning("Please enter a YouTube Channel ID to fetch data.")

    st.markdown("---")
    st.subheader("Channels Collected for Warehousing (In App's Memory):")
    if st.session_state['collected_channels_data']:
        # Display collected channels in a compact DataFrame
        df_channels_temp = pd.DataFrame([ch['channel_info'] for ch in st.session_state['collected_channels_data'].values()])
        st.dataframe(df_channels_temp[['channel_name', 'subscribers', 'total_videos']])
        if len(st.session_state['collected_channels_data']) >= 10:
            st.warning("You have collected data for 10 channels. Consider warehousing them to free up memory.")
    else:
        st.info("No channels collected yet. Enter a Channel ID above to start collecting data.")

# --- Section: Data Warehousing ---
elif choice == "Data Warehousing":
    st.header("Migrate Collected Data to SQL Data Warehouse")
    # Call create_tables to ensure database schema is set up
    db_manager.create_tables()

    if st.session_state['collected_channels_data']:
        st.info(f"You have {len(st.session_state['collected_channels_data'])} channel(s) in temporary storage ready for migration.")

        if st.button("Migrate All Collected Data to SQL Database"):
            total_channels_migrated = 0
            total_videos_migrated = 0
            total_comments_migrated = 0

            with st.spinner("Migrating data... This may take a while depending on the number of videos and comments."):
                for channel_id, data in st.session_state['collected_channels_data'].items():
                    st.subheader(f"Migrating data for channel: {data['channel_info']['channel_name']}")

                    # 1. Process and insert channel data
                    df_channel = process_channel_data(data['channel_info'])
                    if not df_channel.empty:
                        db_manager.insert_channel_data(df_channel)
                        total_channels_migrated += len(df_channel)

                    # 2. Fetch and insert video data (if not already fetched during collection)
                    current_videos_data = data['videos']
                    if not current_videos_data and data['channel_info']:
                        st.info(f"Fetching videos for {data['channel_info']['channel_name']} during migration...")
                        video_ids = youtube_handler.get_playlist_video_ids(data['channel_info']['uploads_playlist_id'])
                        current_videos_data = youtube_handler.get_video_details(video_ids)
                        # Update session state with newly fetched videos
                        st.session_state['collected_channels_data'][channel_id]['videos'] = current_videos_data

                    df_videos = process_video_data(current_videos_data)
                    if not df_videos.empty:
                        db_manager.insert_video_data(df_videos)
                        total_videos_migrated += len(df_videos)

                    # 3. Fetch and insert comments for videos (if not already fetched)
                    current_comments_data = data['comments']
                    if not current_comments_data and current_videos_data:
                        st.info(f"Fetching comments for videos of {data['channel_info']['channel_name']} during migration (Quota intensive!)...")
                        # Iterate through each video to get its comments
                        for video_det in current_videos_data:
                             comments_for_video = youtube_handler.get_comments_of_video(video_det['video_id'])
                             current_comments_data.extend(comments_for_video)
                        # Update session state with newly fetched comments
                        st.session_state['collected_channels_data'][channel_id]['comments'] = current_comments_data

                    df_comments = process_comment_data(current_comments_data)
                    if not df_comments.empty:
                        db_manager.insert_comment_data(df_comments)
                        total_comments_migrated += len(df_comments)

                st.success(f"Migration complete!")
                st.info(f"Total Channels migrated: {total_channels_migrated}")
                st.info(f"Total Videos migrated: {total_videos_migrated}")
                st.info(f"Total Comments migrated: {total_comments_migrated}")

                # Clear the temporary collected data from session state after successful migration
                st.session_state['collected_channels_data'] = {}
                st.info("Temporary collected data cleared from app's memory.")
    else:
        st.info("No channels to migrate. Please collect data in the 'Data Collection' section first.")

# --- Section: Data Analysis ---
elif choice == "Data Analysis":
    st.header("Analyze YouTube Data from SQL Database")

    all_channels = db_manager.get_all_channel_names()
    if all_channels:
        # Create a dictionary to map channel names to their IDs for the selectbox
        channel_names_map = {ch['name']: ch['id'] for ch in all_channels}
        selected_channel_name = st.selectbox(
            "Select a Channel to Filter Analysis (Optional):",
            ["All Channels"] + sorted(list(channel_names_map.keys())) # Add "All Channels" as first option and sort others
        )
        selected_channel_id = channel_names_map.get(selected_channel_name) # Get the ID of the selected channel
    else:
        st.warning("No channels found in the database. Please migrate data first in the 'Data Warehousing' section.")
        selected_channel_name = "All Channels" # Default to 'All Channels' if no data is present
        selected_channel_id = None

    # Predefined analysis queries
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
            JOIN Channels C ON C.channel_id = V.channel_id
            ORDER BY V.likes DESC
            """,
        "Total likes and dislikes for each video and video names":
            """
            SELECT title AS video_title, likes, 0 AS dislikes_placeholder -- YouTube API does not provide dislikes directly
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
            WHERE YEAR(V.published_date) = 2022 -- Use YEAR() for MySQL. For PostgreSQL, use EXTRACT(YEAR FROM V.published_date)
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
            JOIN Channels C ON C.channel_id = V.channel_id
            ORDER BY V.comments_count DESC
            """
    }

    selected_query_description = st.selectbox(
        "Select an Analysis Query:",
        list(analysis_queries.keys()) # Display the user-friendly descriptions as options
    )

    full_query = analysis_queries[selected_query_description]

    # --- Start of FIX for SQL WHERE clause placement ---
    # Dynamically add channel filter to the selected query if a specific channel is chosen
    if selected_channel_name != "All Channels" and selected_channel_id:
        query_upper = full_query.upper()
        insert_point = -1 # Default to inserting at the end

        # Prioritize finding the position of GROUP BY or ORDER BY to insert WHERE before it
        if "GROUP BY" in query_upper:
            insert_point = query_upper.find("GROUP BY")
        elif "ORDER BY" in query_upper:
            insert_point = query_upper.find("ORDER BY")

        if insert_point != -1:
            # Insert the WHERE clause at the determined point
            # Ensure proper spacing around the inserted clause and that it targets the correct table alias (C or V)
            # We assume 'C' alias for Channels or 'V' for Videos as per current queries
            # This logic needs to be robust enough for all query structures.
            # For simplicity, we'll try to determine the alias from the query itself or use C as default.
            alias = 'C' # Default to Channel alias
            if "FROM VIDEOS V" in query_upper and "JOIN CHANNELS C" not in query_upper:
                alias = 'V' # If only Videos table is used directly

            full_query = (
                full_query[:insert_point].strip() +
                f" WHERE {alias}.channel_id = '{selected_channel_id}' " +
                full_query[insert_point:].strip()
            )
        else:
            # If no GROUP BY or ORDER BY, append the WHERE clause.
            # Check if there's already a WHERE clause to append with AND.
            if "WHERE" in query_upper:
                # Determine alias as above
                alias = 'C'
                if "FROM VIDEOS V" in query_upper and "JOIN CHANNELS C" not in query_upper:
                    alias = 'V'
                full_query += f" AND {alias}.channel_id = '{selected_channel_id}'"
            else:
                # If no WHERE clause yet, add it.
                # Determine alias as above
                alias = 'C'
                if "FROM VIDEOS V" in query_upper and "JOIN CHANNELS C" not in query_upper:
                    alias = 'V'
                full_query += f" WHERE {alias}.channel_id = '{selected_channel_id}'"
    # --- End of FIX for SQL WHERE clause placement ---

    # Optional: Allow the user to view the generated SQL query
    # This provides transparency for technical users without cluttering the UI for others.
    with st.expander("View Generated SQL Query"):
        st.code(full_query, language="sql")


    if st.button("Run Analysis Query"):
        with st.spinner("Executing query..."):
            result_df = db_manager.execute_query(full_query)
            if not result_df.empty:
                st.dataframe(result_df)
                # Provide a download button for the query results
                st.download_button(
                    label="Download data as CSV",
                    data=result_df.to_csv(index=False).encode('utf-8'),
                    file_name=f"{selected_query_description.replace(' ', '_').lower()}.csv",
                    mime="text/csv",
                )
            else:
                st.info("No results found for this query or selected channel.")