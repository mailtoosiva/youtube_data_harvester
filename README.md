# YouTube Data Harvesting and Warehousing using SQL and Streamlit

## Project Overview

This project aims to develop a user-friendly Streamlit application that utilizes the Google YouTube Data API v3 to extract comprehensive information from specified YouTube channels, store this data in a SQL database (MySQL or PostgreSQL), and enable users to perform various analytical queries directly within the application.

## Problem Statement

The core problem is to provide a seamless way for users to:
1.  **Retrieve YouTube Channel Data:** Fetch details like channel name, subscribers, total video count, playlist IDs, and for each video, collect video ID, likes, comments count, views, and published date.
2.  **Batch Data Collection:** Allow collection of data for up to 10 different YouTube channels.
3.  **SQL Data Warehousing:** Store the collected data in a structured SQL database (MySQL or PostgreSQL).
4.  **Interactive Data Analysis:** Enable users to query and analyze the stored data using pre-defined SQL queries, with results displayed in the Streamlit application.

## Skills Takeaway

* **Python Scripting:** Core logic for data handling and application flow.
* **Data Collection:** Proficient use of Google YouTube Data API v3.
* **Streamlit:** Building interactive and user-friendly web applications for data display and interaction.
* **API Integration:** Connecting to and utilizing external web services.
* **Data Management using SQL:** Designing database schemas, performing CRUD (Create, Read, Update, Delete) operations, and writing complex SQL queries.
* **Database Interaction:** Working with either MySQL or PostgreSQL.
* **Modular Programming:** Organizing code into reusable and maintainable functional blocks.
* **Version Control:** Managing project code with Git and GitHub.
* **Documentation:** Creating clear and comprehensive project documentation.

## Architecture and Workflow

The project follows a modular architecture:

1.  **Streamlit App (`app.py`):** Provides the user interface, handles user input, and orchestrates calls to other modules.
2.  **YouTube API Handler (`src/youtube_api_handler.py`):** Encapsulates all interactions with the YouTube Data API. It handles authentication, fetches channel details, video IDs from playlists, video statistics, and comments.
3.  **Data Processor (`src/data_processor.py`):** Responsible for cleaning, transforming, and structuring the raw data received from the YouTube API into a format suitable for storage in a SQL database (e.g., Pandas DataFrames).
4.  **Database Manager (`src/database_manager.py`):** Manages all database operations, including creating tables, inserting data, and executing SQL queries. It uses SQLAlchemy for ORM capabilities, allowing flexibility between different SQL databases.
5.  **SQL Database (MySQL/PostgreSQL):** The persistent storage for all harvested YouTube data, structured into `channels`, `videos`, and `comments` tables.

```mermaid
graph TD
    User --&gt; Streamlit_App
    Streamlit_App --&gt; |Input Channel ID| YouTube_API_Handler
    YouTube_API_Handler --&gt; |Fetch Data| YouTube_API
    YouTube_API --&gt; |JSON Response| YouTube_API_Handler
    YouTube_API_Handler --&gt; |Raw Data| Data_Processor
    Data_Processor --&gt; |Cleaned Data (Pandas)| Streamlit_App
    Streamlit_App --&gt; |Trigger Migration| Database_Manager
    Database_Manager --&gt; |Insert Data / Create Tables| SQL_Database
    Streamlit_App --&gt; |Select Query| Database_Manager
    Database_Manager --&gt; |Execute Query| SQL_Database
    SQL_Database --&gt; |Query Results| Database_Manager
    Database_Manager --&gt; |Results (Pandas)| Streamlit_App
    Streamlit_App --&gt; |Display Table/Chart| User
