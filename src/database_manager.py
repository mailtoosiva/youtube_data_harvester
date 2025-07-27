import sqlalchemy
from sqlalchemy import create_engine, text, inspect
from sqlalchemy import Column, String, Integer, BigInteger, Date, Time, DECIMAL, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import streamlit as st

Base = declarative_base()

class Channel(Base):
    __tablename__ = 'channels'
    channel_id = Column(String(255), primary_key=True)
    channel_name = Column(String(255))
    subscribers = Column(BigInteger)
    total_videos = Column(Integer)
    uploads_playlist_id = Column(String(255))

class Video(Base):
    __tablename__ = 'videos'
    video_id = Column(String(255), primary_key=True)
    channel_id = Column(String(255))
    title = Column(String(500))
    published_date = Column(Date)
    views = Column(BigInteger)
    likes = Column(BigInteger)
    comments_count = Column(BigInteger)
    duration_seconds = Column(Integer) # Store duration in seconds

    __table_args__ = (
        sqlalchemy.ForeignKeyConstraint(
            ['channel_id'], ['channels.channel_id'],
            name='fk_channel_id_videos'
        ),
    )

class Comment(Base):
    __tablename__ = 'comments'
    comment_id = Column(String(255), primary_key=True)
    video_id = Column(String(255))
    author = Column(String(255))
    comment_text = Column(String(1000))
    published_date = Column(Date)

    __table_args__ = (
        sqlalchemy.ForeignKeyConstraint(
            ['video_id'], ['videos.video_id'],
            name='fk_video_id_comments'
        ),
    )


class DatabaseManager:
    def __init__(self, db_type='mysql'):
        if db_type == 'mysql':
            self.db_url = (
                f"mysql+mysqlconnector://{st.secrets['DB_USER']}:"
                f"{st.secrets['DB_PASSWORD']}@{st.secrets['DB_HOST']}/"
                f"{st.secrets['DB_NAME']}"
            )
        elif db_type == 'postgresql':
            self.db_url = (
                f"postgresql+psycopg2://{st.secrets['PG_DB_USER']}:"
                f"{st.secrets['PG_DB_PASSWORD']}@{st.secrets['PG_DB_HOST']}/"
                f"{st.secrets['PG_DB_NAME']}"
            )
        else:
            raise ValueError("Unsupported database type. Choose 'mysql' or 'postgresql'.")

        self.engine = create_engine(self.db_url)
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        """Creates tables if they don't exist."""
        try:
            inspector = inspect(self.engine)
            if not inspector.has_table('channels') or not inspector.has_table('videos') or not inspector.has_table('comments'):
                Base.metadata.create_all(self.engine)
                st.success("Database tables created successfully (if they didn't exist).")
            else:
                st.info("Database tables already exist.")
        except Exception as e:
            st.error(f"Error creating tables: {e}")

    def insert_channel_data(self, df_channels):
        """Inserts channel data into the database."""
        if df_channels.empty:
            return
        session = self.Session()
        try:
            for index, row in df_channels.iterrows():
                # Check if channel already exists
                existing_channel = session.query(Channel).filter_by(channel_id=row['channel_id']).first()
                if existing_channel:
                    # Update existing channel data (optional, based on project needs)
                    existing_channel.channel_name = row['channel_name']
                    existing_channel.subscribers = row['subscribers']
                    existing_channel.total_videos = row['total_videos']
                    existing_channel.uploads_playlist_id = row['uploads_playlist_id']
                else:
                    channel = Channel(**row.to_dict())
                    session.add(channel)
            session.commit()
            st.success(f"Inserted/updated {len(df_channels)} channel(s).")
        except Exception as e:
            session.rollback()
            st.error(f"Error inserting channel data: {e}")
        finally:
            session.close()

    def insert_video_data(self, df_videos):
        """Inserts video data into the database."""
        if df_videos.empty:
            return
        session = self.Session()
        try:
            for index, row in df_videos.iterrows():
                existing_video = session.query(Video).filter_by(video_id=row['video_id']).first()
                if existing_video:
                    # Update existing video data
                    existing_video.title = row['title']
                    existing_video.views = row['views']
                    existing_video.likes = row['likes']
                    existing_video.comments_count = row['comments_count']
                    existing_video.duration_seconds = row['duration_seconds']
                    existing_video.published_date = row['published_date']
                else:
                    video = Video(**row.to_dict())
                    session.add(video)
            session.commit()
            st.success(f"Inserted/updated {len(df_videos)} video(s).")
        except Exception as e:
            session.rollback()
            st.error(f"Error inserting video data: {e}")
        finally:
            session.close()

    def insert_comment_data(self, df_comments):
        """Inserts comment data into the database."""
        if df_comments.empty:
            return
        session = self.Session()
        try:
            for index, row in df_comments.iterrows():
                existing_comment = session.query(Comment).filter_by(comment_id=row['comment_id']).first()
                if not existing_comment: # Only insert if comment does not exist (comments usually don't change)
                    comment = Comment(**row.to_dict())
                    session.add(comment)
            session.commit()
            st.success(f"Inserted {len(df_comments)} new comment(s).")
        except Exception as e:
            session.rollback()
            st.error(f"Error inserting comment data: {e}")
        finally:
            session.close()

    def execute_query(self, query):
        """Executes a raw SQL query and returns results as a DataFrame."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
        except Exception as e:
            st.error(f"Error executing query: {e}")
            return pd.DataFrame()

    def get_all_channel_names(self):
        """Fetches all channel names from the database."""
        session = self.Session()
        try:
            channels = session.query(Channel.channel_name, Channel.channel_id).all()
            return [{"name": name, "id": id} for name, id in channels]
        except Exception as e:
            st.error(f"Error fetching channel names: {e}")
            return []
        finally:
            session.close()