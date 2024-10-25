import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from config import Config
from utils.logging_utils import set_custom_logger

class Database:
    def __init__(self):
        self.logger = set_custom_logger()
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=Config.DATABASE_HOST,
                database=Config.DATABASE_NAME,
                user=Config.DATABASE_USER,
                password=Config.DATABASE_PASSWORD,
                port=Config.DATABASE_PORT
            )
            self.logger.info("Database connection established.")
        except Exception as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise

    @contextmanager
    def cursor(self):
        if self.connection is None:
            self.connect()
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Database operation failed: {e}")
            raise
        finally:
            cursor.close()

    def close(self):
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed.")
