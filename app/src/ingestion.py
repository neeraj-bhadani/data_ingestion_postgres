from psycopg2 import sql
from database import Database


class DataIngestion:
    def __init__(self, db: Database, csv_file_path: str):
        """
        Initializes the DataIngestion instance.

        Args:
            db (Database): The Database instance for DB operations.
            csv_file_path (str): Path to the CSV file to ingest.
        """
        self.db = db
        self.csv_file_path = csv_file_path
        self.logger = self.db.logger

    def run(self):
        """
        Orchestrates the entire data ingestion process.
        """
        try:
            self.logger.info("Starting data ingestion process.")

            # Load data into the staging table
            self.load_data_to_staging()

            # Ensure the main transactions table exists
            self.create_transactions_table()

            # Validate and transform data, insert into main table
            self.validate_and_transform()

            # Move invalid records to the error table (Skipped for now)
            # self.move_invalid_records_to_error_table()

            # Create indexes after bulk data insertion
            self.create_indexes()

            # Clean up the staging table
            self.clean_up_staging()

            self.logger.info("Data ingestion process completed successfully.")

        except Exception as e:
            self.logger.error(f"Data ingestion process failed: {e}")
            raise

    def load_data_to_staging(self):
        """
        Loads raw CSV data into the staging table using PostgreSQL's COPY command.
        """
        try:
            with self.db.cursor() as cursor:
                # Create staging table
                create_staging_table_query = """
                CREATE TEMP TABLE staging_transactions (
                    transaction_id VARCHAR(50),
                    agent_name VARCHAR(100),
                    amount NUMERIC(12,2),
                    status VARCHAR(10),
                    created_at VARCHAR(30),
                    updated_at VARCHAR(30),
                    lat DECIMAL(9,6),
                    lon DECIMAL(9,6),
                    email VARCHAR(100),
                    phone_number VARCHAR(20)
                );
                """
                cursor.execute(create_staging_table_query)
                self.logger.info("Staging table created.")

                # Execute COPY command
                with open(self.csv_file_path, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(
                        sql.SQL("COPY staging_transactions FROM STDIN WITH (FORMAT csv, HEADER TRUE)"),
                        f
                    )
                self.logger.info("Data loaded into staging table using COPY.")
        except Exception as e:
            self.logger.error(f"Error loading data to staging table: {e}")
            raise

    def create_transactions_table(self):
        """
        Creates the main transactions table if it does not exist.
        """
        try:
            with self.db.cursor() as cursor:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR(50) PRIMARY KEY,
                    agent_name VARCHAR(100),
                    amount NUMERIC(12,2),
                    status VARCHAR(10),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    lat DECIMAL(9,6),
                    lon DECIMAL(9,6),
                    email VARCHAR(100),
                    phone_number VARCHAR(20)
                );
                """
                cursor.execute(create_table_query)
                self.logger.info("Main transactions table created or already exists.")
        except Exception as e:
            self.logger.error(f"Error creating transactions table: {e}")
            raise

    def validate_and_transform(self):
        """
        Validates and transforms data from the staging table and inserts valid records into the main table.
        """
        try:
            with self.db.cursor() as cursor:
                # Standardize and validate phone numbers
                standardized_phone = """
                    CASE 
                        WHEN phone_number ~ '^\\+?91' THEN 
                            CASE 
                                WHEN LENGTH(regexp_replace(phone_number, '\\D', '', 'g')) >= 12 THEN SUBSTRING(regexp_replace(phone_number, '\\D', '', 'g') FROM 3 FOR 10)
                                ELSE NULL
                            END
                        WHEN phone_number ~ '^0' THEN 
                            CASE 
                                WHEN LENGTH(regexp_replace(phone_number, '\\D', '', 'g')) >= 11 THEN SUBSTRING(regexp_replace(phone_number, '\\D', '', 'g') FROM 2 FOR 10)
                                ELSE NULL
                            END
                        ELSE 
                            CASE 
                                WHEN LENGTH(regexp_replace(phone_number, '\\D', '', 'g')) = 10 THEN regexp_replace(phone_number, '\\D', '', 'g')
                                ELSE NULL
                            END
                    END
                """

                # Standardize emails to lowercase and trim
                standardized_email = "LOWER(TRIM(email))"

                # Insert valid records into main table
                insert_query = sql.SQL("""
                    INSERT INTO transactions (
                        transaction_id, agent_name, amount, status,
                        created_at, updated_at, lat, lon, email, phone_number
                    )
                    SELECT
                        transaction_id,
                        agent_name,
                        amount,
                        status,
                        TO_TIMESTAMP(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US'),
                        TO_TIMESTAMP(updated_at, 'YYYY-MM-DD"T"HH24:MI:SS.US'),
                        lat,
                        lon,
                        {standardized_email},
                        {standardized_phone}
                    FROM
                        staging_transactions
                    WHERE
                        -- Validate phone number: exactly 10 digits starting with 6-9
                        {standardized_phone} ~ '^[6-9]\\d{{9}}$'
                        AND
                        -- Validate email format
                        {standardized_email} ~ '^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$'
                    -- Handling duplicate transaction_id
                    ON CONFLICT (transaction_id) DO NOTHING;
                """).format(
                    standardized_email=sql.SQL(standardized_email),
                    standardized_phone=sql.SQL(standardized_phone)
                )

                cursor.execute(insert_query)
                inserted = cursor.rowcount
                self.logger.info(f"Inserted {inserted} valid records into main table.")
        except Exception as e:
            self.logger.error(f"Error during validation and transformation: {e}")
            raise

    # can also use something like an left anti join later
    # def move_invalid_records_to_error_table(self):
    #     """
    #     Moves invalid records from the staging table to an error table for further analysis.
    #     """
    #     try:
    #         with self.db.cursor() as cursor:
    #             # Create error table if it doesn't exist
    #             create_error_table_query = """
    #             CREATE TABLE IF NOT EXISTS error_transactions (
    #                 transaction_id VARCHAR(50),
    #                 agent_name VARCHAR(100),
    #                 amount NUMERIC(12,2),
    #                 status VARCHAR(10),
    #                 created_at VARCHAR(30),
    #                 updated_at VARCHAR(30),
    #                 lat DECIMAL(9,6),
    #                 lon DECIMAL(9,6),
    #                 email VARCHAR(100),
    #                 phone_number VARCHAR(20)
    #             );
    #             """
    #             cursor.execute(create_error_table_query)
    #             self.logger.info("Error table created or already exists.")
    #
    #             # Insert invalid records into error table
    #             insert_error_query = """
    #                 INSERT INTO error_transactions
    #                 SELECT *
    #                 FROM staging_transactions
    #                 WHERE
    #                     -- Phone number is NULL after standardization
    #                     (
    #                         CASE
    #                             WHEN phone_number ~ '^\\+?91' THEN
    #                                 CASE
    #                                     WHEN LENGTH(regexp_replace(phone_number, '\\D', '', 'g')) >= 12 THEN SUBSTRING(regexp_replace(phone_number, '\\D', '', 'g') FROM 3 FOR 10)
    #                                     ELSE NULL
    #                                 END
    #                             WHEN phone_number ~ '^0' THEN
    #                                 CASE
    #                                     WHEN LENGTH(regexp_replace(phone_number, '\\D', '', 'g')) >= 11 THEN SUBSTRING(regexp_replace(phone_number, '\\D', '', 'g') FROM 2 FOR 10)
    #                                     ELSE NULL
    #                                 END
    #                             ELSE
    #                                 CASE
    #                                     WHEN LENGTH(regexp_replace(phone_number, '\\D', '', 'g')) = 10 THEN regexp_replace(phone_number, '\\D', '', 'g')
    #                                     ELSE NULL
    #                                 END
    #                         END
    #                     ) IS NULL
    #                     OR
    #                     -- Email format is invalid
    #                     LOWER(TRIM(email)) !~ '^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$'
    #             """
    #             cursor.execute(insert_error_query)
    #             moved = cursor.rowcount
    #             self.logger.info(f"Moved {moved} invalid records to error table.")
    #     except Exception as e:
    #         self.logger.error(f"Error moving invalid records to error table: {e}")
    #         raise

    def create_indexes(self):
        """
        Creates indexes on the main table after bulk data insertion for efficient querying.
        """
        try:
            #may create spatial index on location, skipped for now
            with self.db.cursor() as cursor:
                cursor.execute("""
                    CREATE INDEX idx_transactions_created_at ON transactions(created_at);
                    CREATE INDEX idx_transactions_status ON transactions(status);
                    CREATE INDEX idx_transactions_agent_name ON transactions(agent_name);
                    CREATE INDEX idx_transactions_lat_lon ON transactions(lat, lon);
                    CREATE INDEX idx_transactions_status_created_at ON transactions (status, created_at);
                    CREATE INDEX idx_transactions_email ON transactions (email);
                """)
                self.logger.info("Indexes created.")
        except Exception as e:
            self.logger.error(f"Error recreating indexes: {e}")
            raise

    def clean_up_staging(self):
        """
        Cleans up the staging table by truncating it.
        """
        try:
            with self.db.cursor() as cursor:
                cursor.execute("TRUNCATE staging_transactions;")
                self.logger.info("Staging table truncated.")
        except Exception as e:
            self.logger.error(f"Error cleaning up staging table: {e}")
            raise
