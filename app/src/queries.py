import logging
from database import Database

class FraudDetection:
    def __init__(self, db: Database):
        self.logger = logging.getLogger(__name__)
        self.db = db

    def users_multiple_locations(self):
        """
        Identify users who have transactions from multiple unknown or far locations considering 5km as threshold
        distance.
        """
        query = """
        CREATE EXTENSION IF NOT EXISTS postgis;
        WITH user_transactions AS (
            SELECT
                email,
                created_at,
                ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS location
            FROM
                transactions
        )
        SELECT
            ut1.email,
            MAX(ST_Distance(ut1.location, ut2.location)) AS max_distance_meters
        FROM
            user_transactions ut1
        JOIN
            user_transactions ut2
            ON ut1.email = ut2.email
            AND ut1.created_at < ut2.created_at
        GROUP BY
            ut1.email
        HAVING
            MAX(ST_Distance(ut1.location, ut2.location)) > 5000  -- Threshold in meters (5 km)
        ORDER BY
            max_distance_meters DESC;
        """
        try:
            with self.db.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                self.logger.info("Retrieved users transacting from multiple unknown or far locations.")
                return results
        except Exception as e:
            self.logger.error(f"Error executing users_multiple_locations: {e}")
            raise

    def failed_transactions_by_location(self, threshold=2):
        """
        Detect transactions failing from specific locations or areas using grid cells.
        """
        query = """
        -- Ensure PostGIS extension is enabled
        CREATE EXTENSION IF NOT EXISTS postgis;

        WITH failed_transactions AS (
            SELECT
                ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS location
            FROM
                transactions
            WHERE
                status = 'Failed'
        )
        SELECT
            ST_Y(ST_Transform(grid_cell, 4326)) AS grid_lat,
            ST_X(ST_Transform(grid_cell, 4326)) AS grid_lon,
            COUNT(*) AS failed_transaction_count
        FROM (
            SELECT
                ST_SnapToGrid(location::geometry, 1.5) AS grid_cell
            FROM
                failed_transactions
        ) sub
        GROUP BY
            grid_cell
        HAVING
            COUNT(*) > %s
        ORDER BY
            failed_transaction_count DESC;
        """
        try:
            with self.db.cursor() as cursor:
                cursor.execute(query, (threshold,))
                results = cursor.fetchall()
                self.logger.info(f"Retrieved locations with failed transactions exceeding threshold of {threshold}.")
                return results
        except Exception as e:
            self.logger.error(f"Error executing failed_transactions_by_location: {e}")
            raise

    def top_agents_past_year(self, limit=50):
        """
        List the top agents based on their transaction amounts within the past year.
        """
        query = """
        SELECT
            agent_name,
            SUM(amount) AS total_transaction_amount
        FROM
            transactions
        WHERE
            status = 'Success'
            AND created_at >= NOW() - INTERVAL '365 days' -- No results for past week
        GROUP BY
            agent_name
        ORDER BY
            total_transaction_amount DESC
        LIMIT
            %s;
        """
        try:
            with self.db.cursor() as cursor:
                cursor.execute(query, (limit,))
                results = cursor.fetchall()
                self.logger.info(f"Retrieved top {limit} agents based on transaction amounts in the past week.")
                return results
        except Exception as e:
            self.logger.error(f"Error executing top_agents_past_week: {e}")
            raise
