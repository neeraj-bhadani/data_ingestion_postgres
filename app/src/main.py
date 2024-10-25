import time

from queries import FraudDetection
from config import Config
from database import Database
from ingestion import DataIngestion
from utils.logging_utils import set_custom_logger


def main():
    time.sleep(20)  # can add retry mechanism for postgres instance
    logger = set_custom_logger()
    db = Database()
    db.connect()

    # Initialize DataIngestion with the database instance and CSV file path
    ingestion = DataIngestion(db, Config.CSV_FILE_PATH)
    ingestion.run()

    queries = FraudDetection(db)
    logger.info("Starting queries.")

    # 1. Users Transacting from Multiple Locations or far locations
    users_multiple_locs = queries.users_multiple_locations()
    logger.info(f"Users with multiple locations: {len(users_multiple_locs)}")
    for user in users_multiple_locs[:10]:
        logger.info(f"Email: {user['email']}, Max Distance (meters): {user['max_distance_meters']}")

    # 2. Failed Transactions by Location with minimum 2 failures
    failed_txn = queries.failed_transactions_by_location(threshold=2)
    logger.info(f"Failed transactions exceeding threshold: {len(failed_txn)}")
    for record in failed_txn[:10]:
        logger.info(f"Grid Latitude: {record['grid_lat']}, Grid Longitude: {record['grid_lon']}, Failed Transactions: {record['failed_transaction_count']}")

    # 3. Top 50 Agents in the Past Year (Output: 0 for past week)
    top_agents = queries.top_agents_past_year(limit=50)
    logger.info(f"Top {len(top_agents)} agents by transaction amount in the past year:")
    for agent in top_agents:
        logger.info(f"Agent Name: {agent['agent_name']}, Total Amount: {agent['total_transaction_amount']}")

    logger.info("Queries completed.")

    # Close database connection
    db.close()


if __name__ == "__main__":
    main()
