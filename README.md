### Introduction

This documentation provides a detailed guide to set up, run, and understand the data ingestion process for the project. The system is designed to validate and ingest transaction data from a CSV file into a PostgreSQL database using Docker for easy environment setup. The project also includes built-in logging and error handling to ensure smooth operation and traceability.

---

## **1. Setting Up the Project**

### Step 1: Clone the Repository and Checkout the Correct Branch

Begin by cloning the project repository and navigating to the relevant branch.

```bash
git clone git@github.com:analytics1211/de-assigment-bhadaneeraj.git
cd de-assigment-bhadaneeraj
git checkout PaynearbyDeAssignment_neeraj_bhadani
```

### Step 2: Add the CSV File to the Data Folder

Ensure that the `transactions.csv` file is placed into the `data` folder located in the project root directory.

```bash
# Place the CSV file at the following location
de-assigment-bhadaneeraj/data/transactions.csv
```

### Step 3: Set Environment Variables in `.env` File

In the root directory of the project, create a `.env` file and define the required environment variables:

```makefile
DATABASE_HOST=db
DATABASE_USER=postgres
DATABASE_PASSWORD=postgres
DATABASE_NAME=transactions_db
DATABASE_PORT=5432
CSV_FILE_PATH='/app/data/transactions.csv'
```

These environment variables will be used by the application and Docker to configure the database and set the path to the CSV file.

### Step 4: Run Docker Compose

Now that everything is set up, use Docker to run the project. The `docker-compose.yml` file will handle the setup of PostgreSQL and the application container.

```bash
docker-compose up --build
```

This command will build the necessary Docker images and run the services in containers. The application will begin ingesting the data from the CSV file into the PostgreSQL database.

### Step 5: Verify the Data in PostgreSQL

Once the application has finished running, you can check the data in the PostgreSQL database to verify that the ingestion process was successful.

1. **Access the PostgreSQL container**:
    
    ```bash
    docker exec -it de-assigment-bhadaneeraj-db-1 bash
    ```
    
2. **Log into the PostgreSQL database**:
    
    ```bash
    psql -U postgres -d transactions_db
    ```
    
3. **Run queries to verify the data**:
    
    ```sql
    SELECT COUNT(*) FROM transactions;
    
    SELECT COUNT(DISTINCT transaction_id) FROM transactions;
    ```
    

---

## **2. Data Ingestion Process**

The ingestion process is orchestrated by the `DataIngestion` class, which follows a well-structured procedure to ensure data integrity and proper logging. Below is a detailed explanation of the ingestion workflow.

```python
def run(self):
    """
    Orchestrates the entire data ingestion process.
    """
    try:
        self.logger.info("Starting data ingestion process.")

        # Step 1: Load data into the staging table
        self.load_data_to_staging()

        # Step 2: Ensure the main transactions table exists
        self.create_transactions_table()

        # Step 3: Validate and transform data, insert into main table
        self.validate_and_transform()

        # Step 4: (Optional) Move invalid records to the error table, skipped
        # self.move_invalid_records_to_error_table()

        # Step 5: Create indexes after bulk data insertion
        self.create_indexes()

        # Step 6: Clean up the staging table
        self.clean_up_staging()

        self.logger.info("Data ingestion process completed successfully.")

    except Exception as e:
        self.logger.error(f"Data ingestion process failed: {e}")
        raise

```

### **Detailed Steps of the Ingestion Process**

1. **`load_data_to_staging()`**:
    - This method reads the CSV file (based on the `CSV_FILE_PATH` set in the environment) and loads the data into a temporary staging table in the database. The staging table serves as a buffer to handle large datasets efficiently.
2. **`create_transactions_table()`**:
    - This method ensures that the main `transactions` table exists in the database. If it doesn’t exist, it will be created. The main table will store validated and clean transaction data.
3. **`validate_and_transform()`**:
    - **Purpose**: Validates and transforms data from the staging table and inserts valid records into the main table.
    - **Process**:
        - **Standardize and Validate Phone Numbers**:
            - Applies rules to standardize phone numbers to a consistent 10-digit format.
            - Handles various formats, including numbers starting with country code `+91`, `91`, or `0`.
            - Uses SQL `CASE` statements and regular expressions to clean and extract valid phone numbers.
        - **Standardize Emails**:
            - Converts emails to lowercase and trims whitespace.
        - **Insert Valid Records into Main Table**:
            - Uses an SQL `INSERT INTO ... SELECT` statement to insert data from the staging table into the main `transactions` table.
            - **Data Transformation**:
                - Parses `created_at` and `updated_at` timestamps using `TO_TIMESTAMP`.
                - Applies the standardized phone number and email transformations.
            - **Data Validation in WHERE Clause**:
                - Validates phone numbers to ensure they are exactly 10 digits starting with digits `6-9`.
                - Validates email addresses using a regular expression to match standard email formats.
            - **Handling Duplicate Transaction IDs**:
                - Uses `ON CONFLICT (transaction_id) DO NOTHING` to avoid inserting duplicate records based on `transaction_id`.
        - 
4. **(Optional) `move_invalid_records_to_error_table()`**:
    - Although this step is currently skipped, it is designed to move invalid records (those that failed validation) into a separate error table for further analysis or manual correction.
5. **`create_indexes()`**:
    - After inserting the bulk data, indexes are created on key columns (e.g., `transaction_id`, `created_at`, `agent_name`). This improves query performance, especially when dealing with large datasets.
6. **`clean_up_staging()`**:
    - Once the data has been successfully validated and inserted into the main table, the staging table is cleaned up to free up space and ensure that no unnecessary data remains.

---

## **3. Error Handling**

The ingestion process is designed with comprehensive error handling:

- If any step in the process fails, an error is logged, and the process is terminated.
- The system logs both high-level messages (e.g., "Data ingestion process completed successfully") and detailed errors (e.g., a specific failure during validation or data loading).
- All logs are stored in the file `app/logs/utils_logging_utils_log.txt`.

---

---

## **4. Future Improvements**

- **Error Table Implementation**: Future versions of this system can implement the `move_invalid_records_to_error_table()` method to store and handle invalid records, enabling the user to analyze or correct them later.
- **Spatial Indexing**: Utilize spatial indexes in PostgreSQL (e.g., PostGIS extension) to optimize queries involving geolocation data, improving the performance of location-based queries.
- **Unit and Integration Tests**: Expand the test suite with comprehensive unit and integration tests to ensure the reliability and correctness of the ingestion process.
- **Avoid Running DDL Statements in Application Code**
- Fetch latest transaction for each transaction_id with below when dropping duplicates

```sql
MAX(updated_at)
```

---

## **5. Conclusion**

This project automates the ingestion of transaction data from CSV files into a PostgreSQL database, with a focus on validation, logging, and efficiency. The detailed steps provided in this documentation should help you set up, run, and understand the workings of the system.

For any further inquiries or issues, please feel free to connect with me on [[bhadaneeraj@gmail.com](mailto:bhadaneeraj@gmail.com)](mailto:bhadaneeraj@gmail.com) or 9678911531
