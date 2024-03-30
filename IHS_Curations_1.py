import snowflake.connector

class ClassIHSCuraitons:
    def __init__(self, table_info):
        self.table_info = table_info
    
    def read_table_from_snowflake(self, table_name):
        # Snowflake connection parameters
        conn_params = {
            "account": "<your_account>",
            "user": "<your_user>",
            "password": "<your_password>",
            "warehouse": "<your_warehouse>",
            "database": "<your_database>",
            "schema": "<your_schema>",
        }

        # Establish connection
        conn = snowflake.connector.connect(**conn_params)

        # Execute query to fetch data from Snowflake
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")

        # Fetch column names from cursor description
        columns = [col[0] for col in cursor.description]

        # Fetch all rows from the query result
        data = cursor.fetchall()

        # Close cursor and connection
        cursor.close()
        conn.close()

        # Create DataFrame from the fetched data and column names
        df = spark.createDataFrame(data, columns)

        # Select only the required columns based on the configuration
        selected_columns = list(self.table_info[table_name]['columns_mapping'].values())
        df = df.select(selected_columns)

        return df

    # Other methods remain the same...
