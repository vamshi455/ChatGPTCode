class ClassIHSCuraitons:
    def __init__(self, table_info):
        self.table_info = table_info
    
    def read_table_from_snowflake(self, table_name):
        # Dummy test data for demonstration
        if table_name == 'sales':
            data = [(1, 101, 201, 301, 1000, 5),
                    (2, 102, 202, 302, 1500, 8)]
            columns = ['saleorderid', 'customerid', 'segmentid', 'countryid', 'salesamount', 'quantity']
        elif table_name == 'customers':
            data = [(101, 'Customer A'), (102, 'Customer B')]
            columns = ['customerid', 'customername']
        elif table_name == 'segment':
            data = [(201, 'Segment X'), (202, 'Segment Y')]
            columns = ['segmentid', 'segmentname']
        elif table_name == 'country':
            data = [(301, 'Country US')]
            columns = ['countryid', 'countryname']
        else:
            data = []
            columns = []
        return spark.createDataFrame(data, columns)
    
    def create_temp_table(self, df, table_name):
        df.createOrReplaceTempView(table_name)
    
    def join_tables_with_sql(self):
        sql_query = """
            SELECT
                s.saleorderid,
                s.customerid,
                s.segmentid AS sales_segmentid,
                s.countryid AS sales_countryid,
                s.salesamount,
                s.quantity,
                c.customername AS customer_name,
                seg.segmentname AS segment_name,
                co.countryname AS country_name
            FROM sales s
            JOIN customers c ON s.customerid = c.customerid
            JOIN segment seg ON s.segmentid = seg.segmentid
            JOIN country co ON s.countryid = co.countryid
            """
        df_result = spark.sql(sql_query)
        return df_result

def main(table_info):
    # Create an instance of the ClassIHSCuraitons class
    ih_curations = ClassIHSCuraitons(table_info)

    # Read all tables from Snowflake and create temp tables
    for table_name in table_info.keys():
        df = ih_curations.read_table_from_snowflake(table_name)
        ih_curations.create_temp_table(df, table_name)

    # Join tables using SQL
    df_result = ih_curations.join_tables_with_sql()

    # Show resulting dataframe
    df_result.show()

# Define configuration for table information
table_info = {
    'sales': {
        'key_columns': ['saleorderid', 'customerid'],
        'filter_conditions': {'countryid': 'US'},
        'columns_mapping': {
            'saleorderid': 'saleorderid',
            'customerid': 'customerid',
            'segmentid': 'segmentid',
            'countryid': 'countryid',
            'salesamount': 'salesamount',
            'quantity': 'quantity',
            'customername': 'customername',
            'segmentname': 'segmentname',
            'countryname': 'countryname',
            'salesamont': 'salesamont',  # Corrected typo: salesamont -> salesamount
            'salesquantity': 'salesquantity'  # Added salesquantity column
        }
    },
    'customers': {
        'columns_mapping': {'customerid': 'customerid', 'customername': 'customername'},
        'key_column': 'customerid',
        'filter_conditions': {}
    },
    'segment': {
        'columns_mapping': {'segmentid': 'segmentid', 'segmentname': 'segmentname'},
        'key_column': 'segmentid',
        'filter_conditions': {}
    },
    'country': {
        'columns_mapping': {'countryid': 'countryid', 'countryname': 'countryname'},
        'key_column': 'countryid',
        'filter_conditions': {}
    }
    # Add information for other tables similarly
}

# Call the main function to execute the workflow
main(table_info)
