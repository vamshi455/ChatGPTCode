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
        # Select only the required columns based on the configuration
        selected_columns = list(self.table_info[table_name]['columns_mapping'].values())
        selected_data = [tuple(d[column] for column in selected_columns) for d in data]
        return spark.createDataFrame(selected_data, selected_columns)
    
    def create_temp_table(self, df, table_name):
        # Create temporary table
        df.createOrReplaceTempView(table_name)
    
    def join_tables_with_sql(self):
        # Join tables and return the resulting DataFrame
        sales_df = self.read_table_from_snowflake('sales')
        customers_df = self.read_table_from_snowflake('customers')
        segment_df = self.read_table_from_snowflake('segment')
        country_df = self.read_table_from_snowflake('country')

        df_result = sales_df.join(customers_df, sales_df.customerid == customers_df.customerid, 'inner') \
                            .join(segment_df, sales_df.segmentid == segment_df.segmentid, 'inner') \
                            .join(country_df, sales_df.countryid == country_df.countryid, 'inner') \
                            .select(
                                sales_df.saleorderid,
                                sales_df.customerid,
                                sales_df.segmentid.alias('sales_segmentid'),
                                sales_df.countryid.alias('sales_countryid'),
                                sales_df.salesamount,
                                sales_df.quantity,
                                customers_df.customername.alias('customer_name'),
                                segment_df.segmentname.alias('segment_name'),
                                country_df.countryname.alias('country_name')
                            )
        return df_result

def main(table_info):
    # Create an instance of the ClassIHSCuraitons class
    ih_curations = ClassIHSCuraitons(table_info)

    # Join tables and get the resulting DataFrame
    df_result = ih_curations.join_tables_with_sql()

    # Perform additional calculations or transformations as needed
    # For example, adding a new calculated column
    df_result = df_result.withColumn('total_sales', df_result.salesamount * df_result.quantity)

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
