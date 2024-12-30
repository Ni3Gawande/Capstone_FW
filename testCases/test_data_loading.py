import pytest
from Utilities.custom_logger import LogGen
from Utilities.source_target_database import DatabaseChecks

class TestDataExtraction:
    database=DatabaseChecks()
    log_gen = LogGen()
    logger = log_gen.logger()

    def test_filter_transformation(self, connect_mysqldb_engine):
        self.logger.info(
            "Starting data transformation between staging_sales (filtered by sale_date <= 2024-09-20) and filtered_sales tables.")

        try:
            self.logger.info("Fetching the data from staging_sales")
            query_expected = """select * from staging_sales where sale_date <= '2024-09-20' """

            self.logger.info("Fetching  data from the filtered_sales")
            query_actual = """select * from filtered_sales"""

            self.logger.info("Comparing the data between staging_sales and filtered_sales")
            self.database.check_data_validation(query_expected, connect_mysqldb_engine, query_actual,
                                                connect_mysqldb_engine, 'transformation_staging_sales.csv')

            self.logger.info(
                "Data transformation completed on staging_sales (filtered by sale_date <= 2024-09-20) and compared with filtered_sales table.")
        except Exception as e:
            self.logger.error(
                f"Error occurred during data transformation between staging_sales and filtered_sales tables: {e}")
            pytest.fail(f"Test failed due to {e}")

    def test_router_high_region_transformation(self, connect_mysqldb_engine):
        self.logger.info("Starting data transformation between filtered_sale(filtered by region =High) and high_sales")
        try:
            self.logger.info("Fetching  data from the filtered_sales")
            query_expected = """select * from filtered_sales where region ='High' """

            self.logger.info("Fetching  data from the high_sales")
            query_actual = "select * from high_sales"

            self.logger.info("Comparing the data between filtered_sales and high_sales")
            self.database.check_data_validation(query_expected, connect_mysqldb_engine, query_actual,
                                                connect_mysqldb_engine, 'transformation_high_sales.csv')

            self.logger.info("Data Tranformation completed between filtered_sales and high_sales")
        except Exception as e:
            self.logger.error(f"Error occured during transformation {e}")
            pytest.fail(f"Test failed due to {e}")

    def test_router_low_region_transformation(self, connect_mysqldb_engine):
        self.logger.info("Starting data transformation between filtered_sale(filtered by region = Low) and high_sales")
        try:
            self.logger.info("Fetching data from the filtered_sales")
            query_expected = """select * from filtered_sales where region ='Low' """

            self.logger.info("Fetching data from the low_sales")
            query_actual = "select * from low_sales"

            self.logger.info("Comparing the data between filtered_sales and low_sales")
            self.database.check_data_validation(query_expected, connect_mysqldb_engine, query_actual,
                                                connect_mysqldb_engine, 'transformation_low_sales.csv')

            self.logger.info("Data Tranformation completed between filtered_sales and low_sales ")
        except Exception as e:
            self.logger.error(f"Error occured during transformation {e}")
            pytest.fail(f"Test failed due to {e}")

    def test_Aggregate_Sales_data_transformation(self, connect_mysqldb_engine):
        self.logger.info(
            "test_Aggregate_Sales_data_transformation test has started between filtered_sales and monthly_sales_summary_source ")
        try:
            self.logger.info("Feteching data from the filtered_Sales")
            query_expected = """select product_id,month(sale_date) as month,year(sale_date) as year ,sum(quantity*price) as total_sales from filtered_sales group by product_id,month(sale_date),year(sale_date)"""

            self.logger.info("Fetehing data from the monthly_sales_summary_source")
            query_actual = """select * from monthly_sales_summary_source"""

            self.logger.info("Comparing tha data between filtered_sales and monthly_sales_summary_source")
            self.database.check_data_validation(query_expected, connect_mysqldb_engine, query_actual,
                                                connect_mysqldb_engine, 'transformation_aggregate_sales.csv')
            self.logger.info(
                "test_Aggregate_Sales_data_transformation test has completed between filtered_sales and monthly_sales_summary_source ")
        except Exception as e:
            self.logger.error(f"Error occured during data transformation: {e}")
            pytest.fail(f"Test failed due to an error {e}")

    def test_JOINER_transformation(self, connect_mysqldb_engine):
        self.logger.info(
            "Started DataValidation:joining filtered_sales,staging_product,staging_stores and validate data with sales_with_details ")
        try:
            self.logger.info(
                "Fetching data from the filtered_sale,satging_product_staging_stores and joining them together on condition")
            query_expected = """ select s.sales_id,s.product_id,s.store_id,p.product_name,st.store_name,s.quantity,s.price*s.quantity as total_amount,s.sale_date from filtered_sales as s join staging_product as p on s.product_id = p.product_id join staging_stores as st on s.store_id = st.store_id; """

            self.logger.info("Fetching data from the sales_with_details")
            query_actual = """select * from sales_with_deatils"""

            self.logger.info(
                "Comparing the data after joining filtered_sales,staging_product,staging_stores with sales_with details")
            self.database.check_data_validation(query_expected, connect_mysqldb_engine, query_actual,
                                                connect_mysqldb_engine, 'transformation_sales_details.csv')

            self.logger.info(
                "Completed DataValidation:joining filtered_sales,staging_product,staging_stores and validate data with sales_with_details ")
        except Exception as e:
            self.logger.error(f"Error occured during data transformation: {e}")
            pytest.fail(f"Test failed due to an error {e}")

    def test_aggregate_inventory_levels_transformation(self, connect_mysqldb_engine):
        self.logger.info("Started validating data between staging_inventory and aggregate_inventory_levels")
        try:
            self.logger.info("Fetching data from the staging_inventory")
            query_expected = """select store_id,sum(quantity_on_hand) as total_inventory from staging_inventory group by store_id;"""

            self.logger.info("Fetching the data from aggregated_inventory_levels")
            query_actual = """select * from aggregated_inventory_levels"""

            self.logger.info("Comparing the data between staging_inventory and aggregate_inventory_levels")
            self.database.check_data_validation(query_expected, connect_mysqldb_engine, query_actual,
                                                connect_mysqldb_engine, 'transformation_inventory_sales.csv')

            self.logger.info("Completed validating data between staging_inventory and aggregate_inventory_levels")
        except Exception as e:
            self.logger.error(f"Error occured during data transformation: {e}")
            pytest.fail(f"Test failed due to an error {e}")