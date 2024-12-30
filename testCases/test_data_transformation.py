import pytest
from Utilities.custom_logger import LogGen
from Utilities.source_file_target_database import FileToDatabaseValidation
from Utilities.source_target_database import DatabaseChecks

class TestDataExtraction:
    file_database=FileToDatabaseValidation()
    database=DatabaseChecks()
    log_gen = LogGen()
    logger = log_gen.logger()

    def test_fact_sales_table_load(self,connect_mysqldb_engine):
        self.logger.info("test_fact_sales_table_load test has started .......")
        try:
            query_expected = """select sales_id,product_id,store_id,quantity,total_amount as total_sales ,sale_date from sales_with_deatils """
            query_actual = """select sales_id,product_id,store_id,quantity,total_sales,sale_date from fact_sales """
            self.database.check_data_validation(query_expected,connect_mysqldb_engine, query_actual,connect_mysqldb_engine,
                            'fact_Sales_differences.csv')
            self.logger.info("test_fact_sales_table_load test has completed .......")
        except Exception as e:
            self.logger.error(f"Error occured during data transformation: {e}")
            pytest.fail(f"Test failed due to an error {e}")

    @pytest.mark.smoke
    @pytest.mark.regression
    def test_monthly_sales_summary_table_load(self,connect_mysqldb_engine):
        self.logger.info("test_monthly_sales_summary_table_load test has started .......")
        try:
            query_expected = """select * from  monthly_sales_summary_source order by product_id,month,year;"""
            query_actual = """select * from  monthly_sales_summary order by product_id,month,year;"""
            self.database.check_data_validation(query_expected, connect_mysqldb_engine, query_actual, connect_mysqldb_engine,
                            'Monthly_Sales_Differances.csv')
            self.logger.info("test_monthly_sales_summary_table_load test has completed .......")
        except Exception as e:
            self.logger.error(f"Error occured during data transformation: {e}")
            pytest.fail(f"Test failed due to an error {e}")

    @pytest.mark.smoke
    @pytest.mark.regression
    def test_inventory_fact_load(self,connect_mysqldb_engine):
        self.logger.info("test_inventory_fact_load test has started .......")
        try:
            query_expected = """select * from  staging_inventory """
            query_actual = """select * from  fact_inventory """
            self.database.check_data_validation(query_expected, connect_mysqldb_engine, query_actual, connect_mysqldb_engine,
                            'inventory_Fact_differences.csv')
            self.logger.info("test_inventory_fact_load test has completed .......")
        except Exception as e:
            self.logger.error(f"Error occured during data transformation: {e}")
            pytest.fail(f"Test failed due to an error {e}")

    @pytest.mark.smoke
    @pytest.mark.regression
    def test__inventory_levels_by_store_fact_load(self,connect_mysqldb_engine):
        self.logger.info("test__inventory_levels_by_store_fact_load test has started .......")
        try:
            query_expected = """select * from  aggregated_inventory_levels order by store_id;"""
            query_actual = """select store_id,cast(total_inventory as Double) as total_inventory from inventory_levels_by_store;"""
            self.database.check_data_validation(query_expected, connect_mysqldb_engine, query_actual,connect_mysqldb_engine,
                            'Inventory_Levels_By_Store_differences.csv')
            self.logger.info("test__inventory_levels_by_store_fact_load test has completed .......")
        except Exception as e:
            self.logger.error(f"Error occured during data transformation: {e}")
            pytest.fail(f"Test failed due to an error {e}")