import pytest
from Utilities.custom_logger import LogGen
from Utilities.source_file_target_database import FileToDatabaseValidation
from Utilities.source_target_database import DatabaseChecks

class TestDataExtraction:
    file_database=FileToDatabaseValidation()
    database=DatabaseChecks()
    log_gen = LogGen()
    logger = log_gen.logger()

    def test_extraction_from_sales_data_CSV_to_sales_staging_MySQL(self,connect_mysqldb_engine):
        self.logger.info("Data extraction from sales_data.csv to staging_sales has started")
        try:
            self.file_database.file_to_db_verify('Testdata/sales_data.csv', 'csv', 'staging_sales', connect_mysqldb_engine, 'defect_sales.csv')
            self.logger.info("Data extraction from sales_data.csv to sales_staging has completed")
        except Exception as e:
            self.logger.error(f"Error occurred during data extraction: {e}")
            pytest.fail(f"Test failed due to an error: {e}")

    def test_extraction_from_product_data_csv_to_product_staging_MySQL(self,connect_mysqldb_engine):
        self.logger.info('Data extraction from product_data.csv to staging_product has started')
        try:
            self.file_database.file_to_db_verify('Testdata/product_data.csv', 'csv', 'staging_product', connect_mysqldb_engine, 'defect_product.csv')
            self.logger.info("Data extraction from product_data.csv to staging_product has completed")
        except Exception as e:
            self.logger.error(f"Error occured during data extraction {e}")
            pytest.fail(f"Test failed due to {e}")

    def test_extraction_from_supplier_data_json_to_supplier_staging_MySQL(self,connect_mysqldb_engine):
        self.logger.info('Data extraction from supplier_data.json to staging_supplier has started')
        try:
            self.file_database.file_to_db_verify('Testdata/supplier_data.json', 'json', 'staging_supplier', connect_mysqldb_engine,
                              'defect_supplier')
            self.logger.info('Data extraction from supplier_data.json to staging_supplier has completed')
        except Exception as e:
            self.logger.error(f"Error occured during data extraction {e}")
            pytest.fail(f"Test failed due to {e}")

    def test_extraction_from_inventory_data_xml_to_inventory_staging_MySQL(self,connect_mysqldb_engine):
        self.logger.info('Data extraction from inventory_data to staging_inventory has started')
        try:
            self.file_database.file_to_db_verify('Testdata/inventory_data.xml', 'xml', 'staging_inventory', connect_mysqldb_engine,
                              'defect_inventory.csv')
            self.logger.info('Data extraction from inventory_data to staging_inventory has completed')
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test failed due to {e}")

    def test_extraction_from_stores_oracle_to_stores_staging_MySQL(self,connect_mysqldb_engine,connect_sqlserverdb_engine):
        self.logger.info(
            "Data validation started between stores table from sqlserver database to staging_stores table from mysql database")
        try:
            self.logger.info('query_expected:Running SQL query to read the data from stores table from sqlserver database')
            queue_expected = """Select * from stores"""
            self.logger.info('query_actual:Running SQL query to read the data from staging_stores table from mysql database')
            query_actual = """select * from staging_stores"""
            self.database.check_data_validation(queue_expected, connect_sqlserverdb_engine, query_actual, connect_mysqldb_engine, 'defect_store.csv')
            self.logger.info(
                "Data validation completed between stores table of sqlserver database to staging_stores table of mysql database")
        except Exception as e:
            self.logger.error(f"Error occur during data extraction {e}")
            pytest.fail(f"Test failed due to {e}")