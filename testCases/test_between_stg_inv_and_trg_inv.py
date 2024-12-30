import pytest
from Utilities.custom_logger import LogGen
from Utilities.source_target_database import DatabaseChecks

class TestDataExtraction:
    database=DatabaseChecks()
    log_gen = LogGen()
    logger = log_gen.logger()

    def test_required_tables(self, connect_mysqldb_engine):
        self.logger.info("Test case started")
        try:
            tables = ['staging_inventory','aggregated_inventory_levels']
            self.logger.info(f"Validating the following tables present in database: {tables}")
            self.database.check_expected_tables_available_in_database(tables, connect_mysqldb_engine)
            self.logger.info("All the required tables are present in database")
            self.logger.info("Test case passed")
        except Exception as e:
            self.logger.error(f"Error details: {e}")
            pytest.fail(f"Test case failed details: {e}")

    def test_data_present_in_tables(self, connect_mysqldb_engine):
        self.logger.info("TC_02-Verify all the tables have data present inside it")
        try:
            tables = ['staging_inventory','aggregated_inventory_levels']
            self.logger.info(f"Validating the data present inside following tables: {tables}")
            self.database.check_data_available_in_expected_tables(tables, connect_mysqldb_engine)
            self.logger.info("All the tables have data present inside it")
            self.logger.info('Test case passed')
        except Exception as e:
            self.logger.error(f"Error Details : {e}")
            pytest.fail("fTest case failed : {e}")


