import pandas as pd
from Utilities.custom_logger import LogGen
from Utilities.defect_file_utilities import SaveFile
import os


class FileToDatabaseValidation:
    savefile = SaveFile()
    log_gen = LogGen()
    logger = log_gen.logger()

    def file_to_db_verify(self, file_path, file_type, table_name, db_engine, defect_file_path):
        # Read the source file
        if file_type == 'csv':
            self.logger.info(f"Fetching the data from {file_path}")
            df_expected = pd.read_csv(file_path)
        elif file_type == 'xml':
            self.logger.info(f"Fetching the data from {file_path}")
            df_expected = pd.read_xml(file_path, xpath='.//item')
        elif file_type == 'json':
            self.logger.info(f"Fetching the data from {file_path}")
            df_expected = pd.read_json(file_path)
        else:
            self.logger.error(f"Unsupported file type: {file_type}")
            raise ValueError(f"Unsupported file type: {file_type}")

        self.logger.info(f"Fetching data from the database table: {table_name}")
        query = f"SELECT * FROM {table_name}"
        self.logger.info(f"Comparing the data between {file_path} and  {table_name}")
        df_actual = pd.read_sql(query, db_engine)
        defect_file = self.savefile.save_the_mismatch_to_file(df_expected, df_actual, defect_file_path)
        if defect_file.empty:
            self.logger.info('Data Validation Passed')
        else:
            raise AssertionError('Data validation failed')

    def check_data_validation_for_columns(self, source_df, target_df, defect_file_path):
        self.logger.info("Validation the data between source and target data frame")
        defect_file = self.savefile.save_the_mismatch_to_file(source_df, target_df, defect_file_path)
        self.logger.info(f'\n{source_df}')
        self.logger.info(f'\n{target_df}')
        if defect_file.empty:
            self.logger.info('Data Validation Passed')
        else:
            raise AssertionError('Data validation failed')

    def check_entire_data(self, source_file, target_query, engine, defect_file_path):
        expected_data = pd.read_csv(source_file).astype(str)
        actual_data = pd.read_sql(target_query, engine).astype(str)
        defect_file = self.savefile.save_the_mismatch_to_file(expected_data, actual_data, defect_file_path)
        if defect_file.empty:
            self.logger.info('Data Validation Passed')
        else:
            raise AssertionError('Data validation failed')

    @staticmethod
    def check_file_exists(file_path):
        if os.path.exists(file_path):
            return True
        else:
            raise ValueError(f"File not found at: {file_path}")

    def check_data_exists_in_file(self, file_path, file_type):
        if file_type == 'csv':
            df_actual = pd.read_csv(file_path)

        elif file_type == 'xml':
            df_actual = pd.read_xml(file_path, xpath='.//item')

        elif file_type == 'json':
            df_actual = pd.read_json(file_path)

        else:
            self.logger.error(f"Unsupported file type: {file_type}")
            raise ValueError(f"Unsupported file type: {file_type}")

        if df_actual.empty:
            self.logger.error(f"Data is not present in: {file_type}")
            raise AssertionError(f"Data is not present in: {file_type}")
        else:
            self.logger.info(f"Data is present in: {file_type}")
