import pandas as pd
from Utilities.custom_logger import LogGen


class SaveFile:
    log_gen = LogGen()
    logger = log_gen.logger()

    # Defect Data Storage Process
    def save_basic_check_defect_file(self, df, path):
        loc = rf"C:\Users\Anshu\Desktop\folder\ETL\CapstoneTestingFramework\DefectFile\\"
        location=loc+path
        self.logger.error(f'File is store in Project Defect file location and name is : {path}')
        df.to_csv(location, index=False)

    # Storing Mismatched Records from Data Validation
    def save_the_mismatch_to_file(self, source_query, target_query, path):
        defect_file = pd.merge(source_query, target_query, how="outer", indicator=True).query("_merge!='both'")
        defect_file['_merge'] = defect_file['_merge'].replace(
            {"left_only": "source_table", "right_only": "target_table"})
        defect_file.rename(columns={"_merge": "table"}, inplace=True)
        location = fr"C:\Users\Anshu\Desktop\folder\ETL\CapstoneTestingFramework\DefectFile\{path}"
        if not defect_file.empty:
            defect_file.to_csv(location, index=False)
            self.logger.error(f"Mismatch records at location: {location}")
        else:
            self.logger.info("No mismatch records. Defect file was not created.")
        return defect_file

    def save_information_file(self, df, path):
        loc = rf"C:\Users\Anshu\Desktop\folder\ETL\CapstoneTestingFramework\InformationFile\\"
        location = loc + path
        self.logger.info(f'File is store in Project Information file location and name is: {path}')
        df.to_csv(location, index=False)


