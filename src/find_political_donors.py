import sys
from data_summarize import combined_zip_and_date_processing

if __name__ == "__main__":
    INPUT_FILE_PATH = sys.argv[1]
    OUTPUT_BY_ZIP_FILE_PATH = sys.argv[2]
    OUTPUT_BY_DATE_FILE_PATH = sys.argv[3]
    combined_zip_and_date_processing(INPUT_FILE_PATH, OUTPUT_BY_ZIP_FILE_PATH, OUTPUT_BY_DATE_FILE_PATH)

