import sys
from data_summarize import stream_in_out_by_zip, batch_in_out_by_date

if __name__ == "__main__":
    INPUT_FILE_PATH = sys.argv[1]
    OUTPUT_BY_ZIP_FILE_PATH = sys.argv[2]
    OUTPUT_BY_DATE_FILE_PATH = sys.argv[3]
    stream_in_out_by_zip(INPUT_FILE_PATH, OUTPUT_BY_ZIP_FILE_PATH)
    batch_in_out_by_date(INPUT_FILE_PATH, OUTPUT_BY_DATE_FILE_PATH)
