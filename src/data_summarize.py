from heapq import *
import pandas as pd

from record_parser import parse_record_line_with_filter


class Recorder:

    def __init__(self):
        self.heaps = [], []
        self.counts = 0
        self.total = 0

    def addNum(self, num):
        self.counts += 1
        self.total += num
        small, large = self.heaps
        heappush(small, -heappushpop(large, num))
        if len(large) < len(small):
            heappush(large, -heappop(small))

    def findMedian(self):
        small, large = self.heaps
        if len(large) > len(small):
            return int(large[0])
        return int(round((large[0] - small[0]) / 2.0))


def populate_database(input_file_path):
    with open(input_file_path) as input_file:
        record_line = input_file.readline()
        record_list = []
        while record_line:
            record_dict = parse_record_line_with_filter(record_line)
            if record_dict is not None:
                record_list.append(record_dict)
            record_line = input_file.readline()
    db = pd.DataFrame(record_list, columns=['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT'])
    db['TRANSACTION_AMT'] = db['TRANSACTION_AMT'].astype(float)
    return db


# def stream_populate_database(input_file_path):
#     # FIXME: Slow
#     db = pd.DataFrame(columns=['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT'])
#     with open(input_file_path) as input_file:
#         record_line = input_file.readline()
#         while record_line:
#             record_dict = parse_record_line_with_filter(record_line)
#             if record_dict is not None:
#                 db.append(record_dict, ignore_index=True)
#             record_line = input_file.readline()
#     db['TRANSACTION_AMT'] = db['TRANSACTION_AMT'].astype(float)
#     return db

# def update_by_zip(last_record_dict, db=None):
#     if last_record_dict['ZIP_CODE'] is None:
#         return
#     else:
#         if last_record_dict['CMTE_ID'] not in medianvals_by_zip.index.values:
#             record_dict_tr = {'CMTE_ID': last_record_dict['CMTE_ID'],
#                               'ZIP_CODE': last_record_dict['ZIP_CODE'],
#                               'MEDIAN': last_record_dict['TRANSACTION_AMT'],
#                               'COUNTS': 1,
#                               'TOTAL': last_record_dict['TRANSACTION_AMT']}
#             medianvals_by_zip.append(record_dict_tr)
#         else:
#             pass


# def aggregate_by_date(database):
#     result_list = []
#     cmte_ids = database['CMTE_ID'].value_counts().index.tolist()
#     for cmte_id in cmte_ids:
#         sub_db = database.loc[database['CMTE_ID'] == cmte_id]
#         trans_dates = sub_db['TRANSACTION_DT'].value_counts().index.tolist()
#         for trans_date in trans_dates:
#             ssub_db = sub_db.loc[sub_db['TRANSACTION_DT'] == trans_date]
#             trans_amts = ssub_db['TRANSACTION_AMT']
#             result = {'CMTE_ID': cmte_id,
#                       'TRANSACTION_DT': trans_date,
#                       'MEDIAN': trans_amts.median(),
#                       'COUNTS': trans_amts.size,
#                       'TOTAL': trans_amts.sum()}
#             result_list.append(result)
#     by_date_df = pd.DataFrame(result_list, columns=['CMTE_ID', 'TRANSACTION_DT', 'MEDIAN', 'COUNTS', 'TOTAL'])
#     return by_date_df


def aggregate_by_key(recorder_dict, input_record, key):
    cmte_id = input_record['CMTE_ID']
    key_value = input_record[key]
    if key_value is None:
        return
    if cmte_id not in recorder_dict.keys():
        recorder_dict[cmte_id] = {}
        recorder_dict[cmte_id][key_value] = Recorder()
    elif key_value not in recorder_dict[cmte_id].keys():
        recorder_dict[cmte_id][key_value] = Recorder()
    recorder_dict[cmte_id][key_value].addNum(input_record['TRANSACTION_AMT'])
    output_str = "{id}|{kv}|{median}|{counts}|{total}".format(
        id=cmte_id, kv=key_value, median=recorder_dict[cmte_id][key_value].findMedian(),
        counts=recorder_dict[cmte_id][key_value].counts, total=recorder_dict[cmte_id][key_value].total
    )
    return output_str


def stream_in_out_by_zip(input_file_path, output_file_path):
    recorder_dict = {}
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'a') as output_file:
        record_line = input_file.readline()
        while record_line:
            record_dict = parse_record_line_with_filter(record_line)
            if record_dict is not None:
                output_str = aggregate_by_key(recorder_dict, record_dict, key="ZIP_CODE")
                if output_str:
                    output_file.write(output_str+'\n')
            record_line = input_file.readline()


def batch_in_out_by_date(input_file_path, output_file_path):
    recorder_dict = {}
    with open(input_file_path, 'r') as input_file:
        record_line = input_file.readline()
        while record_line:
            record_dict = parse_record_line_with_filter(record_line)
            if record_dict is not None:
                aggregate_by_key(recorder_dict, record_dict, key="TRANSACTION_DT")
            record_line = input_file.readline()
    with open(output_file_path, 'a') as output_file:
        for cmte_id in sorted(recorder_dict.iterkeys()):
            for date in sorted(recorder_dict[cmte_id].iterkeys()):
                output_str = "{id}|{dt}|{median}|{counts}|{total}".format(
                    id=cmte_id, dt=date, median=recorder_dict[cmte_id][date].findMedian(),
                    counts=recorder_dict[cmte_id][date].counts, total=recorder_dict[cmte_id][date].total
                )
                output_file.write(output_str+'\n')


if __name__ == "__main__":
    # import os
    # import os.path as path
    # PROJECT_PATH = os.getcwd()
    # TEST_PATH = path.join(PROJECT_PATH, "insight_testsuite/tests/test_8")
    # INPUT_PATH = path.join(TEST_PATH, "input/itcont.txt")
    # database = populate_database(INPUT_PATH)
    import sys
    INPUT_FILE_PATH = sys.argv[1]
    OUTPUT_BY_ZIP_FILE_PATH = sys.argv[2]
    OUTPUT_BY_DATE_FILE_PATH = sys.argv[3]
    stream_in_out_by_zip(INPUT_FILE_PATH, OUTPUT_BY_ZIP_FILE_PATH)
    batch_in_out_by_date(INPUT_FILE_PATH, OUTPUT_BY_DATE_FILE_PATH)
