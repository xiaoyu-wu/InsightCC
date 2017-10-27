from heapq import *

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


def combined_zip_and_date_processing(input_file_path, zip_file_path, date_file_path):
    recorder_dict_zip = {}
    recorder_dict_date = {}
    with open(input_file_path, 'r') as input_file, open(zip_file_path, 'a') as output_file:
        record_line = input_file.readline()
        while record_line:
            record_dict = parse_record_line_with_filter(record_line)
            if record_dict is not None:
                output_str = aggregate_by_key(recorder_dict_zip, record_dict, key="ZIP_CODE")
                aggregate_by_key(recorder_dict_date, record_dict, key="TRANSACTION_DT")
                if output_str:
                    output_file.write(output_str+'\n')
            record_line = input_file.readline()
    with open(date_file_path, 'a') as output_file:
        for cmte_id in sorted(recorder_dict_date.iterkeys()):
            for date in sorted(recorder_dict_date[cmte_id].iterkeys()):
                output_str = "{id}|{dt}|{median}|{counts}|{total}".format(
                    id=cmte_id, dt=date, median=recorder_dict_date[cmte_id][date].findMedian(),
                    counts=recorder_dict_date[cmte_id][date].counts, total=recorder_dict_date[cmte_id][date].total
                )
                output_file.write(output_str+'\n')


if __name__ == "__main__":
    # For testing use
    import sys
    INPUT_FILE_PATH = sys.argv[1]
    OUTPUT_BY_ZIP_FILE_PATH = sys.argv[2]
    OUTPUT_BY_DATE_FILE_PATH = sys.argv[3]
    stream_in_out_by_zip(INPUT_FILE_PATH, OUTPUT_BY_ZIP_FILE_PATH)
    batch_in_out_by_date(INPUT_FILE_PATH, OUTPUT_BY_DATE_FILE_PATH)
