from heapq import *

from record_parser import parse_record_line_with_filter


class Recorder:
    """Records input TRANSACTION_AMT featuring fast median finding

    Attributes
    ----------
    heaps : tuple of lists
        Two min_heaps for numbers smaller / larger than median.

    counts : int
        Counts of input numbers
    total : int
        Sum of total numbers

    """

    def __init__(self):
        self.heaps = [], []
        self.counts = 0
        self.total = 0

    def addNum(self, num):
        """Adds number into Recorder

        Complexity O(log n)

        Parameters
        ----------
        num : int
            Number to be added into Recorder

        Returns
        -------
        None

        """
        self.counts += 1
        self.total += num
        small, large = self.heaps
        heappush(small, -heappushpop(large, num))
        # Make sure large heap is no shorter than small
        if len(large) < len(small):
            heappush(large, -heappop(small))

    def findMedian(self):
        """Finds median.

        Median should be root of larger number heap or average of the two roots.
        Complexity O(1)

        Returns
        -------
        int
            Running median of all input numbers

        """
        small, large = self.heaps
        if len(large) > len(small):
            return int(large[0])
        return int(round((large[0] - small[0]) / 2.0))


class Recorder2:
    """Records input TRANSACTION_AMT featuring fast number adding

    Attributes
    ----------
    series : list
        One list with all input numbers unsorted.
    counts : int
        Counts of input numbers
    total : int
        Sum of total numbers

    """

    def __init__(self):
        self.series = []
        self.counts = 0
        self.total = 0

    def addNum(self, num):
        """Adds number to the list

        Complexity O(1)

        Parameters
        ----------
        num : int
            Input number

        Returns
        -------
        None

        """
        self.counts += 1
        self.total += num
        self.series.append(num)

    def findMedian(self):
        """Finds median of all input numbers

        Complexity should dominated by Python sorted, O(n * log n)

        Returns
        -------
        int
            Median of all input numbers

        """
        n = self.counts
        if n < 1:
            return None
        if n % 2 == 1:
            return int(sorted(self.series)[n // 2])
        else:
            return int(round(sum(sorted(self.series)[n // 2 - 1:n // 2 + 1]) / 2.0))


def aggregate_by_key(recorder_dict, input_record, key, data_recorder=Recorder, string_output=True):
    """Adds input record into the corresponding data recorder and returns string to write.

    Parameters
    ----------
    recorder_dict : dict
        the nested dictionary storing data recorders
    input_record : dict
        the dictionary of record parsed from one line of raw input txt file
    key : str
        the field (ZIP_CODE or TRANSACTION_DT) according to which to aggregate data
    data_recorder : classobj
        the type of class object (Recorder or Recorder2) to store input record
    string_output : bool
        False mutes string output

    Returns
    -------
    None or str
        If input is invalid or output is muted, returns None. Otherwise returns the string to be written into file.

    """
    cmte_id = input_record['CMTE_ID']
    key_value = input_record[key]

    # Skips record with invalid field of interest, e.g. analyzing by zip but record has invalid zip
    if key_value is None:
        return

    # Opens up new dictionary keys for new incoming id or field value
    if cmte_id not in recorder_dict.keys():
        recorder_dict[cmte_id] = {}
        recorder_dict[cmte_id][key_value] = data_recorder()
    elif key_value not in recorder_dict[cmte_id].keys():
        recorder_dict[cmte_id][key_value] = data_recorder()

    recorder_dict[cmte_id][key_value].addNum(input_record['TRANSACTION_AMT'])

    if string_output:
        output_str = "{id}|{kv}|{median}|{counts}|{total}".format(
            id=cmte_id, kv=key_value, median=recorder_dict[cmte_id][key_value].findMedian(),
            counts=recorder_dict[cmte_id][key_value].counts, total=recorder_dict[cmte_id][key_value].total
        )
        return output_str


def stream_in_out_by_zip(input_file_path, output_file_path):
    """Reads input txt file line by line, calculates running median, and writes to file for every valid record line.

    For use of processing data only by zip.

    Parameters
    ----------
    input_file_path : str
        Path to input txt file
    output_file_path
        Path to output txt file

    Returns
    -------
    None

    """

    recorder_dict = {}  # Nested dictionary to store recorders.

    with open(input_file_path, 'r') as input_file, open(output_file_path, 'a') as output_file:
        record_line = input_file.readline()
        while record_line:  # Reads until file ends
            record_dict = parse_record_line_with_filter(record_line)
            if record_dict is not None:  # Valid record
                output_str = aggregate_by_key(recorder_dict, record_dict, key="ZIP_CODE")
                if output_str:  # Valid zip code
                    output_file.write(output_str+'\n')
            record_line = input_file.readline()


def batch_in_out_by_date(input_file_path, output_file_path):
    """Reads all input records, calculates medians for each combination and writes to file in the end.

    Parameters
    ----------
    input_file_path : str
        Path to input txt file
    output_file_path : str
        Path to output txt file

    Returns
    -------
    None

    """

    recorder_dict = {}  # Nested dictionary to store recorders.
    with open(input_file_path, 'r') as input_file:
        record_line = input_file.readline()
        while record_line:  # Reads until file ends
            record_dict = parse_record_line_with_filter(record_line)
            if record_dict is not None:  # Valid record
                aggregate_by_key(recorder_dict, record_dict, key="TRANSACTION_DT",
                                 data_recorder=Recorder2, string_output=False)
            record_line = input_file.readline()
    with open(output_file_path, 'a') as output_file:
        for cmte_id in sorted(recorder_dict.iterkeys()):  # Sort CMTE_ID alphabetically
            for date in sorted(recorder_dict[cmte_id].iterkeys()):  # Sort TRANSACTION_DT chronologically
                output_str = "{id}|{dt}|{median}|{counts}|{total}".format(
                    id=cmte_id, dt=date, median=recorder_dict[cmte_id][date].findMedian(),
                    counts=recorder_dict[cmte_id][date].counts, total=recorder_dict[cmte_id][date].total
                )
                output_file.write(output_str+'\n')


def combined_zip_and_date_processing(input_file_path, zip_file_path, date_file_path):
    """Combines analysis by zip and by date to skip one round of file reading.

    Parameters
    ----------
    input_file_path : str
        Path to input txt file
    zip_file_path : str
        Path to output txt file for zip analysis
    date_file_path : str
        Path to output txt file for date analysis

    Returns
    -------
    None

    """

    # Nested dictionaries for the two analyses, by zip and by date
    recorder_dict_zip = {}
    recorder_dict_date = {}

    # Stream in and stream out for zip analysis
    with open(input_file_path, 'r') as input_file, open(zip_file_path, 'a') as output_file:
        record_line = input_file.readline()
        while record_line:  # Reads until file ends
            record_dict = parse_record_line_with_filter(record_line)
            if record_dict is not None:  # Valid record
                # for zip analysis
                output_str = aggregate_by_key(recorder_dict_zip, record_dict, key="ZIP_CODE")
                # for date analysis
                aggregate_by_key(recorder_dict_date, record_dict, key="TRANSACTION_DT",
                                 data_recorder=Recorder2, string_output=False)
                if output_str:  # Valid zip code
                    output_file.write(output_str+'\n')
            record_line = input_file.readline()

    # Writes date analysis after all date is in
    with open(date_file_path, 'a') as output_file:
        for cmte_id in sorted(recorder_dict_date.iterkeys()):  # Sort CMTE_ID alphabetically
            for date in sorted(recorder_dict_date[cmte_id].iterkeys()):  # Sort TRANSACTION_DT chronologically
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
