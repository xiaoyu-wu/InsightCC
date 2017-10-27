import os
import os.path as path
import datetime

# Fields of interest in each line of record with corresponding position
TARGET_FIELDS = {"CMTE_ID": 0,
                 "ZIP_CODE": 10,
                 "TRANSACTION_DT": 13,
                 "TRANSACTION_AMT": 14,
                 "OTHER_ID": 15}


def parse_record_line(record_line):
    """
    Parses input string of record into a dictionary with information of interest
    Parameters
    ----------
    record_line : string
        A string for one line of record, separated by bars

    Returns
    -------
    dict
        The dictionary with info of interest
    """
    line_splited = record_line.split("|")
    record_dict = {}
    for field, index in TARGET_FIELDS.items():
        record_dict[field] = line_splited[index]
    return record_dict


def is_valid_other_id(other_id):
    return not bool(other_id)


def is_valid_trans_date(trans_date):
    if len(trans_date) != 8:
        return False
    try:
        datetime.datetime.strptime(trans_date, "%m%d%Y")
    except ValueError:
        return False
    return True


def is_valid_zip_code(zip_code):
    if len(zip_code) < 5:
        return False
    else:
        return True


def is_valid_cmte_id(cmte_id):
    return bool(cmte_id)


def is_valid_trans_amt(trans_amt):
    return bool(trans_amt)


def is_valid_record(record_dict):
    cmte_id = record_dict['CMTE_ID']
    other_id = record_dict['OTHER_ID']
    trans_amt = record_dict['TRANSACTION_AMT']
    if is_valid_other_id(other_id) and is_valid_cmte_id(cmte_id) and is_valid_trans_amt(trans_amt):
        return True
    else:
        return False


def parse_record_line_with_filter(record_line):
    record_dict = parse_record_line(record_line)

    if not is_valid_record(record_dict):
        return None

    if is_valid_zip_code(record_dict['ZIP_CODE']):
        record_dict['ZIP_CODE'] = record_dict['ZIP_CODE'][:5]
    else:
        record_dict['ZIP_CODE'] = None

    if not is_valid_trans_date(record_dict['TRANSACTION_DT']):
        record_dict['TRANSACTION_DT'] = None

    record_dict.pop('OTHER_ID')
    record_dict['TRANSACTION_AMT'] = int(record_dict['TRANSACTION_AMT'])
    return record_dict
