import os
import os.path as path
import datetime

# Fields of interest in each line of record with corresponding position according to the format described by FEC
TARGET_FIELDS = {"CMTE_ID": 0,
                 "ZIP_CODE": 10,
                 "TRANSACTION_DT": 13,
                 "TRANSACTION_AMT": 14,
                 "OTHER_ID": 15}


def parse_record_line(record_line):
    """Parses input string of record into a dictionary with information of interest

    Parameters
    ----------
    record_line : str
        A string for one line of record, separated by bars

    Returns
    -------
    dict
        The dictionary with info of interest, each field is str

    """
    line_splited = record_line.split("|")
    record_dict = {}
    for field, index in TARGET_FIELDS.items():
        record_dict[field] = line_splited[index]
    return record_dict


def is_valid_other_id(other_id):
    """Checks if OTHER_ID is empty

    Parameters
    ----------
    other_id : str

    Returns
    -------
    bool
        True if valid, False if not.

    """
    return not bool(other_id)


def is_valid_trans_date(trans_date):
    """Checks if TRANSACTION_DT is valid, not empty or malformed

    Parameters
    ----------
    trans_date : str

    Returns
    -------
    bool
        True if valid, False if not.

    """
    if len(trans_date) != 8:  # MMDDYYY is 8 digits
        return False
    try:
        datetime.datetime.strptime(trans_date, "%m%d%Y")
    except ValueError:
        return False
    return True


def is_valid_zip_code(zip_code):
    """Checks if ZIP_CODE is valid, at least 5 digits

    Parameters
    ----------
    zip_code : str

    Returns
    -------
    bool
        True if valid, False if not.

    """
    if len(zip_code) < 5:
        return False
    else:
        return True


def is_valid_cmte_id(cmte_id):
    """Checks if CMTE_ID is valid, not empty

    Parameters
    ----------
    cmte_id : str

    Returns
    -------
    bool
        True if valid, False if not.

    """
    return bool(cmte_id)


def is_valid_trans_amt(trans_amt):
    """Checks if TRANSACTION_AMT is valid, not empty.

    Parameters
    ----------
    trans_amt : str

    Returns
    -------
    bool
        True if valid, False if not.

    """
    return bool(trans_amt)


def is_valid_record(record_dict):
    """Checks if a dictionary of record is valid, with non-empty CMTE_ID, TRANSACTION_AMT and empty OTHER_ID.

    Parameters
    ----------
    record_dict : dict
        Dictionaries parsed from one line of record

    Returns
    -------
    bool
        True if valid, False if not.

    """
    cmte_id = record_dict['CMTE_ID']
    other_id = record_dict['OTHER_ID']
    trans_amt = record_dict['TRANSACTION_AMT']
    if is_valid_other_id(other_id) and is_valid_cmte_id(cmte_id) and is_valid_trans_amt(trans_amt):
        return True
    else:
        return False


def parse_record_line_with_filter(record_line):
    """Takes in one line of record, checks if record conform with input considerations, and outputs valid record.

    Parameters
    ----------
    record_line : str
        One line of record read from raw input txt file

    Returns
    -------
    None or dict
        If record is valid, return record parsed in dictionary form. Otherwise None.

    """
    record_dict = parse_record_line(record_line)

    # check validness
    if not is_valid_record(record_dict):
        return None
    if is_valid_zip_code(record_dict['ZIP_CODE']):
        # crop the first 5 digits
        record_dict['ZIP_CODE'] = record_dict['ZIP_CODE'][:5]
    else:
        # set invalid zip codes to None, but still passing on for analysis
        record_dict['ZIP_CODE'] = None

    if not is_valid_trans_date(record_dict['TRANSACTION_DT']):
        # set invalid dates to None, but still passing on for analysis
        record_dict['TRANSACTION_DT'] = None

    # OTHER_ID is no longer needed after validness check
    record_dict.pop('OTHER_ID')
    # make TRANSACTION_AMT integer for analysis
    record_dict['TRANSACTION_AMT'] = int(record_dict['TRANSACTION_AMT'])
    return record_dict
