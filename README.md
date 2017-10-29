# Insight Code Competition 2017

This repo is my solution to 2017 Insight Code Competition: find political donors.

## Approach
 - An parser is first build to process each line of raw input file. The returned record as a dictionary is filtered taking into consideration for valid input.
 - For the task of calculating running median by recepient and zip, since output is expected for every line of record, we need an efficient algorithm to calculate median. The data structure chosen here is heap which facilitates median finding with complexity O(1). The complexity of inserting is O(log n).
 - For the task of calculating running median by recepient and date, since output is expected after all records are read, we can accept a more expensive way to calculate median, but data insertion should be cheap. The data structure chosen here is simply list with insertion complexity O(1) and median-finding complexity O(n log n), which is determined by the sorting algorithm.
 - Nested dictionary is used to store records for specific combination of recepient and zip / date. 

## Dependencies
This solution is written in Python 2.7 with the standard libraries. No third-party library is needed.

## Run instructions
1. Clone and checkout master branch
```
[~]$ git clone https://github.com/xiaoyu-wu/InsightCC
[~]$ cd ~/InsightCC
[~/InsightCC]$ git checkout master
```
2. To run tests:
```
[~/InsightCC]$ cd insight_testsuite
[~/InsightCC/insight_testsuite]$ ./run_tests.sh
```
3. To distill input file (./input/itcont.txt) into two output files (./output/medianvals_by_zip.txt and ./output/medianvals_by_date.txt) as described in Details of Challenge:
```
[~/InsightCC]$ ./run.sh
```
4. Three functions are exposed as API: `stream_in_out_by_zip` processes streaming data and writes to output file for every valid input; `batch_in_out_by_date` processes data as a batch of input and output after all data is read; `combined_zip_and_date_processing` executes the previous two tasks with higher effieciency by skipping one round of input file reading.
