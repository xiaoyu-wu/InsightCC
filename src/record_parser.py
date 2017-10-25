import os
import os.path as path

if __name__ == "__main__":
    PROJECT_PATH = os.getcwd()
    TEST_PATH = path.join(PROJECT_PATH, "insight_testsuite/tests/test_1")
    INPUT_PATH = path.join(TEST_PATH, "input/itcont.txt")
