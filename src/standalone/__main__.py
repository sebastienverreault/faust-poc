from src.standalone.Tests.Test_01 import Test_01
from src.standalone.Tests.Test_02 import Test_02
from src.standalone.Tests.Test_03 import Test_03
from src.standalone.Tests.Test_04 import Test_04

import os

def main():
    """
        Entry Point
    """
    filename = os.path.abspath(r"./src/data/sample.log")
    listOfTest = ( 
        Test_01(filename), 
        Test_02(filename), 
        Test_03(filename), 
        Test_04(filename), 
    )
    for test in listOfTest:
        test.run()
        input("Press Enter to continue...")

    input("Press Enter to quit...")


if __name__ == "__main__":
    main()
