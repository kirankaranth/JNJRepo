"This module dynamically links variables with values in Data folder at runtime."

from __future__ import print_function
import base64
import os
import sys
import traceback
from robot.api import logger

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
PARENT_PATH = os.path.abspath(os.path.join(DIR_PATH, os.pardir))
if PARENT_PATH not in sys.path:
    sys.path.append(PARENT_PATH)
import Data.Web as Web


class DataAccess(object):
    "Links variable names to values dynamically at runtime."

    def __init__(self):
        self.curdir = os.path.dirname(os.path.realpath(__file__))
        self.parent_dir = os.path.abspath(os.path.join(self.curdir, os.pardir))
        self.setuppath()
        self.test_server = ""

    def setuppath(self):
        """
        If the parentDir is not in sys.path this function adds it to sys.path
        """
        if self.parent_dir not in sys.path:
            sys.path.insert(0, self.parent_dir)

    @staticmethod
    def get_safe_string(check_string):
        """
        Returns a unicode safe string for the passed check_string

        :param check_string: String to check for unicode safety.
        :type check_string: String
        :returns: Safe part of the check_string ignoring error chars
        """
        if isinstance(check_string, str):
            check_string = str(unicode(check_string.strip(), errors="ignore")).strip()
        elif isinstance(check_string, unicode):
            check_string = check_string.encode("utf-8", errors="ignore").strip()
        return check_string

    @staticmethod
    def get_secured_data(fieldvalue):
        """
        Gets base64 decoded value for the specified value

        :param fieldvalue: Base64 encoded string
        :type fieldvalue: string
        """
        try:
            fieldvalue = base64.b64decode(fieldvalue)
        except BaseException:
            logger.warn("Failed to decode base64 value.")
        return fieldvalue

    def get_data(self, area, fieldname, secured=False):
        """
        Fetches data from data files based on area, test server and fieldname

        :param area: Specifies the area under which a variable is defined
        :param fieldname: Specifies which field name to be looked up for value
        :returns: Value of the field searched under area.test_server location
        """
        fieldvalue = ""
        try:
            command = area + "." + self.test_server + "." + fieldname
            fieldvalue = eval(command)
            if not secured:
                logger.info(command + ": " + str(fieldvalue))
        except BaseException:
            logger.warn(
                "Failed to fetch value for field: "
                + area
                + "."
                + self.test_server
                + "."
                + fieldname
            )
            logger.warn(traceback.format_exc())
        return fieldvalue

    def get_Web_data(self, fieldname, secured=False):
        "Gets data for the Web field name"
        return self.get_data("Web", fieldname, secured)

    def get_Web_data_secured(self, fieldname, secured=True):
        "Gets data for the Web field name securely"
        return self.get_data("Web", fieldname, secured)

    def get_Dataanalytics_data(self, fieldname, secured=False):
        "Gets data for the Dataanalytics field name"
        return self.get_data("Dataanalytics", fieldname, secured)

    def get_Dataanalytics_data_secured(self, fieldname, secured=True):
        "Gets data for the Dataanalytics field name securely"
        return self.get_data("Dataanalytics", fieldname, secured)

    def get_Api_data(self, fieldname, secured=False):
        "Gets data for the Api field name"
        return self.get_data("Api", fieldname, secured)

    def get_Api_data_secured(self, fieldname, secured=True):
        "Gets data for the Api field name securely"
        return self.get_data("Api", fieldname, secured)


class Counter(object):
    "Maintains a counter for screenshot or any other counter throughout the tests."

    def __init__(self):
        self.count = 2

    def get_value(self):
        "Returns current count value"
        return self.count

    def increment_value(self):
        "Increments current count value"
        self.count += 1

    def reset_count(self):
        "Resets current count value to 2"
        self.count = 2


COUNT = Counter()
d = DataAccess()
var = d.get_data
wvar = d.get_Web_data  # Access any Web data variable
wvar_secured = d.get_Web_data_secured  # Access Web data variable without logging
# dvar = d.get_Dataanalytics_data                    # Access any Dataanalytics data variable
# dvar_secured = d.get_Dataanalytics_data_secured      # Access Dataanalytics data variable without logging
# avar = d.get_Api_data                    # Access any Api data variable
# avar_secured = d.get_Api_data_secured      # Access Api data variable without logging


if __name__ == "__main__":
    d.test_server = "Dev"
    print(var("test_value", "Web"))
