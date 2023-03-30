from __future__ import print_function

from builtins import staticmethod

from robot.api import logger
import os
import json
import shutil
import re
import traceback
import time
import getpass

from tabulate import tabulate


class Utils:
    def __init__(self):
        self.timer_start = 0
        self.timer_stop = 0
        self.timer_result_str = None

    def start_timer(self):
        """
        Starts a timer
        """
        self.timer_start = time.perf_counter()

    def stop_timer(self):
        """
        Stops timer
        """
        self.timer_stop = time.perf_counter()

    def time_taken(self):
        """
        return: a string message of the time taken
        """
        self.timer_result_str = (
            f"Test executed in {self.timer_stop - self.timer_start:0.4f} seconds"
        )
        self.reset_timer()
        return self.timer_result_str

    def reset_timer(self):
        self.timer_start = 0
        self.timer_stop = 0

    def write_log_msg(self, message):
        logger.console(message)

    @staticmethod
    def tabulate_string(tab_str):
        """
        Surrounds a string using tabulate for enhanced viewing in reports
        +--------------------------+
        | {tab_str}                |
        +--------------------------+
        :param tab_str:  string to tabulate
        :return: tabulated string
        """
        return tabulate([[f"{tab_str}"]], tablefmt="grid")

    @staticmethod
    def get_tables_and_keys_from_databricks(system):
        """
        Retrieves a dictionary of table and primary keys from Databricks file system
        :param system: the database under test
        :return: dictionary of all tables and primary keys
        """
        # system = 'panda'
        cwd = os.getcwd()

        # Copy directory from dbfs to temp directory
        os.system(
            "dbfs cp -r dbfs:/config/{0} {1}/temp/{2} --overwrite".format(
                system, cwd, system
            )
        )

        dict = {}
        for file in os.listdir("{0}/temp/{1}".format(cwd, system)):
            if "offset" not in str(file):
                with open(
                    "{0}/temp/{1}/{2}".format(cwd, system, os.fsdecode(file)), "r"
                ) as read_file:
                    data = json.load(read_file)
                    dict[data["tableName"]] = data["keyFields"]

        # Remove temp directory
        shutil.rmtree("{0}/temp/{1}".format(cwd, system))
        return dict

    @staticmethod
    def get_specific_tables_and_keys_from_databricks(system, specific_tables):
        """
        Retrieves a dictionary of specific table and primary keys from Databricks file system.
        Adapted from get_tables_and_keys_from_databricks for faster tests.
        :param system: the database under test
        :return: dictionary of all tables and primary keys
        """
        cwd = os.getcwd()

        # Copy directory from dbfs to temp directory
        os.system(
            "dbfs cp -r dbfs:/config/{0} {1}/temp/{2} --overwrite".format(
                system, cwd, system
            )
        )
        specific_tables = list(specific_tables.split(" "))

        dict = {}
        for file in os.listdir("{0}/temp/{1}".format(cwd, system)):
            if "offset" not in str(file):
                with open(
                    "{0}/temp/{1}/{2}".format(cwd, system, os.fsdecode(file)), "r"
                ) as read_file:
                    data = json.load(read_file)
                    if data["tableName"] in specific_tables:
                        dict[data["tableName"]] = data["keyFields"]

        # Remove temp directory
        shutil.rmtree("{0}/temp/".format(cwd))

        return dict

    @staticmethod
    def build_hive_table_on_flat_file(table, system, location, col_names):
        columns_and_types = []
        str_for_regex = ""
        for column in col_names:
            col_and_type = "`" + column + "`" + " STRING"
            columns_and_types.append(col_and_type)
            str_for_regex = str_for_regex + """(.*?)\\\\|\\\\^\\\\/"""
        system_and_table = system.upper() + "." + table.upper()
        create_table_string = (
            """DROP TABLE IF EXISTS """
            + system_and_table
            + """;\nCREATE EXTERNAL TABLE """
            + system_and_table
            + """(\n"""
            + ",\n".join(map(str, columns_and_types))
            + """)\nROW FORMAT\n SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'\nWITH SERDEPROPERTIES\n ( \n"input.regex" = '"""
            + str_for_regex[:-14]
            + """(.*)'\n)\nSTORED AS TEXTFILE\nLOCATION '"""
            + location
            + """';"""
        )
        return create_table_string

    @staticmethod
    def build_hive_table_on_csv(table, system, location, col_names):
        columns_and_types = []
        for column in col_names:
            col_and_type = "`" + column + "`" + " STRING"
            columns_and_types.append(col_and_type)
        system_and_table = system.upper() + "." + table.upper()
        create_table_string = (
            """DROP TABLE IF EXISTS """
            + system_and_table
            + """;\nCREATE EXTERNAL TABLE """
            + system_and_table
            + """(\n"""
            + ",\n".join(map(str, columns_and_types))
            + """)\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY ','\nSTORED AS TEXTFILE\nLOCATION '"""
            + location
            + """'\nTBLPROPERTIES("skip.header.line.count"="1");"""
        )
        return create_table_string

    @staticmethod
    def build_hive_table_on_parquet(table, system, location, col_names):
        columns_and_types = []
        for column in col_names:
            col_and_type = "`" + column + "`" + " STRING"
            columns_and_types.append(col_and_type)
        system_and_table = system.upper() + "." + table.upper()
        create_table_string = (
            """DROP TABLE IF EXISTS """
            + system_and_table
            + """;\nCREATE EXTERNAL TABLE """
            + system_and_table
            + """(\n"""
            + ",\n".join(map(str, columns_and_types))
            + """)\nSTORED AS PARQUET\nLOCATION '"""
            + location
            + """';"""
        )
        return create_table_string

    @staticmethod
    def get_tag_from_test_name(testname):
        """
        Extracts the jira test id from the robot test name using a regular expression.
        Ensures that Jira test id is in the format (4char-4digits_*digits)
        :param testname:  the name of the robot test
        :return jira_test_id: the jira story id with test number

        """
        try:
            # check testname has 4 letters,-,4digits,_, any amount of digits
            if re.search("^[\\w]{4}-[\\d]{4,5}_[\\d*]", testname):
                if len(testname) >= 12:
                    jira_test_id = testname.split(" ", 1)[0]
                    return jira_test_id
                else:
                    jira_test_id = testname
            else:
                raise ValueError(
                    "[ERROR] - The Test name must be prefixed with Jira ID eg.(XXXX_1111_11 "
                )
        except Exception as e:
            traceback.print_exc()
            raise
        return jira_test_id[:9]

    @staticmethod
    def get_source_system_sql(table, system_filter):
        l1_groups_path = "../src/main/config/l1_groups.json"
        with open(l1_groups_path) as file:
            l1_groups = json.load(file)
            for edm in l1_groups:
                if edm == table.upper():

                    queries = l1_groups[edm]["queries"]
                    if system_filter.lower() != "all":
                        queries = {system_filter.lower(): queries[system_filter]}

                    return queries

    @staticmethod
    def get_sql_name(object_name, source):
        l1_groups_path = "../src/main/config/l1_groups.json"
        with open(l1_groups_path) as file:
            l1_groups = json.load(file)
            return l1_groups[object_name.upper()]["queries"][source]

    @staticmethod
    def get_test_query(object_name, source, src_sys_cd):
        query_file_path = "../src/main/sql"

        sql_file = Utils.get_sql_name(object_name, source)
        sql_path = os.path.join(query_file_path, sql_file)

        with open(sql_path) as file:
            sql_raw = file.read()
            sql_raw = sql_raw.replace("{src}", source + "_robot")
            sql_raw = sql_raw.replace("{l1}", "l1_robot")
            sql_raw = sql_raw.replace("{p360}", "p360_robot")
            query = sql_raw.replace("{src_sys_cd}", src_sys_cd)

        return query

    @staticmethod
    def get_test_query_db(object_name, source, src_sys_cd):
        query_file_path = "../src/main/sql"

        sql_file = Utils.get_sql_name(object_name, source)
        sql_path = os.path.join(query_file_path, sql_file)

        with open(sql_path) as file:
            sql_raw = file.read()
            sql_raw = sql_raw.replace("{src}", source)
            sql_raw = sql_raw.replace("{l1}", "l1")
            sql_raw = sql_raw.replace("{p360}", "p360")
            query = sql_raw.replace("{src_sys_cd}", src_sys_cd)
        return query

    @staticmethod
    def check_expected_in_actual(expected, actual):

        if len(expected) != len(actual):
            return False

        for row in expected:
            if row not in actual:
                return False
        return True

    @staticmethod
    def get_tester_name():
        try:
            user = os.environ["JENKINS_USERNAME"]
        except KeyError:
            user = getpass.getuser()
        return user

    @staticmethod
    def get_test_env_query_db(object_name, source, src_sys_cd, env):
        query_file_path = "../src/main/sql"

        sql_file = Utils.get_sql_name(object_name, source)
        sql_path = os.path.join(query_file_path, sql_file)

        with open(sql_path) as file:
            sql_raw = file.read()
            sql_raw = sql_raw.replace("{src}", source)
            sql_raw = sql_raw.replace("{env}", env)
            sql_raw = sql_raw.replace("{l1}", "l1")
            sql_raw = sql_raw.replace("{p360}", "p360")
            query = sql_raw.replace("{src_sys_cd}", src_sys_cd)
        return query
