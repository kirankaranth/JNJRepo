"""
Handles user defined arguments passed from command line.
"""
import argparse
import os
import sys


class UserArguments:
    """
    Handles creation of parser and parsing of user input for XRay uploader
    """

    def __init__(self):
        "Sets up default values for the parser"
        self.jira_url = "https://jira.jnj.com"
        self.parser = self._create_user_arguments()

    def _create_user_arguments(self):
        parser = argparse.ArgumentParser("Parser for XRay tests/results uploader.")
        parser.add_argument(
            "-jurl",
            "--jira_url",
            help="Jira URL to connect to for execution.",
            default=self.jira_url,
        )
        parser.add_argument(
            "-u",
            "--username",
            help="Username to connect to Jira. "
            + "User should have update access in Jira Project",
            default="",
        )
        parser.add_argument(
            "-p", "--password", help="Password to connect to Jira.", default=""
        )
        parser.add_argument(
            "-r",
            "--results_file",
            help="The Robot Framework result Output.xml file",
            default="",
        )
        parser.add_argument(
            "-dr",
            "--dryrun",
            action="store_true",
            help="Should the execution have no effect on Jira",
        )
        parser.add_argument(
            "-nu",
            "--no_jira_upload",
            action="store_true",
            help="No upload of result into Jira. Only use for PDF generation.",
        )
        parser.add_argument("-rel", "--release", help="Release number", default="")
        parser.add_argument(
            "-jprj",
            "--jira_project_id",
            help="The Jira project code id for the project",
            default="",
        )
        parser.add_argument(
            "-xex",
            "--xray_testexecution_id",
            help="If the value specified then the result uploaded to"
            " this test execution id rather than creating a new one.",
            default="",
        )
        parser.add_argument(
            "-xtenv",
            "--xray_test_env",
            nargs="*",
            help="Test environment information to be added to test execution",
            default=[],
        )
        parser.add_argument(
            "-tzd",
            "--time_difference_from_gmt",
            help="Timezone difference from GMT e.g. +0530 for IST,"
            " -0400 for EST. If not given the system checks time "
            "difference from local machine to GMT",
            default="",
        )
        parser.add_argument(
            "-type",
            "--test_type",
            help="Type of test environment being executed e.g. UAT, System",
            choices=["System", "UAT"],
            default="System",
        )
        parser.add_argument(
            "-a",
            "--action",
            help="Action to perform for XRay uploader",
            choices=["Create_Test", "Add_Result", "Create_Test_And_Result"],
            default="Create_Test",
        )
        parser.add_argument(
            "-json",
            "--JSON_FORMAT",
            action="store_true",
            help="If specified then it means that the result "
            "file is of type json rather than Robot Output.xml file.",
        )
        parser.add_argument(
            "-df",
            "--datafile",
            help="Data file which contains information for earlier variables.",
            default="",
        )
        parser.add_argument(
            "-cvar",
            "--tests_fields",
            help="During tests creation you can pass additional jira fields"
            + " in dict format through this. "
            + "E.g. {'customfield_114300': 'custom value', ...}",
            default="",
        )
        parser.add_argument(
            "-tvar",
            "--tests_execution_fields",
            help="During tests results creation you can pass additional data for"
            + " test execution fields in dict format through this. "
            + "E.g. {'customfield_114300': 'custom value', ...}",
            default="",
        )
        parser.add_argument(
            "-ct_json",
            "--CREATE_TEST_JSON_FORMAT",
            action="store_true",
            help="If specified then it creates json format "
            "from Robot Output.xml file for test creation.",
        )
        parser.add_argument(
            "-res_json",
            "--RESULT_UPLOAD_JSON_FORMAT",
            action="store_true",
            help="If specified then it creates json format "
            "from Robot Output.xml file for result upload.",
        )
        # Json parameters
        json_params = [
            "-ct_json",
            "--CREATE_TEST_JSON_FORMAT",
            "-res_json",
            "--RESULT_UPLOAD_JSON_FORMAT",
            "-h",
            "--help",
        ]
        if any([i for i in json_params if i in sys.argv[1:]]):
            parser = self._add_json_creator_argument(parser)
        return parser

    @staticmethod
    def _add_json_creator_argument(parser):
        """
        Adds json creation argument arguments to parser
        :param parser: Parser to add options for json creation
        :return: Updated parser object
        """
        parser.add_argument(
            "-jof",
            "--json_output_file",
            help="Json output file path to be created from robot results.",
            default="Json_Results.json",
        )
        parser.add_argument(
            "-jpr",
            "--json_overall_priority",
            help="Priority to be applied across all tests in Json create test.",
            default="Medium",
        )
        parser.add_argument(
            "-jav",
            "--json_overall_affect_versions",
            help="Affect Version(s) to be applied across all tests in Json",
            nargs="*",
            default=[],
        )
        parser.add_argument(
            "-jfv",
            "--json_overall_fixed_versions",
            help="Fixed Version(s) to be applied across all tests in Json",
            nargs="*",
            default=[],
        )
        parser.add_argument(
            "-jtc",
            "--json_overall_test_components",
            help="Test Components to be applied across all tests in Json",
            nargs="*",
            default=[],
        )
        parser.add_argument(
            "-jtac",
            "--json_test_approval_category",
            help="Test Approval Category to be applied across all tests in Json",
            default="",
        )
        parser.add_argument(
            "-jta",
            "--json_overall_test_assignee",
            help="Test Assignee to be applied across all tests in Json",
            default="",
        )
        parser.add_argument(
            "-jtr",
            "--json_overall_test_reporter",
            help="Test Reporter to be applied across all tests in Json",
            default="",
        )
        parser.add_argument(
            "-jtco",
            "--json_overall_test_comment",
            help="Test comment to be applied across all tests in Json",
            default="",
        )
        parser.add_argument(
            "-jtenv",
            "--json_overall_test_environment",
            help="Test environment information to be applied across "
            + "all tests in Json",
            default="",
        )
        parser.add_argument(
            "-jtcat",
            "--json_overall_test_category",
            help="Test category to be applied across all tests in Json",
            default="",
        )
        parser.add_argument(
            "-jtts",
            "--json_overall_test_set",
            help="Created tests are added to this testset",
            default="",
        )
        parser.add_argument(
            "-jttp",
            "--json_overall_test_plan",
            help="Created tests are added to this testplan",
            default="",
        )
        return parser

    def parse_arguments(self, user_arguments=None):
        """
        Parses arguments specified by the user for XRay uploader
        :param user_arguments: List of user arguments.
        :return: values read from parser
        """
        if user_arguments is None:
            user_arguments = sys.argv[1:]
        args = self.parser.parse_args(user_arguments)
        if args.datafile and os.path.exists(args.datafile):
            args = self._process_data_file(args)
        return args

    def _process_data_file(self, args):
        """
        Reads data in the data file and updates argument values. Data from this
        file overrides what is defined by user in command line
        :param args: Arguments containing value for data file
        :return: Updated args values
        """
        with open(args.datafile, "r") as file_object:
            file_lines = file_object.readlines()
        for line in file_lines:
            line = line.strip()
            if not line or "=" not in line:
                # Blank line or information line
                continue
            key, value = line.split("=")
            exec("args." + key.strip() + " = '" + value.strip() + "'")
        args.dryrun = self._update_boolean_variable_values(args.dryrun)
        args.JSON_FORMAT = self._update_boolean_variable_values(args.JSON_FORMAT)
        args.no_jira_upload = self._update_boolean_variable_values(args.no_jira_upload)
        return args

    @staticmethod
    def _update_boolean_variable_values(variable):
        """
        Updates the boolean variable value after reading from data file to ensure boolean
        False remain as False
        :param variable: Variable to update
        :return: Actual value for boolean variable
        """
        actual_value = True
        if str(variable).lower() == "false" or not variable:
            actual_value = False
        return actual_value
