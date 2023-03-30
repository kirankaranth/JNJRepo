"""
This module handles creation of tests and results into Jira XRay.
"""
import os
from user_arguments import UserArguments
from test_identifier import TestIdentifier
from test_creation import TestCreation
from result_creation import ResultCreation
from XRay_Result_Parser import Parser
from Handler.jira_handler import JiraHandler
import xray_logger_handler


class XrayUploader:
    """
    Handles creation of XRay test and results through Robot or Json format report
    """

    def __init__(self):
        parser = UserArguments()
        self.args = parser.parse_arguments()
        if not os.path.isabs(self.args.results_file):
            self.args.results_file = os.path.abspath(self.args.results_file)
        report_file = self.args.results_file
        self.logger = xray_logger_handler.setup(
            results_file=report_file, action=self.args.action
        )
        self.jira = None
        self.suites = []
        self.classified_dict = {}

    def process_xray_request(self):
        """
        Handles the overall flow for XRay test/result upload
        """
        self._read_test_results()
        if self.args.CREATE_TEST_JSON_FORMAT or self.args.RESULT_UPLOAD_JSON_FORMAT:
            self.create_json_format()
        else:
            self.jira = JiraHandler(self.args)
            self._classify_tests()
            if self.args.action in ["Create_Test", "Create_Test_And_Result"]:
                self._upload_robot_tests_to_xray()
            if self.args.action in ["Add_Result", "Create_Test_And_Result"]:
                self._add_results_to_test_execution()

    def _read_test_results(self):
        """
        Based on type of test report being uploaded read the data from the parsing functionality
        Updates data read into self.suites
        """
        if self.args.JSON_FORMAT:
            result_parser = Parser.json_parser.JsonParser
        else:
            result_parser = Parser.xml_robot.XmlRobot
        parser = result_parser(self.args.results_file)
        self.logger.info("Parsing Results file for test information")
        parser.parse_results()
        self.suites = parser.suites

    def create_json_format(self):
        """
        Creates json format after data is read in class structure of self.suites
        """
        json_creator = Parser.json_creator.JsonCreator(self.suites, self.args)
        json_creator.create_json_file()

    def _classify_tests(self):
        """
        Classifies tests into formats required for creating test/results in XRay.
        Queries Jira XRay for existing data to build this functionality
        Sets self.classified_dict to format:
                {classification: {test_from_report: jira linked test}}
        """
        self.logger.info("Classifying Tests")
        test_identifier = TestIdentifier(self.suites, self.args, self.jira)
        self.classified_dict = test_identifier.classify_tests()

    def _upload_robot_tests_to_xray(self):
        """
        Uploads robot test to XRay using self.classified_dict
        """
        test_creator = TestCreation(self.classified_dict, self.args, self.jira)
        test_creator.process_test_creation()

    def _add_results_to_test_execution(self):
        """
        Uploads robot test to XRay using self.classified_dict
        """
        story_id = self._get_story_id()
        obj_name = self._get_object_name()

        result_creator = ResultCreation(
            self.classified_dict, self.args, self.jira, story_id, obj_name
        )
        result_creator.process_test_result_creation()

    def _get_story_id(self):
        """
        Returns the story id from suites
        """
        for suite in self.suites:
            if suite.jira_id:
                return suite.jira_id[0]

        return ""

    def _get_object_name(self):
        """
        Returns the object name from suites.tests
        """
        suite = self.suites[0]
        test = suite.tests[0]
        test_name_split = test.name.split()
        return test_name_split[2]


if __name__ == "__main__":
    XRAY_UPLOADER = XrayUploader()
    XRAY_UPLOADER.process_xray_request()
