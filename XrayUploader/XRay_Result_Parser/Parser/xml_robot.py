"""
Provides a xml processing starting instance. This supports in creating report
for Robot framework test results
"""
from ..Support.soup_support import Soup
from ..Support.metadata_support import MetadataSupport
from ..RobotParser.test_parser import TestParser
from ..RobotParser.test_suite_parser import TestSuiteParser


class XmlRobot:
    """
    Starts execution of Robot Output Result which is the default Output.xml file
    generated through Robot Framework results
    """

    def __init__(self, report_path, parent_folder=""):
        """
        Initialized by passing the Robot Framework output xml report path to be processed
        :param report_path: Absolute path to Json report.
        """
        self.report_path = report_path
        self.soup_support = Soup(self.report_path)
        self.meta_data_support = MetadataSupport(self.soup_support.bs_soup)
        self.parent_folder = parent_folder
        self.suites = []

    def parse_results(self):
        """
        Parses xml results file read from Robot Framework output.xml results
        :return: suite object list read from parsing robot data
        """
        dependencies = self.meta_data_support.get_dependencies()
        metadata_dict = self.meta_data_support.get_meta_data_combined_information()
        all_tests = self.soup_support.bs_soup.find_all("test")
        test_counter = 0
        while test_counter < len(all_tests):
            test = all_tests[test_counter]
            data_driven_list = self._check_if_suite_data_driven(test)
            suite_parser = TestSuiteParser(test, self.soup_support, self.parent_folder)
            suite_object = suite_parser.fetch_test_suite_data(dependencies)
            test_parser = TestParser(test, self.soup_support)
            if data_driven_list:
                test_object = self._process_data_driven_suite_tests(
                    all_tests,
                    data_driven_list[test.attrs["name"]],
                    suite_object,
                    metadata_dict,
                )
            else:
                test_object = test_parser.fetch_test_data(suite_object, metadata_dict)
            if suite_object.tests is None:
                suite_object.tests = [test_object]
            else:
                suite_object.tests.append(test_object)
            if suite_object not in self.suites:
                self.suites.append(suite_object)
            if data_driven_list:
                test_counter += len(data_driven_list[test.attrs["name"]])
            else:
                test_counter += 1

    def _process_data_driven_suite_tests(
        self, all_tests, data_driven_test_position, suite_object, metadata_dict
    ):
        """
        Process data driven test under a single test object. The positions are identified
        by data_driven_test_position
        :param all_tests: All tests to process
        :param data_driven_test_position: Data driven tests positions
        :param suite_object: Suite object under which this test is created
        :param metadata_dict: Dict information to be updated with test variables
        :return: test object with data driven format of test results
        """
        tests_to_process = [
            x for i, x in enumerate(all_tests) if i in data_driven_test_position
        ]
        overall_test_object = None
        for test_counter, test in enumerate(tests_to_process):
            test_parser = TestParser(test, self.soup_support)
            test_object = test_parser.fetch_test_data(suite_object, metadata_dict)
            if not overall_test_object:
                overall_test_object = test_object
            else:
                overall_test_object = self._update_overall_test_object(
                    overall_test_object, test_object, test_counter + 1
                )
        return overall_test_object

    @staticmethod
    def _update_overall_test_object(
        overall_test_object, test_object, execution_counter=1
    ):
        """
        Overall test object is to be updated with step details
        :param overall_test_object: Overall Test object to be added for execution
        :param test_object: Test object to append step information to the overall test
        :param execution_counter: Execution counter for the step information
        :return: Overall Test Object updated with executions
        """
        for step in test_object.steps:
            step.execution_counter = execution_counter
        overall_test_object.variables["USER"].update(test_object.variables["USER"])
        overall_test_object.variables["ROBOT"].update(test_object.variables["ROBOT"])
        overall_test_object.steps.extend(test_object.steps)
        return overall_test_object

    @staticmethod
    def _check_if_suite_data_driven(test):
        """
        Checks if the test under a suite is data driven. It combines all such tests
        together for processing under a single report. And returns a counter to skip
        such tests from being processed separately
        :param test: soup of tag with name test
        :return: list of same name location
        """
        suite = test.parent
        data_driven_tests = {}
        tests_under_suite = suite.findChildren("test", recursive=False)
        if len(tests_under_suite) > 1:

            def duplicates(sequence, item):
                return [
                    position
                    for position, element in enumerate(sequence)
                    if element == item
                ]

            all_tests_name = [x.attrs["name"] for x in tests_under_suite]
            for test_name in all_tests_name:
                test_duplicate = duplicates(all_tests_name, test_name)
                if len(test_duplicate) > 1:
                    data_driven_tests[test_name] = test_duplicate
        return data_driven_tests
