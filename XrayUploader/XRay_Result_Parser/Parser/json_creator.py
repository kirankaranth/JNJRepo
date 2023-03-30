"""
Creates Json template file from data read from Robot Framework test report.
"""
import json
import os
import xray_logger_handler
from date_and_time_support import DateTimeSupport
from . import parser_variables as variables


class JsonCreator:
    """
    Creates json file from data read from robot results
    """

    def __init__(self, suites, args):
        self.suites = suites
        self.args = args
        self.report_path = self.args.json_output_file
        self.json_data_dict = {}
        self.date_time_support = DateTimeSupport(self.args)
        self.logger = xray_logger_handler.setup()

    def _update_dict(self, update_dict, field, value, variable=False, date_type=False):
        """
        Updates field into a data dictionary.
        :param update_dict: Dictionary to update
        :param field: Dictionary key
        :param value: Value to be updated
        :param variable: Check if value is of type variable
        :param date_type: Check if value is data type
        """
        if date_type:
            value = self.date_time_support.get_datetime_representation_json(value)
        elif variable:
            top_level_variables = self._read_variable_dict(value)
            keyword_level_variables = self._read_variable_dict(
                value.get("Keyword_Name_Variables", {})
            )
            argument_level_variables = self._read_variable_dict(
                value.get("Arguments", {})
            )
            top_level_variables.extend(keyword_level_variables)
            top_level_variables.extend(argument_level_variables)
            value = ",".join(top_level_variables)
        if value:
            update_dict[field] = value

    @staticmethod
    def _read_variable_dict(variable_dict):
        """
        Variable dict which may contain keys for USER and ROBOT to be read in the
        format key:value,key2:value2.. etc.
        :param variable_dict: Dict which may contain variable values for ROBOT and USER types
        :return: string of value read from the dict
        """
        variable_value = []
        if isinstance(variable_dict, dict):
            for key in variable_dict.get("USER", {}):
                variable_value.append(key + ":" + variable_dict["USER"][key])
            for key in variable_dict.get("ROBOT", {}):
                variable_value.append(key + ":" + variable_dict["ROBOT"][key])
        return variable_value

    def create_json_file(self):
        """
        Creates Json format file from data read from robot report Output.xml
        :return: file path created with json data
        """
        if self.args.CREATE_TEST_JSON_FORMAT:
            self._create_test_json_dict()
        elif self.args.RESULT_UPLOAD_JSON_FORMAT:
            self._add_result_json_dict()
        json_log_data = json.dumps(self.json_data_dict, indent=2)
        if not os.path.isabs(self.report_path):
            directory = os.path.dirname(self.args.results_file)
            self.report_path = os.sep.join([directory, self.report_path])
        with open(self.report_path, "w") as json_file:
            json_file.write(json_log_data)
        self.logger.info("Created Json file from Robot Data at: %s" % self.report_path)
        self.logger.info("Json file data: \n%s" % json_log_data)

    def _check_and_update_dict(self, value, update_dict, field_name):
        """
        Updates field name into update dict based on if value is available
        :param value: Check to see if the value is available
        :param update_dict: Dict to update field value to
        :param field_name: Name of the field to update in update_dict
        """
        if value:
            self._update_dict(update_dict, field_name, value)

    def _create_test_json_dict(self):
        """
        Processes data read from robot framework output to json format data
        """
        self.json_data_dict[variables.PROJECT_FIELD] = self.args.jira_project_id
        if self.suites:
            self.json_data_dict[variables.SUITE_FIELD] = []
        for suite in self.suites:
            suite_dict = {}
            self._update_dict(suite_dict, variables.JIRA_ID_FIELD, suite.jira_id)
            self._update_dict(
                suite_dict, variables.TEST_TYPE, variables.AUTOMATION_TEST_TYPE
            )
            self._update_dict(suite_dict, variables.TEST_LABELS, suite.label_id)
            priority = (
                self.args.json_overall_priority or variables.AUTOMATION_TEST_PRIORITY
            )
            self._update_dict(suite_dict, variables.TEST_PRIORITY, priority)
            self._check_and_update_dict(
                self.args.json_overall_affect_versions,
                suite_dict,
                variables.TEST_AFFECTS_VERSION,
            )
            self._check_and_update_dict(
                self.args.json_overall_fixed_versions,
                suite_dict,
                variables.TEST_FIX_VERSION,
            )
            self._check_and_update_dict(
                self.args.json_overall_test_components,
                suite_dict,
                variables.TEST_COMPONENTS,
            )
            self._check_and_update_dict(
                self.args.json_test_approval_category,
                suite_dict,
                variables.TEST_APPROVAL_CATEGORY,
            )
            self._check_and_update_dict(
                self.args.json_overall_test_assignee,
                suite_dict,
                variables.TEST_ASSIGNEE,
            )
            self._check_and_update_dict(
                self.args.json_overall_test_reporter,
                suite_dict,
                variables.TEST_REPORTER,
            )
            self._check_and_update_dict(
                self.args.json_overall_test_comment, suite_dict, variables.TEST_COMMENT
            )
            self._check_and_update_dict(
                self.args.json_overall_test_environment,
                suite_dict,
                variables.TEST_ENVIRONMENT_INFO,
            )
            self._check_and_update_dict(
                self.args.json_overall_test_category,
                suite_dict,
                variables.TEST_CATEGORY,
            )
            self._check_and_update_dict(
                self.args.json_overall_test_set,
                suite_dict,
                variables.TEST_SET_FOR_TESTS,
            )
            self._check_and_update_dict(
                self.args.json_overall_test_plan,
                suite_dict,
                variables.TEST_PLAN_FOR_TESTS,
            )
            suite_dict[variables.TESTS_SECTION] = self._get_create_tests_json_list(
                suite.tests
            )
            self.json_data_dict[variables.SUITE_FIELD].append(suite_dict)

    def _get_create_tests_json_list(self, tests):
        """
        Reads tests information in a list of dicts for json generation.
        Supports generation of json for create test workflow
        :param tests: Test objects under a suite
        :return: list of dictionary containing test information.
        """
        tests_list = []
        for test in tests:
            test_dict = {}
            self._update_dict(test_dict, variables.TESTS_NAME, test.name)
            self._check_and_update_dict(
                test.jira_id, test_dict, variables.JIRA_ID_FIELD
            )
            self._check_and_update_dict(test.label_id, test_dict, variables.TEST_LABELS)
            self._check_and_update_dict(
                test.doc, test_dict, variables.TESTS_DOCUMENTATION
            )
            self._check_and_update_dict(
                test.doc, test_dict, variables.TESTS_DOCUMENTATION
            )
            test_dict[variables.STEPS_SECTION] = self._get_create_tests_steps_list(
                test.steps
            )
            tests_list.append(test_dict)
        return tests_list

    def _get_create_tests_steps_list(self, steps):
        """
        Reads steps information in a list of dicts for json generation.
        Supports generation of json for create test workflow
        :param steps: Step objects under a test
        :return: list of dictionary containing steps information.
        """
        steps_list = []
        for step_number, step in enumerate(steps):
            step_dict = {}
            self._update_dict(step_dict, variables.STEPS_NUMBER, str(step_number + 1))
            self._update_dict(step_dict, variables.STEPS_DESCRIPTION, step.description)
            self._update_dict(step_dict, variables.STEPS_EXPECTED, step.expected)
            self._update_dict(
                step_dict, variables.STEPS_VARIABLES, step.variables, variable=True
            )
            self._update_dict(
                step_dict, variables.STEPS_DATA_DRIVEN, str(step.execution_counter)
            )
            steps_list.append(step_dict)
        return steps_list

    def _add_result_json_dict(self):
        """
        Processes data read from robot framework output to json format data
        Supports creation of test result.
        """
        if self.suites:
            self.json_data_dict[variables.SUITE_FIELD] = []
        for suite in self.suites:
            suite_info_dict = {variables.SE_INFO: {}}
            info_dict = suite_info_dict[variables.SE_INFO]
            self._update_dict(
                info_dict, variables.SE_SUMMARY, variables.SE_SUMMARY_DEFAULT
            )
            self._update_dict(
                info_dict, variables.SE_DESCRIPTION, variables.SE_DESCRIPTION_DEFAULT
            )
            self._update_dict(
                info_dict, variables.SE_DESCRIPTION, variables.SE_DESCRIPTION_DEFAULT
            )
            self._update_dict(
                info_dict, variables.SE_START_DATE, variables.SE_DESCRIPTION_DEFAULT
            )

            suite_info_dict[variables.TESTS_SECTION] = self._get_create_tests_json_list(
                suite.tests
            )
            self.json_data_dict[variables.SUITE_FIELD].append(suite_info_dict)

    def _read_attachment_information(self, attachments):
        """
        Reads attachments information in a list of dicts for json generation
        :param attachments: Step objects under a test
        :return: list of dictionary containing attachments information.
        """
        attachments_list = []
        for attachment in attachments:
            attachment_dict = {}
            self._update_dict(attachment_dict, "path", attachment.attachment_path)
            self._update_dict(
                attachment_dict,
                "creation_time",
                attachment.time_created,
                date_type=True,
            )
            attachments_list.append(attachment_dict)
        return attachments_list
