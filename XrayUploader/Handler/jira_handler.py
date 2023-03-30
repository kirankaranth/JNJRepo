"""
This module handles working with Jira.
"""
import functools
import json
import ntpath
import os
import sys
import threading
import traceback
from jira import JIRA
from jira.exceptions import JIRAError

current_file_path = os.path.dirname(os.path.realpath(__file__))
parent_directory_path = os.path.abspath(os.path.join(current_file_path, os.pardir))
if parent_directory_path not in sys.path:
    sys.path.append(parent_directory_path)
from user_arguments import UserArguments
import xray_logger_handler
from xray_api_requests import XrayApiRequests
from Report_Generation.report_generator import ReportGenerator
from date_and_time_support import DateTimeSupport
import variables


class JiraHandler(object):
    """
    Handles connection and api execution to Jira using python jira api class
    """

    def __init__(self, user_args):
        self.logger = xray_logger_handler.setup()
        self.args = user_args
        self.jira = self._connect_to_jira()
        self.existing_jira_tests = {}
        self.xray_api = None
        self.date_time_support = DateTimeSupport(self.args)

    def _connect_to_jira(self):
        self.logger.info("Connecting to Jira: %s", self.args.jira_url)
        jira = JIRA(
            self.args.jira_url, basic_auth=(self.args.username, self.args.password)
        )
        self.logger.info("Connected to Jira as user: %s", self.args.username)
        return jira

    def setup_linked_tests(self, jira_id_list):
        """
        Searches for existing linked tests within Jira.
        If found they are updated to a dict self.existing_jira_tests
        :param jira_id_list: List of Jira id for the suite
        """
        linked_tests = []
        for test_jira_id in self.existing_jira_tests:
            if test_jira_id in jira_id_list:
                linked_tests.extend(self.existing_jira_tests[test_jira_id])
        if not linked_tests:
            for test_jira_id in jira_id_list:
                linked_tests.extend(self._get_linked_test_issues(test_jira_id))
                self.existing_jira_tests[test_jira_id] = linked_tests
        return linked_tests

    def create_test_execution(self, story_id, release, object_name):
        """
        Create a new test execution for the project if it is not specified from
        command line
        :return: Test Execution Jira object
        """
        jira_test_execution = None
        if not self.args.no_jira_upload:
            self.logger.info(
                "Creating new Test Execution since no existing "
                + "Test Execution specified."
            )
            if release:
                test_title = (
                    variables.SUMMARY + release + " - " + object_name + " - " + story_id
                )
            else:
                test_title = variables.SUMMARY + object_name + " - " + story_id
            jira_test_execution = self.jira.create_issue(
                project=self.args.jira_project_id,
                summary=test_title,
                description=variables.EXECUTION_DESCRIPTION,
                issuetype={"name": "Test Execution"},
            )
            self.logger.info("New Test Execution created %s", jira_test_execution.key)
        return jira_test_execution

    def _get_linked_test_issues(self, story_jira_id):
        """
        Looks up tests under jira id
        :param story_jira_id: Jira Id for the test
        :return: test classification of one of the types: []
        """
        searched_tests = []
        if isinstance(story_jira_id, list):
            for issue in story_jira_id:
                searched_tests.extend(
                    self.jira.search_issues(
                        "issuetype = Test and issue in linkedissues(" + issue + ")"
                    )
                )
        else:
            searched_tests = self.jira.search_issues(
                "issuetype = Test and issue in linkedissues(" + story_jira_id + ")"
            )
        return searched_tests

    def create_test(self, report_test, semaphore=None):
        """
        Creates a test in Jira based on the test object built through parsing xml/json reports.
        :param report_test: XRay_Result_Parser.Models.test_model.Test object
        :param semaphore: For parallel execution acquire a semaphore before running
        :return: True if test created successfully
        """
        logs = []
        lock = threading.Lock()
        if semaphore:
            semaphore.acquire()
        test_creation_dict = self._create_test_dict(report_test)
        test_issue = self.jira.create_issue(**test_creation_dict)
        story_id = self._add_link_to_jira_issue(test_issue, report_test.jira_id)
        self.update_issue_with_dict(test_issue, self.args.tests_fields)
        log_info = "'%s' created under '%s' as id '%s'"
        log_info = log_info % (
            test_issue.fields.summary,
            ", ".join(story_id),
            test_issue.key,
        )
        logs.append(log_info)
        lock.acquire()
        for log in logs:
            self.logger.info(log)
        lock.release()
        if semaphore:
            semaphore.release()

    @property
    @functools.lru_cache()
    def jira_field_mapping(self):
        """
        Fetches jira fields mapping in a dict of format {"Screen Name": "ID name",...}
        :return: dict of field to id mapping for jira
        """
        all_fields = self.jira.fields()
        return {field["name"]: field["id"] for field in all_fields}

    def update_issue_with_dict(self, issue_object, update_dict):
        """
        Updates the issue with update dictionary values
        :param issue_object: Jira issue to update
        :param update_dict: Dictionary data to update jira issue
        """
        if update_dict:
            try:
                if isinstance(update_dict, str):
                    update_dict = self._create_dict_from_string(update_dict)
                else:
                    update_dict = json.loads(update_dict)
            except TypeError:
                self.logger.info(
                    "Could not update issue %s with user arguments",
                    issue_object.key,
                    str(update_dict),
                )
                update_dict = None
        if update_dict:
            update_fields_dict = {}
            found = True
            for key in update_dict:
                mapped_id = self.jira_field_mapping.get(key, "")
                # If field name exists in mapping
                if mapped_id:
                    update_fields_dict[mapped_id] = update_dict[key]
                else:
                    found = False
                    break
            if found:
                self.logger.info("Updating fields %s", str(update_fields_dict))
                issue_object.update(fields=update_fields_dict)

    def _create_dict_from_string(self, update_dict_string):
        """
        Updates the dictionary string passed from command line into json readable data
        :param update_dict_string: String of format of dict
        :return: quoted string for dict creation
        """
        data_dict = {}
        if update_dict_string.startswith("{") and update_dict_string.endswith("}"):
            # Removing braces from dict {}
            update_dict_string = update_dict_string[1:-1]
            for pair in update_dict_string.split(","):
                key, value = pair.split(":", 1)
                key = key.strip()
                value = value.strip()
                value_list = []
                if value.startswith("[") and value.endswith("]"):
                    value = value[1:-1]
                    for inner_value in value.split(","):
                        value_list.append(inner_value.strip())
                    data_dict[key] = value_list
                else:
                    data_dict[key] = value
                self._update_value_for_known_jira_types(data_dict, key, value_list)
        return data_dict

    @staticmethod
    def _update_value_for_known_jira_types(data_dict, key, value_list):
        """
        Process further update for data dict for values of known data types
        :param data_dict: dict of data updating the values
        :param key: key being updated in data_dict
        :param value_list: value list being added in data dict
        :return: Updated data_dict value
        """
        value_update_list = []
        if key in ["Affects Version/s", "Fix Version/s"]:
            for value in value_list:
                value_update_list.append({"name": value})
            data_dict[key] = value_update_list

    def _create_test_dict(self, report_test, update_test=False):
        """
        Creates dictionary to process creation of new test
        :param report_test: The test read from xml/json report
        :param update_test: Checks if the dict needs to be updating
                the test or creating a new test
        :return: dictionary to process data creation/update
        """
        test_dict = {
            "description": report_test.doc,
            "labels": report_test.label_id,
            "customfield_11400": variables.TEST_TYPE_MAPPING,
            "customfield_11404": self._get_step_creation_data(report_test),
        }
        if not update_test:
            test_dict.update(
                {
                    "project": self.args.jira_project_id,
                    "summary": report_test.name,
                    "issuetype": variables.TEST_ISSUE_TYPE,
                }
            )
        return test_dict

    @staticmethod
    def _get_variables_from_data_dict(step_variables):
        """
        Gets step variables from from dict
        :param step_variables: step variables dict
        """
        step_data = []
        for first_level in step_variables:
            for key in step_variables[first_level]:
                key_level_dict = step_variables[first_level][key]
                if (
                    key in ["USER", "ROBOT"]
                    and isinstance(key_level_dict, dict)
                    and key_level_dict
                ):
                    step_data.extend(list(key_level_dict.keys()))
        return "\n".join(sorted(list(set(step_data))))

    def _get_step_creation_data(self, test):
        """
        Creates dict for adding a step definition to test
        :param test: XRay_Result_Parser.Models.test_model.Test object
        :return: step data dict
        """
        steps_data_list = []
        for step_counter, step in enumerate(test.steps):
            step_description = self._get_safe_step_data(step.description)
            step_data = self._get_variables_from_data_dict(step.variables)
            result = self._get_safe_step_data(step.expected)
            step_dict = {
                "index": step_counter + 1,
                "fields": {
                    "Action": result,
                    "Data": step_data,
                    "Expected Result": step_description,
                },
            }
            steps_data_list.append(step_dict)
        return {"steps": steps_data_list}

    @staticmethod
    def _get_safe_step_data(data):
        """
        Checks for unsafe characters in data string and replaces
        them with acceptable strings
        :param data: data string to process
        :return: Updated data string
        """
        return data.replace("{", "(").replace("}", ")")

    def _add_link_to_jira_issue(self, jira_test, story_id):
        """
        Creates a link on the jira issue to the newly created jira test under the project
        :param jira_test: Created test issue
        :param story_id: Issue Id to link the test with. Can be more than 1
        """
        if not story_id:
            self.logger.info(
                "No linked jira issue specified."
                " Test %s is not linked to any particular issue",
                jira_test.fields.summary,
            )
            return story_id
        if isinstance(story_id, str):
            story_id = [story_id]
        for issue in story_id:
            self.jira.create_issue_link(
                type="Tests", inwardIssue=jira_test.key, outwardIssue=issue
            )
        return story_id

    @staticmethod
    def _verify_strings_equal(jira_data_dict, update_data_dict, field_name):
        """
        Compares for equivalence of field name from two dictionary contents
        :param jira_data_dict: Data dict read from jira issue
        :param update_data_dict: Data dict to update jira issue
        :param field_name: Name of the field to check for equivalence
        :return: True if the field_name value is equal in both dicts
        """
        return jira_data_dict.get(field_name, "") == update_data_dict.get(
            field_name, ""
        )

    @staticmethod
    def _verify_customfield_11400_equal(jira_data_dict, update_data_dict):
        """
        Compares for equivalence of customfield_11400 from two dictionary contents
        :param jira_data_dict: Data dict read from jira issue
        :param update_data_dict: Data dict to update jira issue
        :return: True if the customfield_11400 value is equal in both dicts
        """
        test_same = True
        field_name = "customfield_11400"
        if jira_data_dict.get(field_name) and update_data_dict.get(field_name):
            test_same = jira_data_dict.get(field_name).value == update_data_dict.get(
                field_name, {}
            ).get("value", "")
            test_same = test_same and jira_data_dict.get(
                field_name
            ).id == update_data_dict.get(field_name, {}).get("id", "")
        return test_same

    @staticmethod
    def _check_test_is_same(jira_test_steps, update_steps):
        """
        Checks if the test is same using the jira test steps and test steps to update

        :param jira_test_steps: Steps read from jira test
        :param update_steps: Test steps read from report
        :return: True if the test is same as read from report when compared with jira test
        """
        test_same = True
        for counter, step in enumerate(jira_test_steps):
            jira_step_expected = getattr(
                getattr(step, "fields", ""), "Expected Result", ""
            )
            jira_step_action = getattr(getattr(step, "fields", ""), "Action", "")
            jira_step_data = getattr(getattr(step, "fields", ""), "Data", "")
            jira_step_attachments = getattr(step, "attachments", [])

            test_same = jira_step_action == update_steps[counter]["fields"].get(
                "Action", ""
            )
            test_same = test_same and jira_step_expected == update_steps[counter][
                "fields"
            ].get("Expected Result", "")
            test_same = test_same and jira_step_data == update_steps[counter][
                "fields"
            ].get("Data", "")
            test_same = test_same and jira_step_attachments == update_steps[
                counter
            ].get("attachments", [])
            if not test_same:
                break
        return test_same

    def _verify_customfield_11404_equal(self, jira_data_dict, update_data_dict):
        """
        Compares for equivalence of customfield_11404 from two dictionary contents
        :param jira_data_dict: Data dict read from jira issue
        :param update_data_dict: Data dict to update jira issue
        :return: True if the customfield_11404 value is equal in both dicts
        """
        field_name = "customfield_11404"
        test_same = True
        if not (jira_data_dict.get(field_name) and update_data_dict.get(field_name)):
            test_same = False
        else:
            jira_test_steps = jira_data_dict.get(field_name).steps
            update_steps = update_data_dict.get(field_name, {}).get("steps", [])
            if len(jira_test_steps) != len(update_steps):
                test_same = False
            else:
                test_same &= self._check_test_is_same(jira_test_steps, update_steps)

        return test_same

    def _check_test_requires_update(self, jira_test, test_creation_dict):
        """
        Verifies if the jira test requires to be updated. This saves on test
        update if there is no diff in test contents.
        :param jira_test: Existing jira test issue
        :param test_creation_dict: Dict of data that will be used to verify test contents
        :return: False if the jira_test content do not match with test_creation_dict
        """
        test_same = True
        jira_test_data = jira_test.fields.__dict__
        for field_name in test_creation_dict:
            if field_name in ["description"] and not self._verify_strings_equal(
                jira_test_data, test_creation_dict, field_name
            ):
                test_same = False
                break
            if (
                field_name == "customfield_11400"
                and not self._verify_customfield_11400_equal(
                    jira_test_data, test_creation_dict
                )
            ):
                test_same = False
                break
            if (
                field_name == "customfield_11404"
                and not self._verify_customfield_11404_equal(
                    jira_test_data, test_creation_dict
                )
            ):
                test_same = False
                break
        return test_same

    def _update_test_label(self, test_creation_dict, jira_test):
        """
        Update test label as a special case.
        :param test_creation_dict: Test data dict
        :param jira_test: Jira test issue to update
        :returns: String specifying if label was updated
        """
        label_updated = None
        if "labels" in test_creation_dict:
            labels = test_creation_dict.pop("labels")
            if jira_test.fields.labels != labels and labels:
                str_labels = str(labels).replace("'", '"')
                label_value_dict = json.loads('{"labels": ' + str_labels + "}")
                try:
                    jira_test.update(label_value_dict)
                    label_updated = label_value_dict.get("labels")
                except JIRAError:
                    self.logger.warning("Failed to update test label %s", str_labels)
        return label_updated

    def update_existing_test(
        self, report_test, jira_test, dry_run=False, semaphore=None
    ):
        """
        Updates existing test in Jira.
        :param report_test: The test read from the xml/json report
        :param jira_test: Test found in jira matching report test
        :param dry_run: If True then do not actually update the test but only list
                what will happen if it were actually executed
        :param semaphore: For parallel execution acquire a semaphore before running
        """
        logs = []
        lock = threading.Lock()
        if semaphore:
            semaphore.acquire()

        if not dry_run:
            test_creation_dict = self._create_test_dict(report_test, update_test=True)
            label_updated = self._update_test_label(test_creation_dict, jira_test)
            if label_updated:
                log_info = "'%s' Test label updated. New Labels: %s" % (
                    jira_test.fields.summary,
                    ", ".join(label_updated),
                )
                logs.append(log_info)

            if self._check_test_requires_update(jira_test, test_creation_dict):
                log_info = (
                    "'%s' under '%s' need not be updated as steps match. Test id: '%s'."
                )
                log_info = log_info % (
                    jira_test.fields.summary,
                    self.args.jira_project_id,
                    jira_test.key,
                )
                logs.append(log_info)
            else:
                jira_id_list = self._match_test_link(jira_test, report_test.jira_id)
                try:
                    jira_test.update(**test_creation_dict)
                except JIRAError:
                    log_info = "'%s' under '%s' updated failed. Test has id id '%s'"
                    log_info = log_info % (
                        jira_test.fields.summary,
                        ", ".join(jira_id_list),
                        jira_test.key,
                    )
                    logs.append(log_info)
                    logs.append(traceback.format_exc())
                else:
                    self.update_issue_with_dict(jira_test, self.args.tests_fields)
                    log_info = "'%s' under '%s' is updated and has id '%s'"
                    log_info = log_info % (
                        jira_test.fields.summary,
                        ", ".join(jira_id_list),
                        jira_test.key,
                    )
                    logs.append(log_info)
        else:
            log_info = (
                "'%s' under '%s' would have been updated which has id '%s'. "
                + "This is a DryRun."
            )
            log_info = log_info % (
                jira_test.fields.summary,
                self.args.jira_project_id,
                jira_test.key,
            )
            logs.append(log_info)
        lock.acquire()
        for log in logs:
            self.logger.info(log)
        lock.release()

        if semaphore:
            semaphore.release()

    def _match_test_link(self, jira_test, report_jira_id):
        """
        Checks if the test already matches exactly the same issue types that it should.
        If not then it updates the additional links
        :param jira_test: Jira issue of type test which is updated
        :param report_jira_id: Report Jira id that the new test should link now.
        """
        if not report_jira_id:
            return report_jira_id
        if isinstance(report_jira_id, str):
            report_jira_id = [report_jira_id]
        linked_issues_to_jira_test = [
            link.outwardIssue.key for link in jira_test.fields.issuelinks
        ]
        difference = list(set(report_jira_id) - set(linked_issues_to_jira_test))
        if difference:
            self._add_link_to_jira_issue(jira_test, difference)
        return report_jira_id

    def add_result(
        self, test, jira_test, test_execution_issue, counter=1, uploaded=None
    ):
        """
        Adds results to Jira for ready to upload results tests
        :param test: Test to upload result for
        :param jira_test: Existing Jira test object
        :param test_execution_issue: Test Execution issue object
        :param counter: The number of test result being uploaded
        :param uploaded: Return parameter value for upload function
        """
        if not uploaded:
            uploaded = [True]
        log_info = "%d Uploading: '%s' with Jira-ID: %s" % (
            counter,
            test.name,
            jira_test.key,
        )
        self.logger.info(log_info)
        test_upload_json, test_execution_issue = self._create_json_from_result(
            test, jira_test, test_execution_issue
        )
        query_result = ""
        if not self.args.no_jira_upload:
            query_result = self.xray_api.upload_results_to_xray(test_upload_json)
        if not query_result:
            if not self.args.no_jira_upload:
                self.logger.info("Jira upload disabled by user")
                uploaded[0] = False
            else:
                self.logger.info(
                    "Upload Restricted for Test due to user argument no_jira_upload"
                )
        else:
            if not self.args.xray_testexecution_id:
                self.args.xray_testexecution_id = query_result

    def update_dependencies_to_test_execution(
        self, test_execution_object, classified_dependencies
    ):
        """
        After the results are added this function is used to update test execution
        documentation to list of any dependency variance from GxP qualified dependencies
        :param test_execution_object: Test Execution object from Jira
        :param classified_dependencies: List of dependencies used to execute the tests
        """
        dependencies = self._verify_dependencies(classified_dependencies)
        if dependencies:
            self.xray_api.update_test_execution_gxp_req_details(
                test_execution_object, dependencies
            )

    def _get_test_execution_update_dict(
        self,
        test_execution_start_time,
        tests_first_start_time,
        test_execution_end_time,
        tests_last_end_time,
    ):
        """
        Test execution update dict for times. If the tests were executed
        earlier or ended later then these times can be updated here.
        :param test_execution_start_time: Execution time for test execution
        :param tests_first_start_time: Tests first execution time
        :param test_execution_end_time: Execution time for test execution
        :param tests_last_end_time: Tests last execution time
        :return: Dict of data to update
        """
        update_dict = {}
        if tests_last_end_time > test_execution_end_time:
            update_dict["customfield_11417"] = self.date_time_support.convert_for_jira(
                tests_last_end_time
            )

        if tests_first_start_time < test_execution_start_time:
            update_dict["customfield_11416"] = self.date_time_support.convert_for_jira(
                tests_first_start_time
            )

        return update_dict

    @staticmethod
    def _read_file_dependency_into_gxp_dependencies(file_path, gxp_dependencies):
        """
        Reads requirement file dependency into gxp dependency list
        :param file_path: File to parse containing requirements
        :param gxp_dependencies: GxP dependencies list
        """
        with open(file_path, "r") as file_object:
            read_dependencies = file_object.readlines()
            for read_dependency in read_dependencies:
                if read_dependency.strip():
                    gxp_dependencies.append(read_dependency.strip())

    def _verify_dependencies(self, dependency_list):
        """
        Verifies that the dependencies used for processing the robot tests are within the
        listed set of dependencies
        :param dependency_list: List of dependencies read from the test report
        :return: List of requirements different from GxP qualified requirements
        """
        dir_path = os.path.dirname(os.path.realpath(__file__))
        parent_dir = ntpath.dirname(dir_path)
        dependency_folder = os.sep.join([parent_dir, "dependencies"])
        gxp_dependencies = []
        for root, _, file_names in os.walk(dependency_folder):
            for file_name in file_names:
                file_path = os.sep.join([root, file_name])
                self._read_file_dependency_into_gxp_dependencies(
                    file_path, gxp_dependencies
                )
        return self._compare_dependencies(dependency_list, gxp_dependencies)

    def _compare_dependencies(self, project_dependencies, gxp_dependencies):
        """
        Compares between project and gxp dependencies to ensure if all requirements used are within
        the list of compliant gxp dependencies
        :param project_dependencies: List of dependencies used by project to build the test
        :param gxp_dependencies: GxP dependencies list from the project
        :return: List of requirements different from GxP qualified requirements
        """
        different_requirements = list(set(project_dependencies) - set(gxp_dependencies))
        if different_requirements:
            self.logger.info(
                "Difference in requirements used to run the "
                "tests from qualified GxP requirements"
            )
            for requirement in different_requirements:
                self.logger.info(requirement)
        return different_requirements

    def _create_json_from_result(self, test, jira_test, test_execution_issue=None):
        """
        Creates json payload to upload results into XRay. Uploads results
        to single Test Execution. For multiple test execution uploads user
        may have to execute the uploader again
        :param test: Test read from report to be uploaded
        :param jira_test: Existing Jira test object
        :param test_execution_issue: Test execution issue
        :returns: Json format to upload the test results
        """
        # Generating PDF Report for Test
        pdf_report_generator = ReportGenerator(test, jira_test, self.args)
        pdf_report_generator.generate_all_pdf_reports()
        self.xray_api = XrayApiRequests(test, jira_test, self.args)
        if not test_execution_issue and self.args.xray_testexecution_id:
            test_execution_issue = self.jira.issue(self.args.xray_testexecution_id)
        return self.xray_api.build_test_execution_payload(test_execution_issue)


def print_jira_mapping():
    """
    Prints Jira field mapping that can be then used to update the scripts
    depending on them.
    """
    user_args = UserArguments()
    args = user_args.parse_arguments()
    jira_args = JiraHandler(args)
    for field_value, field_name in jira_args.jira_field_mapping.items():
        print(field_name, ":", field_value)


if __name__ == "__main__":
    print_jira_mapping()
