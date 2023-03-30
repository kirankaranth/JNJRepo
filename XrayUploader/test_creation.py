"""
Handles creation of Test in XRay.
"""
import variables
import xray_logger_handler
from thread_execution import ThreadExecution


class TestCreation:
    """
    Creates test in XRay.
    """

    def __init__(self, test_classification, args, jira):
        """
        Initializes TestCreation with classified tests
        :param test_classification: dict of format:
            {classification: {test_from_report: jira linked test}}
        :param args: User defined arguments to support test creation
        :param jira: Object of class Handler.jira_support.JiraHandler
        """
        self.logger = xray_logger_handler.setup()
        self.test_classification = test_classification
        self.args = args
        self.jira = jira

    def process_test_creation(self):
        """
        Process creation of tests based on classification
        """
        self.logger.info("Creating Tests:")
        self._display_existing_tests()
        self._process_creation_of_new_tests()
        self._process_update_of_changed_tests()

    def _display_existing_tests(self):
        """
        Displays tests which already exist and need not be recreated.
        These tests have a classification of "Exact Test Found"
        """
        existing_tests = self.test_classification.get(
            variables.TEST_MATCHES_EXACTLY, []
        )
        if existing_tests:
            self.logger.info("Existing Tests: %d", len(existing_tests))
        for test_count, test in enumerate(existing_tests):
            jira_test = existing_tests[test]
            self.logger.info(
                "%d: '%s' test already exists in issues '%s' as id '%s'",
                test_count + 1,
                jira_test.fields.summary,
                test.jira_id,
                jira_test.key,
            )

    def _process_creation_of_new_tests(self):
        """
        Processes creation of new tests in Jira XRay based on classification
        """
        new_tests = self.test_classification.get(variables.TEST_NOT_FOUND, [])
        if new_tests:
            self.logger.info("Creating New Tests.")
            self.logger.info("Total tests: %d", len(new_tests))
        thread_executor = ThreadExecution()
        thread_semaphore = thread_executor.get_semaphore()
        for test_count, test in enumerate(new_tests):
            if self.args.dryrun:
                self.logger.info(
                    "%d: '%s' created under '%s' as id '%s'",
                    test_count + 1,
                    test.name,
                    test.jira_id,
                    "DryRun. Test Not Created.",
                )
            else:
                thread_executor.add_to_threads(
                    self.jira.create_test, test, semaphore=thread_semaphore
                )
        thread_executor.start_and_join_threads()

    def _process_update_of_changed_tests(self):
        """
        Processes update of existing tests which may have steps changed or meta data
        information update in the next upload.
        """
        existing_tests = self.test_classification.get(variables.TEST_NAME_FOUND, [])
        if existing_tests:
            self.logger.info("Update required for Tests.")
            self.logger.info("Total tests for update: %d", len(existing_tests))
        thread_executor = ThreadExecution()
        thread_semaphore = thread_executor.get_semaphore()
        for test_count, test in enumerate(existing_tests):
            jira_test = existing_tests[test]
            if jira_test.fields.status.name == "Completed":
                update_message = (
                    "%d Test '%s' id '%s' has status Completed and cannot"
                    + " be updated. Please Reopen to update this test."
                )
                self.logger.info(
                    update_message,
                    test_count + 1,
                    jira_test.fields.summary,
                    jira_test.key,
                )
            else:
                thread_executor.add_to_threads(
                    self.jira.update_existing_test,
                    test,
                    jira_test,
                    dry_run=self.args.dryrun,
                    semaphore=thread_semaphore,
                )
        thread_executor.start_and_join_threads()
