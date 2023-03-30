"""
Adds constants to deal with json parser information.
"""
import ntpath
import os


def get_version():
    """
    Fetches version number for the release from file __version__ in the root of the project
    :return: Version of the project
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    level = 4
    version_number = "1.0"
    for _ in range(level):
        dir_path = ntpath.dirname(dir_path)
    file_path = os.sep.join([dir_path, "__version__"])
    if os.path.exists(file_path):
        version_number = open(file_path, "r").read().strip()
    return version_number


# Json Creator constants
CREATE_TEST = "Create_Test"
ADD_RESULT = "Add_Result"

# JSON Schema variables
# Suite fields
PROJECT_FIELD = "project"
SUITE_FIELD = "suite"
AUTOMATION_TEST_TYPE = "Manual"
AUTOMATION_TEST_PRIORITY = "Medium"
JIRA_ID_FIELD = "jiraIds"
TEST_TYPE = "testType"
TEST_LABELS = "testLabels"
TEST_PRIORITY = "testPriority"
TEST_AFFECTS_VERSION = "testAffectsVersions"
TEST_FIX_VERSION = "testFixVersions"
TEST_COMPONENTS = "testComponents"
TEST_APPROVAL_CATEGORY = "testApprovalCategory"
TEST_ASSIGNEE = "testAssignee"
TEST_REPORTER = "testReporter"
TEST_COMMENT = "testComment"
TEST_ENVIRONMENT_INFO = "testEnvironmentInformation"
TEST_CATEGORY = "testCategory"
TEST_SET_FOR_TESTS = "testSet"
TEST_PLAN_FOR_TESTS = "testPlan"
# Test fields
TESTS_SECTION = "tests"
TESTS_NAME = "testName"
TESTS_DOCUMENTATION = "testDocumentation"
# Step fields
STEPS_SECTION = "steps"
STEPS_NUMBER = "stepNumber"
STEPS_DESCRIPTION = "stepDescription"
STEPS_EXPECTED = "stepExpected"
STEPS_VARIABLES = "stepData"
STEPS_DATA_DRIVEN = "stepDataDriven"

# Execution result fields
SE_INFO = "info"
SE_SUMMARY = "summary"
version = get_version()
SE_SUMMARY_DEFAULT = "Execution of automated tests for release v" + version
SE_DESCRIPTION = "description"
SE_DESCRIPTION_DEFAULT = (
    "This execution is automatically created when importing"
    + " execution results from an external source"
)
SE_START_DATE = "startDate"
