"""
Contains hard coded constant definition for XRay uploader functionality
"""

# Classifications
TEST_NOT_FOUND = "Not Found"
TEST_NAME_FOUND = "Test Name Found"
TEST_MATCHES_EXACTLY = "Exact Test Found"

# Jira Issue types
TEST_ISSUE_TYPE = "Test"
TEST_TYPE_MAPPING = {"value": "Manual", "id": "12900"}
TEST_COMPLETED_STATUS = "Completed"

# Dependencies used for executing tests
DEPENDENCIES = "dependencies"

# XRay upload constant strings
SUMMARY = "Test Execution - "
EXECUTION_DESCRIPTION = (
    "This execution is automatically created when "
    + "importing execution results from an external source. Updated: "
)

# XRay upload api variables
XRAY_EXECUTION_IMPORT_API = "/rest/raven/1.0/import/execution/"
API_HEADERS = {"Content-Type": "application/json"}

# Report version generated for PDF report
REPORT_VERSION = "1.0"
REPORT_OUTPUT_FOLDER = "temp_xray_reports"

# Test Execution date support
TEST_EXECUTION_FIRST_START = "first_start"
TEST_EXECUTION_LAST_END = "last_end"
