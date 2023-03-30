*** Settings ***
Documentation    Tests for checking folder location on ADLS Jira-ID: AGON-3088.
Force Tags          AGON-3088   R8
Suite Setup  		Connect to CDL Databricks
Suite Teardown  	Disconnect from Databricks
Test Setup          Setup Test
Resource		    Tests/Support.robot
Resource            ../Support-Level1.robot
Variables           Data/data_level_1/variable_util_tests.py
*** Test Cases ***
AGON-3088_01 SIT Test that the EDMs are created in the correct location
    [Documentation]   I check that for each EDM the underlying files are in the correct ADLS location
    [Tags]   AGON-2499_06
    Given I check the location of the EDM's in ADLS
    Then I expect that the EDM's location are correct

*** Keywords ***
I check the location of the EDM's in ADLS
    The EDM's location in ADLS is checked

The EDM's location in ADLS is checked
    log to console  PASS

I expect that the EDM's location are correct
    The location of the EDM's are correct

The location of the EDM's are correct
    set test variable   ${test_status}  PASS

    FOR     ${table}   IN  @{TABLES_AND_LOCATIONS}
        ${expected_location}=     get from dictionary     ${TABLES_AND_LOCATIONS}   ${table}
        ${actual_location}=   return location of table on dbfs   ${table}   l1
        run keyword     add result to report   Actual Result: ${actual_location}
        run keyword     add result to report   Expected Result: ${expected_location}\n
        run keyword if  "${actual_location}"=="${expected_location}"   add result to report     Test Passed: Table location was equal to expected location for ${table}\n   ELSE    add result to report    Test Failed: Table location was not equal to expected location for ${table}\n
        run keyword if  "${actual_location}"!="${expected_location}"     set test variable   ${test_status}  FAIL
    END

    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 table was in the incorrect location on ADLS  ELSE    add result to report  Test Passed: All table in correct location on ADLS
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
    Teardown Test
