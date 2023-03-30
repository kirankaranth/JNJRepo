*** Settings ***
Documentation       Tests for checking L1 curation - Purchase Order - MD_DATA_SCHEMA  Jira-ID:JEKT-205.
Force Tags          JEKT-205   POC Release   MD_DATA_SCHEMA    Regression
Suite Setup  		Connect to CDL Databricks
Suite Teardown  	Disconnect from Databricks
Test Setup          Setup Test
Resource		    Tests/Support.robot
Resource             ../Support-Level1MultiTables.robot
Variables           Data/data_level_1/variable_data_schema_cdl_l1.py

*** Test Cases ***
JEKT-0205_01 SIT Test to check that the CDL L1 table has the correct number of columns
   [Documentation]   Validate that CDL L1 has the correct number of columns  ${table_number_of_columns}
   [Tags]  JEKT-205_01
   Given I have access to Databricks database  ${table}
   When I check that the requirements are implemented correctly
   Then I expect that the target table contains required number of columns  ${table_number_of_columns}  ${tables}

JEKT-0205_02 SIT Test to check that the CDL L1 is created in the correct location
    [Documentation]    I check that the underlying files are in the correct ADLS location ${table_edm_location}
    [Tags]   JEKT-205_02
    Given I have access to Databricks database    ${table}
    When I check that the requirements are implemented correctly
    Then I expect that the EDM location is correct  ${table_edm_location}  ${tables}

JEKT-0205_03 SIT Test to check that the CDL L1 datatypes
    [Documentation]  Validate that the columns are of correct datatype for ${column_data_Types}
    [Tags]   JEKT-205_03
    Given I have access to Databricks database    ${table}
    When I check that the requirements are implemented correctly
    Then I expect that columns are of the correct datatype     ${tables}   ${column_data_Types}