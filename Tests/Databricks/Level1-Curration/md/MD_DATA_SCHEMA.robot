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
   [Documentation]   Validate that CDL L1 has the correct number of columns
   [Tags]  JEKT-205_01
   [Template]    Validate that table has the correct number of columns
   ${COLUMN_COUNTS}     ${TABLES}   table

JEKT-0205_02 SIT Test to check that the CDL L1 is created in the correct location
    [Documentation]    I check that the underlying files are in the correct ADLS location
    [Tags]   JEKT-205_02
    [Template]    Validate that the tables underlying files are in the correct ADLS location
    ${TABLES}    ${table_edm_location}     table

JEKT-0205_03 SIT Test to check that the CDL L1 datatypes
    [Documentation]  Validate that the columns are of correct datatype
    [Tags]   JEKT-205_03
    [Template]    Validate that the columns are of correct datatype
    ${TABLES}    ${column_data_Types}     table