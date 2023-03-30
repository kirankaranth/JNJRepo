*** Settings ***
Documentation       Tests for checking L1 curation - Purchase Order - MD_SER_NUM_STOCK_SGMNT  Jira-ID:JEKT-24
Force Tags          JEKT-24   POC Release  MD_SER_NUM_STOCK_SGMNT
Suite Setup  		Connect to CDL Databricks
Suite Teardown  	Disconnect from Databricks
Test Setup          Setup Test
Resource		    Tests/Support.robot
Resource             ../Support-Level1Updated.robot
Variables           Data/data_level_1/variable_md_ser_num_stock_sgmnt.py

*** Test Cases ***
JEKT-0024_01 SIT ${table} Test to check the uniqueness of the primary keys
    [Documentation]   Validate that the curated table: ${system}.${table} contains unique primary keys ${PRIMARY_KEYS}
    [Tags]   JEKT-24_01
    Given I have access to Databricks database    table
    When I check that the requirements are implemented correctly
    Then I expect that table contains only unique primary keys

JEKT-0024_02 SIT ${table} Test that the columns contains a date in UTC
    [Documentation]   Validate that the columns marked as Follow UTC for ${system}.${table} contain a date in UTC format only
    ...    Validation is done for column/columns: ${UTC_COLUMNS} for source/sources ${source_system}
    [Tags]        JEKT-24_02
    Given I have access to Databricks database     table
    When I check that the requirements are implemented correctly
    Then I expect that columns which are marked as Follow UTC don't contain null     ${UTC_COLUMNS}     ${source_system}     ${table}
    And I expect that these columns contain a date in UTC format                     ${UTC_COLUMNS}     ${source_system}     ${table}

JEKT-0024_03 ${table} Test that specified columns for source system contain all null values
    [Documentation]   Validate that for ${system}.${table} columns contains all nulls
     ...   \n Validation is made for columns: ${NULL_COLUMNS} for sources: ${NULL_SOURCES}
    [Tags]      JEKT-24_03
    Given I have access to Databricks database    table
    When I check that the requirements are implemented correctly
    Then I expect that listed columns contains all nulls values   ${NULL_SOURCES}       ${NULL_COLUMNS}      ${table}

JEKT-0024_04 ${table} Test that specified columns for source systems contain all whitespace removed for sources ["atl","bba","bbl","bbn","hcs"]
    [Documentation]  Validate that for ${system}.${table} columns contain all whitespace removed for sources : ${WHITESPACE_SOURCES_1}  Columns :  ${WHITESPACE_COLUMNS_1}
    [Tags]       JEKT-24_04
    Given I have access to Databricks database    table
    When I check that the requirements are implemented correctly
    Then I expect that all whitespaces removed from columns    ${WHITESPACE_SOURCES_1}    ${WHITESPACE_COLUMNS_1}   ${table}

JEKT-0024_05 SIT ${table} Test that specified columns for source systems contain predefined values
    [Documentation]    Validate that ${system}.${table}  columns contain predefined values for sources : ${source_system} Predefined values:  ${COLUMNS_WITH_PREDEFINED_VALUES_PER_SOURCE_EXTEND}
    [Tags]    JEKT-24_05    JEKT-24_T
    Given I have access to Databricks database   table
    When I check that the requirements are implemented correctly
    Then I check that columns contain all predefined values for list of sources    ${source_system}    ${COLUMNS_WITH_PREDEFINED_VALUES_PER_SOURCE_EXTEND}   ${table}

JEKT-0024_06 ${table} Test that specified columns for source systems contain all whitespace removed for sources ["mbp","mrs","p01","svs"]
    [Documentation]  Validate that for ${system}.${table} columns contain all whitespace removed for sources : ${WHITESPACE_SOURCES_2}  Columns :  ${WHITESPACE_COLUMNS_2}
    [Tags]       JEKT-24_06
    Given I have access to Databricks database    table
    When I check that the requirements are implemented correctly
    Then I expect that all whitespaces removed from columns    ${WHITESPACE_SOURCES_2}    ${WHITESPACE_COLUMNS_2}   ${table}

