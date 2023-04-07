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
    [Documentation]   Validate that the curated table: ${system}.${table} contains unique primary keys
    [Tags]   JEKT-24_01
    [Template]    Validate that the curated table contains unique primary keys
    ${table}    ${PRIMARY_KEYS}      table

JEKT-0024_02 SIT ${table} Test that the columns contains a date in UTC
    [Documentation]   Validate that the columns marked as Follow UTC for ${system}.${table} contain a date in UTC format only
    [Tags]        JEKT-24_02
    [Template]    Validate that the columns marked as Follow UTC contain a date in UTC format
    ${source_system}        ${UTC_COLUMNS}          ${table}        table

JEKT-0024_03 ${table} Test that specified columns for source system contain all null values
    [Documentation]   Validate that for ${system}.${table} columns contains all nulls
    [Tags]      JEKT-24_03
    [Template]    Validate that for all listed columns contains all nulls values
    ${NULL_SOURCES}       ${NULL_COLUMNS}      ${table}     table

JEKT-0024_04 ${table} Test that specified columns for source systems contain all whitespace removed
    [Documentation]  Validate that for ${system}.${table} columns contain all whitespace removed as per trim requirement
    [Tags]       JEKT-24_04
    [Template]   Validate that trim implemented correctly for all listed columns per source
    ${WHITESPACE_SOURCES_1}       ${WHITESPACE_COLUMNS_1}      ${table}    table
    ${WHITESPACE_SOURCES_2}       ${WHITESPACE_COLUMNS_2}      ${table}    table

JEKT-0024_05 SIT ${table} Test that specified columns for source systems contain predefined values
    [Documentation]    Validate that ${system}.${table}  columns contain predefined values
    [Tags]    JEKT-24_05    JEKT-24_T
    [Template]    Validate that columns contain all predefined values for list of sources
    ${source_system}    ${COLUMNS_WITH_PREDEFINED_VALUES_PER_SOURCE_EXTEND}   ${table}    table