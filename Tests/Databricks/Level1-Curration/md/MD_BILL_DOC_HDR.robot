*** Settings ***
Documentation       Tests for checking L1 curation - DART L1 Billing - MD_BILL_DOC_HDR Jira-ID: JEKT-226.
Force Tags          JEKT-226   Release 2   MD_BILL_DOC_HDR
Suite Setup         Connect to CDL Databricks
Suite Teardown      Disconnect from Databricks
Test Setup          Setup Test
Resource            Tests/Support.robot
Resource            ../Support-Level1Updated.robot
Variables           Data/data_level_1/variable_md_bill_doc_hdr.py

*** Test Cases ***
JEKT-0226_01 SIT ${table} Test to check the uniqueness of the primary keys
    [Documentation]   Validate that the curated table: ${system}.${table} contains unique primary keys
    [Tags]   JEKT-226_01
    [Template]    Validate that the curated table contains unique primary keys
    ${table}    ${PRIMARY_KEYS}      table

JEKT-0226_02 SIT ${table} Test that the columns contains a date in UTC
    [Documentation]   Validate that the columns marked as Follow UTC for ${system}.${table} contain a date in UTC format only
    [Tags]   JEKT-226_02
    [Template]    Validate that the columns marked as Follow UTC contain a date in UTC format
    ${source_system}        ${UTC_COLUMNS}          ${table}        table

JEKT-0226_03 SIT ${table} Test that the columns contains a date in UTC or contains nulls as per filter requirement
    [Documentation]   Validate that the columns for ${system}.${table} are Follow UTC or contains nulls as per filter requirement
    [Tags]   JEKT-226_03
    [Template]    Validate that columns Follow UTC Format or contain nulls as per the filter requirement
    ${source_system}    ${UTC_COLUMNS_WITH_NULL}   ${table}     table

JEKT-0226_04 SIT ${table} Test that specified columns for source systems contain all nulls
    [Documentation]   Validate that for ${system}.${table} columns contains all nulls
    [Tags]       JEKT-226_04
    [Template]  Validate that Populate as null for all listed columns implemented correctly per source
    ${NULL_SOURCES_1}     ${NULL_COLUMNS_1}    ${table}    table
    ${NULL_SOURCES_2}     ${NULL_COLUMNS_2}    ${table}    table
    ${NULL_SOURCES_3}     ${NULL_COLUMNS_3}    ${table}    table

JEKT-0226_05 SIT ${table} Test that specified columns for source systems contain all whitespace removed
    [Documentation]  I check that for ${system}.${table} columns contain all whitespace removed as per trim requirement.
    [Tags]       JEKT-226_05
    [Template]   Validate that trim implemented correctly for all listed columns per source
    ${WHITESPACE_SOURCES_1}     ${WHITESPACE_COLUMNS_1}    ${table}    table
    ${WHITESPACE_SOURCES_2}     ${WHITESPACE_COLUMNS_2}    ${table}    table
    ${WHITESPACE_SOURCES_3}     ${WHITESPACE_COLUMNS_3}    ${table}    table
    ${WHITESPACE_SOURCES_4}     ${WHITESPACE_COLUMNS_4}    ${table}    table
    ${WHITESPACE_SOURCES_5}     ${WHITESPACE_COLUMNS_5}    ${table}    table
