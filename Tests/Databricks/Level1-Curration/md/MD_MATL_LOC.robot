*** Settings ***
Documentation       Tests for checking L1 curation - Material - MD_MATL_LOC  Jira-ID: JEKT-186.
Force Tags          JEKT-186   CDL_L1_2023_R14   MD_MATL_LOC
Suite Setup  		Connect to CDL Databricks
Test Teardown    	Disconnect from Databricks
Test Setup          Setup Test
Resource		    Tests/Support.robot
Resource            ../Support-Level1Updated.robot
Variables           Data/data_level_1/variable_md_matl_loc.py

*** Test Cases ***
JEKT-0186_01 SIT ${table} Test to check that the table has the correct number of columns
    [Documentation]   Validate that the target table: ${system}.${table} contains required number of columns     ${COLUMN_COUNT}
    [Tags]   JEKT-0186_01
    [Template]    Validate that the target table contains required number of columns
    ${table}        ${COLUMN_COUNT}

JEKT-0186_02 SIT ${table} Test to check the uniqueness of the primary keys
    [Documentation]   Validate that the curated table: ${system}.${table} contains unique primary keys      ${PRIMARY_KEYS}
    [Tags]   JEKT-0186_02
    [Template]    Validate that the curated table contains unique primary keys
    ${table}    ${PRIMARY_KEYS}      table

JEKT-0186_03 SIT ${table} Test the datatypes of the columns
    [Documentation]   Validate that the columns are of correct datatype for ${system}.${table}      ${COLUMNS_AND_DATATYPES}
    [Tags]   JEKT-0186_03
    [Template]    Validate that columns are of the correct datatype
    ${table}

JEKT-0186_04 SIT ${table} Test that the EDM is created in the correct location
    [Documentation]   Validate that the ${system}.${table}'s underlying files are in the correct ADLS location      ${table_location}
    [Tags]   JEKT-0186_04
    [Template]    Validate that EDM location is correct
    ${table}        ${table_location}

JEKT-0186_05 SIT ${table} Test that the columns contains a date in UTC
    [Documentation]   Validate that the columns marked as Follow UTC for ${system}.${table} contain a date in UTC format only
    [Tags]        JEKT-0186_05
    [Template]    Validate that the columns marked as Follow UTC contain a date in UTC format
    ${source_system}        ${UTC_COLUMNS}          ${table}        table

JEKT-0186_06 SIT ${table} Test that the columns contains a date in UTC or contains nulls as per filter requirement
    [Documentation]   Validate that the columns for ${system}.${table} with sources     ${ALL_UTC_WITH_NULL_SOURCES}    and column  ${UTC_COLUMNS_WITH_NULL}
    ...               Follow UTC or contains nulls as per filter requirement
    ...               Filter pattern: "case when 'column_name' = '00000000' then null else to_timestamp('column_name', ""yyyyMMdd"") end - Follow UTC"
    [Tags]   JEKT-0186_06
    [Template]    Validate that columns Follow UTC Format or contain nulls as per the filter requirement
    ${ALL_UTC_WITH_NULL_SOURCES}        ${UTC_COLUMNS_WITH_NULL}       ${table}    table

JEKT-0186_07 SIT ${table} Test that specified columns for source systems contain all whitespace removed
    [Documentation]  Validate that for ${system}.${table} columns contain all whitespace removed as per trim requirement.
    [Tags]   JEKT-0186_07
    [Template]   Validate that trim implemented correctly for all listed columns per source
    ${ALL_WHITESPACE_SOURCES_jet}       ${WHITESPACE_COLUMNS_jet}      ${table}    table
    ${ALL_WHITESPACE_SOURCES_jsw}       ${WHITESPACE_COLUMNS_jsw}      ${table}    table
    ${ALL_WHITESPACE_SOURCES_mtr}       ${WHITESPACE_COLUMNS_mtr}      ${table}    table
	${ALL_WHITESPACE_SOURCES_gmd}       ${WHITESPACE_COLUMNS_gmd}      ${table}    table
	${ALL_WHITESPACE_SOURCES_X}         ${WHITESPACE_COLUMNS_X}        ${table}    table
	${ALL_WHITESPACE_SOURCES_Y}         ${WHITESPACE_COLUMNS_Y}        ${table}    table
	${ALL_WHITESPACE_SOURCES_deu}       ${WHITESPACE_COLUMNS_deu}      ${table}    table
	${ALL_WHITESPACE_SOURCES_sjd}       ${WHITESPACE_COLUMNS_sjd}      ${table}    table
	${ALL_WHITESPACE_SOURCES_djd}       ${WHITESPACE_COLUMNS_djd}      ${table}    table
	${ALL_WHITESPACE_SOURCES_jem}       ${WHITESPACE_COLUMNS_jem}      ${table}    table
	${ALL_WHITESPACE_SOURCES_jes}       ${WHITESPACE_COLUMNS_jes}      ${table}    table
	${ALL_WHITESPACE_SOURCES_bbn}       ${WHITESPACE_COLUMNS_bbn}      ${table}    table

#JEKT-0186_07a SIT ${table} Test that specified columns for source systems contain all whitespace removed
#    [Documentation]  Validate that for ${system}.${table} columns contain all whitespace removed as per trim requirement.
#    [Tags]   JEKT-0186_07a
#    [Template]   Validate that trim implemented correctly for all listed columns per source
#    ${ALL_NULL_SOURCES_X}       ${NULL_COLUMNS_X}      ${table}    table

JEKT-0186_08 SIT ${table} Test that specified columns for source systems contain all nulls
    [Documentation]   Validate that for ${system}.${table} columns contains all nulls
    [Tags]       JEKT-0186_08
    [Template]  Validate that Populate as null for all listed columns implemented correctly per source
    ${ALL_NULL_SOURCES_jet}       ${NULL_COLUMNS_jet}      ${table}    table
    ${ALL_NULL_SOURCES_jsw}       ${NULL_COLUMNS_jsw}      ${table}    table
    ${ALL_NULL_SOURCES_mtr}       ${NULL_COLUMNS_mtr}      ${table}    table
    ${ALL_NULL_SOURCES_bw2}       ${NULL_COLUMNS_bw2}      ${table}    table
    ${ALL_NULL_SOURCES_gmd}       ${NULL_COLUMNS_gmd}      ${table}    table
    ${ALL_NULL_SOURCES_deu}       ${NULL_COLUMNS_deu}      ${table}    table
    ${ALL_NULL_SOURCES_jem}       ${NULL_COLUMNS_jem}      ${table}    table
    ${ALL_NULL_SOURCES_jes}       ${NULL_COLUMNS_jes}      ${table}    table
    ${ALL_NULL_SOURCES_sjd}       ${NULL_COLUMNS_sjd}      ${table}    table
	${ALL_NULL_SOURCES_djd}       ${NULL_COLUMNS_djd}      ${table}    table
    ${ALL_NULL_SOURCES_X}         ${NULL_COLUMNS_X}        ${table}    table
    ${ALL_NULL_SOURCES_bba}       ${NULL_COLUMNS_bba}      ${table}    table
    ${ALL_NULL_SOURCES_bbn}       ${NULL_COLUMNS_bbn}      ${table}    table

JEKT-0186_09 SIT ${table} Test that specified columns for source systems contain predefined values
    [Documentation]    Validate that ${system}.${table}  columns contain predefined values  for given column    ${COLUMNS_WITH_PREDEFINED_VALUES}
    [Tags]   JEKT-0186_09
    [Template]    Validate that columns contain all predefined values for list of sources
    ${source_system}    ${COLUMNS_WITH_PREDEFINED_VALUES}   ${table}    table

