*** Settings ***
Documentation     SQL ALCHEMY TESTS
Suite Setup  	  Connect to orm
Suite Teardown    Tear Down
Force Tags        AFND-0000	 SQL_ALCHEMY_TESTS	TESTS
Resource          Tests/Support.robot

*** Test Cases ***

SQL_ALCHEMY_TESTS_01 I query a table to return multiple columns with a condition
    [Documentation]    Check query can return multiple columns
    [Tags]    SQL_ALCHEMY_TESTS_01
    Given I return 10 ${column_names} records from ${schema}.GENEALOGYBASE with conditions MATNR_IN = ${matnr}

SQL_ALCHEMY_TESTS_02 I query a table to return 1 columns with a condition
    [Documentation]    Check query can return 1 column
    [Tags]    SQL_ALCHEMY_TESTS_02
    Given I return 25 AUFNR records from ${schema}.GENEALOGYBASE with conditions MATNR_IN = ${matnr}

SQL_ALCHEMY_TESTS_03 I query a table to return multiple columns with multiple conditions
    [Documentation]    Check query can return multiple columns with multiple conditions
    [Tags]    SQL_ALCHEMY_TESTS_02
    Given I return 1 ${column_names} records from ${schema}.GENEALOGYBASE with conditions MATNR_IN = ${matnr} AND BATCH_IN = '${batch_number}'

SQL_ALCHEMY_TESTS_04 I count a table with one condition
    [Documentation]    Check query can count table with one condition
    [Tags]    SQL_ALCHEMY_TESTS_04
    Given I count ${schema}.GENEALOGYBASE with conditions AUFNR = '${aufnr}'

SQL_ALCHEMY_TESTS_05 I count a table with multiple conditions
    [Documentation]    Check query can count table with multiple conditions
    [Tags]    SQL_ALCHEMY_TESTS_05
    Given I count MDH.BTCH with conditions MATL_ID = ${matl_id} AND BTCH_NUM = '${batch_number2}'

SQL_ALCHEMY_TESTS_06 I return column descriptions from table
    [Documentation]    Check query can return descriptions of a table
    [Tags]    SQL_ALCHEMY_TESTS_06
    Given I return description from table BTCH in schema MDH

SQL_ALCHEMY_TESTS_07 Test Join with clause and condition
    [Documentation]    Check query can return results from a join condition
    [Tags]    SQL_ALCHEMY_TESTS_07
    Given I return 10 join tables MDH.BTCH with dbo.BTCH_GENEALOGY for ${column_names_join} on clause ${clause} with condition BTCH_NUM like 'R%'

SQL_ALCHEMY_TESTS_08 Test Join with clause and condition can return count
    [Documentation]    Check query can return count for column from a join condition
    [Tags]    SQL_ALCHEMY_TESTS_08
    Given I distinct count join tables MDH.BTCH with dbo.BTCH_GENEALOGY on clause ${clause2} with condition BTCH_NUM like 'R%'

SQL_ALCHEMY_TESTS_09 Test Join with clause and condition can return count
    [Documentation]    Check query can return count for column from a join condition with empty condition
    [Tags]    SQL_ALCHEMY_TESTS_09
    Given I distinct count join tables MDH.BTCH with dbo.BTCH_GENEALOGY on clause ${clause} with condition ${EMPTY}

SQL_ALCHEMY_TESTS_10 I query a table to return multiple columns with all columns
    [Documentation]    Check query can return all columns with multiple conditions
    [Tags]    SQL_ALCHEMY_TESTS_10
    Given I return 5 ${all_columns} records from ${schema}.GENEALOGYBASE with conditions MATNR_IN = ${matnr} AND BATCH_IN = '${batch_number}'

SQL_ALCHEMY_TESTS_11 I query a table to return multiple columns with multiple conditions
    [Documentation]    Check query can return multiple columns with multiple conditions
    [Tags]    SQL_ALCHEMY_TESTS_11
    Given I return 10 ${column_names_LIMSResults} records from Stage.LIMSResults with conditions SITE like 'Leiden%'

Tear Down
	Write to file	TEST-0000-SQL_ALCHEMY
	Run Keywords	Disconnect ORM
