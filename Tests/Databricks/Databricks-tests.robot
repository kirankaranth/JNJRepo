*** Settings ***

Force Tags          AFND-2435	DATABRICKS
Suite Setup  		Set Up DBFS
Suite Teardown  	Disconnect from Databricks
Test Timeout
Resource		    Tests/Support.robot


*** Test Cases ***


#AFND-2435_01 I check that the target tables are less than 32 characters in length
#    [Documentation]   Check that the target tables are less than 32 characters in length
#    [Tags]   AFND-2435_01
#    [Timeout]
#    Given I check that the target tables are the correct length
#
#AFND-2435_02 I check the uniqueness of the primary keys for ingested tables in schema
#    [Documentation]   I run ingestion tests for all tables in target for a database
#    [Tags]   AFND-2435_02
#    [Timeout]
#    Given I check the uniqueness of the primary keys for ingested tables
#
#AFND-2435_03 I check tables exist in target database with correct table names
#    [Documentation]   I check tables exist in target database with correct table names
#    [Tags]   AFND-2435_03
#    [Timeout]
#    Given I check tables exist in target database with correct table names
#
#AFND-2435_04 I check each table to see that the table files are in the correct folder
#    [Documentation]   I check each table to see that the table files are in the correct folder
#    [Tags]   AFND-2435_04
#    [Timeout]
#    Given I check each table in schema to see that the table files are in the correct folder
#
#AFND-2435_05 I compare counts of source and target
#    [Documentation]   I compare counts of source and target
#    [Tags]   AFND-2435_05
#    [Timeout]
#    Given I compare counts of source and target
#
#AFND-2435_06 I compare the difference in records between source and target
#    [Documentation]   I compare the difference in records between source and target
#    [Tags]   AFND-2435_06
#    [Timeout]
#    Given I compare the difference in records between source and target

#AFND-2435_07 I compare first 100 records from source and target
#    [Documentation]   I compare first 100 records from source and target
#    [Tags]   AFND-2435_08
#    Given I get difference in records between source and target

AFND-2435_08 I parse flat file
    [Documentation]   I parse flat file
    [Tags]   AFND-2435_08   setupstg
    Given I create hive views from flat files in blob for all tables in database

AFND-2435_09 I parse csv file
    [Documentation]   I parse csv file
    [Tags]   AFND-2435_09
    Given I create hive views from csv files in blob for all tables in database

AFND-3785-file I parse parquet file
    [Documentation]   I parse parquet file
    [Tags]   AFND-3785
    Given I create hive views from parquet files in blob for all tables in database
