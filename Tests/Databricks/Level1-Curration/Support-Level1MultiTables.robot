*** Settings ***
Resource    Tests/Databricks/Support-Databricks.robot
Library       Library/Utils.py
*** Variables ***
${execution_status}        PASS
${data}               PASS
${Prod_env_loc}       prod
${Prod_env}           prd

*** Keywords ***
I have access to Databricks database
    [Arguments]      ${databricks_name}
    Successfully logging in to the Databricks platform     ${databricks_name}
    log    Connected to ${databricks_name}

Successfully logging in to the Databricks platform
    [Arguments]       ${databricks_name}
    Run keyword if    '${databricks_name}'=='view'    Check the user has successful connection to Databricks views
    Run keyword if    '${databricks_name}'=='table'   Check the user has successful connection to Databricks
    set suite variable    ${databricks_name}    ${databrick_url}

I check that the requirements are implemented correctly
    Check that requirements are implemented correctly

Check that requirements are implemented correctly
    Log    Check is performed

Check the user has successful connection to Databricks
    Run keyword if      ${Connection}==True         Log to console  CONNECTED TO DATABRICKS
    set test variable   ${databrick_connection_Type}   Table
    Run keyword if      ${Connection}==False        Reconnect to Databrick or Reconnect to Databrick views

Check the user has successful connection to Databricks views
    run keyword if      ${Connection}==True         log to console  CONNECTED TO DATABRICKS VIEWS SUCCESSFULLY
    set test variable   ${databrick_connection_Type}   View
    run keyword if      ${Connection}==False     Reconnect to Databrick or Reconnect to Databrick views

Reconnect to Databrick or Reconnect to Databrick views
    Log to console  CONNECTION TO DATABRICKS FAILED
    Run Keyword If      ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    Run keyword if      ${Connection}==True      Log to console  RECONNECTED SUCCESSFULLY

I expect that the target table contains required number of columns
    [Arguments]     ${COLUMN_COUNTS}     ${TABLES}
    Validate the target table contains required number of columns       ${COLUMN_COUNTS}     ${TABLES}
    log   Required number of columns : ${COLUMN_COUNTS}


Validate the target table contains required number of columns
    [Arguments]     ${COLUMN_COUNTS}     ${TABLES}
    FOR     ${result}   IN  @{TABLES}
    ${table}=  Set Variable    ${result}
    ${number_of_columns}=     get from dictionary     ${COLUMN_COUNTS}    ${table}
    Validate that the target table contains required number of columns    ${number_of_columns}  ${table}
    END
    log test to report per step     ${jira_id_tag}

Validate that the target table contains required number of columns
    [Arguments]     ${number_of_columns}    ${table}
    ${columns}=   get columns from table  ${system}.${table}
    ${column_count}=   get length  ${columns}
    Add result to report   Validate that the target table: ${system}.${table} contains required number of columns
    Add result to report   Expected columns number : ${number_of_columns}
    Add result to report   Actual columns number : ${column_count}
    ${status}       Run Keyword And Return Status    Should be equal as numbers     ${column_count}     ${number_of_columns}
    Run keyword if  ${status}==True       Add result to report    Test PASSED : The ${system}.${table} contains required columns number ${number_of_columns}
    Run keyword if  ${status}==False      Add result to report    !Test FAILED: The ${system}.${table} expected ${number_of_columns} columns but contains ${column_count} columns
    Run keyword if  ${status}==False      Set test variable    ${execution_status}    FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    Add result To report    ${\n}
    Log to console   Actual Colums: ${column_count} ${table}
    Log to console   Expected Colums: ${number_of_columns} ${table}

I expect that table contains only unique primary keys
    Validate Uniqueness Of The Primary Keys Are As Expected For The Curated Table    ${PRIMARY_KEYS}
    Log    Primary key list:${PRIMARY_KEYS}

Fail test and Log error
    [Arguments]            ${var}
    Set test variable      ${execution_status}  FAIL
    Add result to report   !Test Failed, ${var} not following requirements
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS

I expect that columns are of the correct datatype
    [Arguments]     ${TABLES}    ${COLUMNS_AND_DATATYPES}
    validate columns for multiple tables are of the correct datatype    ${TABLES}    ${COLUMNS_AND_DATATYPES}
    Log     Checked columns and datatypes: ${COLUMNS_AND_DATATYPES}

validate columns for multiple tables are of the correct datatype
    [Arguments]      ${TABLES}   ${ALL_COLUMNS_AND_DATATYPES}
    FOR     ${result}   IN  @{TABLES}
    ${table}=  Set Variable    ${result}
    ${COLUMNS_AND_DATATYPES}=     get from dictionary     ${ALL_COLUMNS_AND_DATATYPES}    ${table}
    Add result to report  Checking for table: ${table}
    Validate that columns are of the correct datatype     ${COLUMNS_AND_DATATYPES}   ${table}
    Log     Checked columns and datatypes: ${ALL_COLUMNS_AND_DATATYPES}
    END
    log test to report per step     ${jira_id_tag}

Validate that columns are of the correct datatype
    [Arguments]    ${COLUMNS_AND_DATATYPES}    ${target}
    ${columns_and_datatypes_from_dbfs}=       return_column_descriptions_from_table       ${target}  ${system}
    FOR     ${result}   IN  @{columns_and_datatypes_from_dbfs}
    ${column}=    set variable  ${result}[name]
    ${datatype}=     get from dictionary     ${COLUMNS_AND_DATATYPES}   ${column}
    ${datatype_from_dbfs}=    set variable  ${result}[type]
    ${status}    Run Keyword And Return Status    dictionary should contain item   ${COLUMNS_AND_DATATYPES}   ${column}   ${datatype_from_dbfs}
    Add result to report  Expected: Column ${column} to be of ${datatype} datatype
    Add result to report  Actual: Column ${column} is of ${datatype_from_dbfs} datatype
    Run keyword if   ${status}==False     Set test variable   ${execution_status}  FAIL
    Run keyword if   ${status}==False     Add result to report    FAILED: Column ${column} is of ${datatype_from_dbfs} datatype but required datatype is ${datatype} in ${system}.${target}
    Run keyword if   ${status}==True      Add result to report    PASSED: Column ${column} is of ${datatype} datatype as required in ${system}.${target} \n
    Log to console   Column ${column} is checked
    END
    Run keyword if   '${execution_status}'=='PASS'   Add result to report   \nTest Passed: All Columns are correct datatype.   ELSE   Add result to report   \nTest Failed: At least one datatype was incorrect.


I expect that the EDM location is correct
    [Arguments]    ${table_edm_location}    ${LIST_TABLES}
    Validate EDM location for all tables    ${table_edm_location}    ${LIST_TABLES}
    log   EDM location : ${table_edm_location}


Validate EDM location for all tables
    [Arguments]    ${table_edm_location}    ${TABLES}
    FOR     ${result}   IN  @{TABLES}
    ${table}=  Set Variable    ${result}
    ${table_location}=     get from dictionary     ${table_edm_location}    ${table}
    Validate that EDM location is correct    ${table_location}    ${table}
    END
    log test to report per step     ${jira_id_tag}

Validate that EDM location is correct
    [Arguments]    ${table_location}    ${table}
    ${actual_location}=   return location of table on dbfs   ${table}   ${system}
    ${env}    Run keyword and return status      Should Contain      ${actual_location}    ${Prod_env_loc}
    log     ${actual_location}
    log     ${table_location}
    Run keyword if   ${env}==True     Update location to use proper syntax    ${table_location}
    Add result to report   Actual Result: ${actual_location}
    Add result to report   Expected Result: ${table_location}
    ${status}        Run keyword and return status      should be equal   ${actual_location}   ${table_location}
    run keyword if  ${status}==True     Add result to report   PASSED:- The ${table}'s underlying files are in the correct ADLS location
    run keyword if  ${status}==False    Add result to report   FAILED: Table location was not equal to expected location
    run keyword if  ${status}==False    Set test variable      ${execution_status}    FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    Add result To report    ${\n}

Update location to use proper syntax
    [Arguments]    ${table_location}
    ${temp} =	Fetch From Left      ${table_location}     ${Prod_env_loc}
    ${temp2} =	Fetch From Right     ${table_location}     ${Prod_env_loc}
    ${table_location}    Catenate    ${temp}${Prod_env}${temp2}
    set test variable     ${table_location}
    log       ${table_location}

# Templates
Validate that table has the correct number of columns
    [Arguments]     ${COLUMN_COUNTS}      ${TABLES}    ${databrick_name}
    Given I have access to Databricks database     ${databrick_name}
    When I check that the requirements are implemented correctly
    Then I expect that the target table contains required number of columns     ${COLUMN_COUNTS}     ${TABLES}

Validate that the tables underlying files are in the correct ADLS location
    [Arguments]    ${TABLES}    ${table_edm_location}   ${databrick_name}
    Given I have access to Databricks database       ${databrick_name}
    When I check that the requirements are implemented correctly
    Then I expect that the EDM location is correct       ${table_edm_location}    ${TABLES}

Validate that the columns are of correct datatype
    [Arguments]   ${TABLES}    ${COLUMNS_AND_DATATYPES}     ${databrick_name}
    Given I have access to Databricks database    ${databrick_name}
    When I check that the requirements are implemented correctly
    Then I expect that columns are of the correct datatype    ${TABLES}    ${COLUMNS_AND_DATATYPES}