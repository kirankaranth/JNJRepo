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
    [Arguments]     ${number_of_columns}     ${table}
    Validate that the target table contains required number of columns    ${number_of_columns}  ${table}
    Log     Required number of columns: ${number_of_columns}

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
    log test to report per step     ${jira_id_tag}

I expect that table contains only unique primary keys
    Validate Uniqueness Of The Primary Keys Are As Expected For The Curated Table    ${PRIMARY_KEYS}
    Log    Primary key list:${PRIMARY_KEYS}

Validate Uniqueness Of The Primary Keys Are As Expected For The Curated Table
    [Arguments]               ${Primary_Key_List}
    ${Primary_Key_List}       Convert To String     ${Primary_Key_List}
    ${Primary_Key_List}       Remove String    ${Primary_Key_List}    {    }   '    [    ]
    ${primary_key_check}      return_primary_check    ${system}.${table}    ${Primary_Key_List}
    ${status}    Run keyword and return Status    Should Be Empty    ${primary_key_check}
    Run keyword if    ${status}==True       Add result to report    Test PASSED: Uniqueness of the primary keys are as expected for the curated table
    Run keyword if    ${status}==False      Add result to report    !Test FAILED: At least one primary keys are not as expected.
    Run keyword if    ${status}==False      Add result to report     Primary keys were not unique, data was returned\n:${primary_key_check}
    Run keyword if    ${status}==False      Set test variable        ${execution_status}    FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    log test to report per step     ${jira_id_tag}

Fail test and Log error
    [Arguments]            ${var}
    Set test variable      ${execution_status}  FAIL
    Add result to report   !Test Failed, ${var} not following requirements
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS

I expect that columns are of the correct datatype
    [Arguments]      ${target}   ${COLUMNS_AND_DATATYPES}
    Validate that columns are of the correct datatype     ${COLUMNS_AND_DATATYPES}   ${target}
    Log     Checked columns and datatypes: ${COLUMNS_AND_DATATYPES}


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
    Run keyword if   ${status}==False     Add result to report    FAILED: Column ${column} is of ${datatype_from_dbfs} datatype but required datatype is ${datatype} in ${system}.${target}\n
    Run keyword if   ${status}==True      Add result to report    PASSED: Column ${column} is of ${datatype} datatype as required in ${system}.${target} \n
    Log to console   Column ${column} is checked
    END
    Run keyword if   '${execution_status}'=='PASS'   Add result to report   \nTest Passed: All Columns are correct datatype.   ELSE   Add result to report   \nTest Failed: At least one datatype was incorrect.
    log test to report per step     ${jira_id_tag}

I expect that columns which are marked as Follow UTC don't contain null
    [Arguments]       ${UTC_COLUMNS}    ${source}      ${target}
    Validate that columns don't contain null    ${UTC_COLUMNS}      ${source}      ${target}
     Log     UTC columns list: ${UTC_COLUMNS} for source/sources: ${source}

Validate that columns don't contain null
    [Arguments]       ${column_list}      ${source}      ${target}
    ${temp}     run keyword and return status     Should Be String        ${source}
    run keyword if     ${temp}==True      Check that columns don't contain null for one source            ${column_list}      ${source}      ${target}
    run keyword if     ${temp}==False     Check that columns don't contain null for list of sources       ${column_list}      ${source}      ${target}

Check that columns don't contain null for one source
    [Arguments]       ${column_list}      ${source}      ${target}
    Validate that column or columns do not contain nulls       ${source}    ${column_list}      ${target}

Check that columns don't contain null for list of sources
    [Arguments]       ${column_list}      ${source}      ${target}
    FOR     ${source}   IN  @{source}
    Validate that column or columns do not contain nulls     ${source}    ${column_list}      ${target}
    END

Validate that column or columns do not contain nulls
    [Arguments]        ${source}     ${column_list}   ${target}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
    ${status}    return_count_of_table_with_nulls_UTCS  ${system}.${target}    ${alias}    ${column_list}
    Run keyword if    ${status}==True        Add result to report   !FAILED: Columns ${column_list} contains nulls for source ${source} in ${system}.${target}\n
    Run keyword if    ${status}==True        Set test variable      ${execution_status}    FAIL
    Run Keyword And Continue On Failure      Should Be Equal As Strings   ${execution_status}     PASS
    Log to console      For source ${source} columns: ${column_list} tested

I expect that these columns contain a date in UTC format
    [Arguments]     ${UTC_COLUMNS}      ${source}     ${target}
    Validate that columns contain a date in UTC format    ${UTC_COLUMNS}      ${source}     ${target}
    Log     UTC columns list: ${UTC_COLUMNS} for source/sources: ${source}

Validate that columns contain a date in UTC format
    [Arguments]     ${UTC_COLUMNS}      ${source}     ${target}
    ${temp}     run keyword and return status     Should Be String        ${source}
    Run keyword if   ${temp}==True      Check UTC for one source          ${UTC_COLUMNS}      ${source}     ${target}
    Run keyword if   ${temp}==False     Check UTC for list of sources     ${UTC_COLUMNS}      ${source}     ${target}
    log test to report per step     ${source}_${UTC_COLUMNS}[0]

Check UTC for one source
    [Arguments]     ${UTC_COLUMNS}      ${source}     ${target}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
    Validate that columns contains a date in UTC per source   ${UTC_COLUMNS}    ${alias}     ${target}

Check UTC for list of sources
   [Arguments]     ${UTC_COLUMNS}      ${source}     ${target}
    FOR     ${source}   IN  @{source}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
    Validate that columns contains a date in UTC per source   ${UTC_COLUMNS}    ${alias}     ${target}
    END

Validate that columns contains a date in UTC per source
    [Arguments]     ${utc_columns}     ${source}     ${target}
    FOR     ${column}   IN  @{utc_columns}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    Check UTC format for column     ${system}  ${target}  ${column}     ${source}
    END
    Run keyword if   '${execution_status}'=='PASS'     Add result to report   Test Passed: \nAll columns ${utc_columns} are in UTC format as per requirements for ${source} source
    Run keyword if   '${execution_status}'=='FAIL'     Add result to report   Test Failed: At least one column in not in UTC Format for ${source} source.

Check UTC format for column
    [Arguments]    ${system}  ${target}   ${column}     ${source}
    ${utc_date}=         run keyword    return_date_as_utc_source     ${system}    ${target}   ${column}     ${source}
    ${is_not_none}=      Run keyword and return status         should not be equal as strings   '${utc_date}'   'None'
    ${ends_with_0000}=   Run keyword and return status         should end with    ${utc_date}   +0000
    Log    ${utc_date}
    Log to console    Checked for ${column} and source ${source}
    Run keyword if    ${is_not_none}==False        Fail test and Log error    ${column}
    Run keyword if    ${ends_with_0000}==True      Add UTC check result to report   ${column}   ${utc_date}    ${target}

Add UTC check result to report
    [Arguments]    ${column}   ${utc_date}    ${target}
    Add result to report   The column ${column} ends with +0000 offset
    Add result to report   Data presented in column: ${utc_date}
    Add result to report   PASSED: The ${column} column Follow UTC format as required for ${system}.${target}\n

I expect that columns Follow UTC Format or contain nulls as per the filter requirement
    [Arguments]        ${source}    ${utc_columns}      ${target}
    Validate that columns Follow UTC Format or contain nulls     ${utc_columns}   ${source}     ${target}
    Log    UTC columns list: ${utc_columns} for source/sources: ${source}

Validate that columns Follow UTC Format or contain nulls
    [Arguments]      ${column_list}   ${source}     ${target}
    ${temp}     run keyword and return status     Should Be String        ${column_list}
    Run keyword if   ${temp}==True     Check that column follow UTC Format or contain nulls for one source            ${column_list}   ${source}     ${target}
    Run keyword if   ${temp}==False     Check that column follow UTC Format or contain null for list of sources    ${column_list}   ${source}     ${target}
    log test to report per step     ${source}_${column_list}[0]

Check that column follow UTC Format or contain nulls for one source
    [Arguments]      ${column_list}   ${source}     ${target}
    Validate that column follow UTC Format or contain null per source     ${source}    ${column_list}     ${target}

Check that column follow UTC Format or contain null for list of sources
    [Arguments]      ${column_list}   ${source}     ${target}
    FOR     ${source}   IN  @{source}
    Validate that column follow UTC Format or contain null per source     ${source}    ${column_list}     ${target}
    END

Validate that column follow UTC Format or contain null per source
    [Arguments]        ${source}     ${column_list}     ${target}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}    ${source}
    FOR     ${column}   IN  @{column_list}
    ${check_not_nulls}    return_count_of_table_is_not_null_per_col       ${system}.${target}        ${alias}     ${column}
    ${status}             Run keyword and return status     Should Not Be Equal As Integers    ${check_not_nulls}    0
    ${check_nulls}        return_count_of_table_is_null_per_col        ${system}.${target}       ${alias}     ${column}
    ${null_status}        Run keyword and return status     Should Not Be Equal As Integers    ${check_nulls}    0
    Run keyword if    ${status}==True          Check UTC format for column          ${system}    ${target}    ${column}       ${alias}
    Run keyword if    ${null_status}==True     Add result to report    The column ${column} contains ${check_nulls} of nulls as per filter requirement in ${system}.${target}
    END

I expect that the EDM location is correct
    [Arguments]    ${table_location}    ${table}
    Validate that EDM location is correct    ${table_location}    ${table}
    log    EDM location: ${table_location}

Validate that EDM location is correct
    [Arguments]    ${table_location}    ${table}
    ${actual_location}=   return location of table on dbfs   ${table}   ${system}
    ${env}    Run keyword and return status      Should Contain      ${actual_location}    ${Prod_env_loc}
    log     ${actual_location}
    log     ${table_location}
    Run keyword if   ${env}==True     Update location to use proper syntax    ${table_location}
    Add result to report   Actual Result: ${actual_location}
    Add result to report   Expected Result: ${table_location}\n
    ${status}        Run keyword and return status      should be equal   ${actual_location}   ${table_location}
    run keyword if  ${status}==True     Add result to report   PASSED: The ${table}'s underlying files are in the correct ADLS location
    run keyword if  ${status}==False    Add result to report   FAILED: Table location was not equal to expected location\n
    run keyword if  ${status}==False    Set test variable      ${execution_status}    FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    log test to report per step     ${jira_id_tag}

Update location to use proper syntax
    [Arguments]    ${table_location}
    ${temp} =	Fetch From Left      ${table_location}     ${Prod_env_loc}
    ${temp2} =	Fetch From Right     ${table_location}     ${Prod_env_loc}
    ${table_location}    Catenate    ${temp}${Prod_env}${temp2}
    set test variable     ${table_location}
    log       ${table_location}

I expect that listed columns contains all nulls values
    [Arguments]     ${source_list}    ${column_list}     ${target}
    Validate That Colums Contains Nulls For Sources    ${source_list}    ${column_list}     ${target}
    log    Column list for null check: ${column_list} source/sources: ${source_list}

I expect that listed columns contains a hashtag values for source systems
   [Arguments]     ${source_list}    ${column_list}    ${target}
    Validate that colums contains hashtag values for sources    ${source_list}    ${column_list}    ${target}
    log     Column list for hashtag test: ${column_list} for source/sources: ${source_list}

I expect that listed columns contains a hashtag values or values as per filter requirement
    [Arguments]     ${source_list}    ${column_list}    ${target}
    Validate that colums contains hashtag values or values as per filter requirement    ${source_list}    ${column_list}    ${target}
    log     Column list for hashtag test: ${column_list} for source/sources: ${source_list}

I expect that listed columns contains a null values or values as per filter requirement
   [Arguments]     ${source_list}    ${column_list}    ${target}
    Validate that colums contains null values or values as per filter requirement    ${source_list}    ${column_list}    ${target}
    log    Column list for null check: ${column_list} source/sources: ${source_list}

I expect that the listed columns contains all nulls values after providing the filter conditions of multiple unions
    [Arguments]     ${source_list}     ${column_list}    ${null_filter_condition}     ${target}
    Validate that the columns contain null values as per the filter requirement of multiple unions      ${source_list}       ${column_list}    ${null_filter_condition}     ${target}
    log    Column list for null check: ${column_list} source/sources: ${source_list}

I expect that all whitespaces removed from columns
   [Arguments]     ${source_list}    ${column_list}    ${target}
   Validate that all whitespaces removed for source    ${source_list}    ${column_list}    ${target}
   log    Column list for whitespaces check: ${column_list} for source/sources: ${source_list}

Validate that all whitespaces removed for source
  [Arguments]     ${source_list}    ${column_list}    ${target}
   ${temp}     run keyword and return status     Should Be String        ${source_list}
    Run keyword if   ${temp}==True      Check that all whitespaces removed for one source            ${column_list}    ${source_list}    ${target}
    Run keyword if   ${temp}==False     Check all whitespaces removed for list of sources            ${column_list}    ${source_list}    ${target}
    log test to report per step     ${source_list}_${column_list}[0]

Check that all whitespaces removed for one source
    [Arguments]      ${column_list}      ${source_list}    ${target}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source_list}
    ${column_count}   Get Length   ${column_list}
    Run keyword if   ${column_count}>=45      Check that whitespaces is removed for each column per source    ${alias}    ${column_list}    ${target}
    Run keyword if   ${column_count}<45       Check that whitespaces is removed for list of columns           ${alias}    ${column_list}    ${target}

Check all whitespaces removed for list of sources
    [Arguments]       ${column_list}     ${source_list}    ${target}
    FOR  ${source}  IN     @{source_list}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
   run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${column_count}   Get Length   ${column_list}
    Run keyword if   ${column_count}>=45     Check that whitespaces is removed for each column per source       ${alias}    ${column_list}    ${target}
    Run keyword if   ${column_count}<45      Check that whitespaces is removed for list of columns              ${alias}    ${column_list}    ${target}
   END

Check that whitespaces is removed for list of columns
    [Arguments]     ${alias}      ${column_list}    ${target}
    add system information to report whitespaces   ${system}.${target}     ${alias}
    ${whitespace}=  return_count_of_whitespaces_for_table     ${system}.${target}      ${alias}       ${column_list}
    log to console    Alias ${alias} tested for columns ${column_list}
    run keyword if    ${whitespace}==True    add result to report   Test Passed : Columns: ${column_list} contains no whitespaces for ${alias}\n
    run keyword if    ${whitespace}==False   add result to report   Test Failed : At least one column in ${column_list}contains whitespaces for ${alias}\n
    run keyword if    ${whitespace}==False   set test variable      ${execution_status}  FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS

Check that whitespaces is removed for each column per source
     [Arguments]     ${alias}      ${column_list}    ${target}
     add system information to report whitespaces   ${system}.${target}     ${alias}
     FOR  ${column}  IN     @{column_list}
     ${whitespace}    return_count_of_table_whitespaces_per_column     ${system}.${target}  ${alias}     ${column}
     log to console    Alias ${alias} tested for columns ${column}
     run keyword if    ${whitespace}==True    add result to report   Test Passed : Column ${column} contain no whitespaces for ${alias}\n
     run keyword if    ${whitespace}==False   add result to report   Test Failed : Column ${column} contain whitespaces for ${alias}\n
     run keyword if    ${whitespace}==False   set test variable      ${execution_status}  FAIL
     Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
     END

Validate that colums contains nulls for sources
    [Arguments]     ${source_list}    ${column_list}     ${target}
    ${temp}     run keyword and return status     Should Be String         ${source_list}
    Run keyword if   ${temp}==True      Check that column contain nulls for one source            ${column_list}   ${source_list}     ${target}
    Run keyword if   ${temp}==False     Check that column contain null for list of sources    ${column_list}   ${source_list}     ${target}
    log test to report per step     ${source_list}_${column_list}[0]


Validate that colums contains hashtag values for sources
    [Arguments]     ${source_list}    ${column_list}    ${target}
    ${temp}     run keyword and return status     Should Be String        ${source_list}
    Run keyword if   ${temp}==True      Check that column contain hashtag for one source             ${column_list}   ${source_list}    ${target}
    Run keyword if   ${temp}==False     Check that column contain hashtag for list of sources    ${column_list}   ${source_list}    ${target}
    log test to report per step    ${source_list}_${column_list}[0]

Validate that colums contains hashtag values or values as per filter requirement
    [Arguments]     ${source_list}    ${column_list}    ${target}
    ${temp}     run keyword and return status     Should Be String        ${source_list}
    Run keyword if   ${temp}==True      Check that column contain hashtag or value for one source             ${column_list}   ${source_list}    ${target}
    Run keyword if   ${temp}==False     Check that column contain hashtag or value for list of sources    ${column_list}   ${source_list}    ${target}
    log test to report per step    ${source_list}_${column_list}[0]

Validate that colums contains null values or values as per filter requirement
    [Arguments]     ${source_list}    ${column_list}    ${target}
    ${temp}     run keyword and return status     Should Be String        ${source_list}
    Run keyword if   ${temp}==True      Check that column contain nulls or value for one source             ${column_list}   ${source_list}    ${target}
    Run keyword if   ${temp}==False     Check that column contain nulls or value for list of sources    ${column_list}   ${source_list}    ${target}
    log test to report per step    ${source_list}_${column_list}[0]

Validate that the columns contain null values as per the filter requirement of multiple unions
    [Arguments]      ${source_list}      ${column_list}    ${null_filter_condition}     ${target}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source_list}
    log    ${Connection}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    add system information to report null     ${system}.${target}     ${alias}
    ${all_nulls}=     return_count_of_table_all_nulls_with_condition    ${system}.${target}       ${alias}       ${null_filter_condition}    ${column_list}
    log to console    Alias ${alias} tested for columns ${column_list}
    run keyword if    ${all_nulls}==True    add result to report   Test Passed : Columns: ${column_list} contains nulls values for ${alias}\n
    run keyword if    ${all_nulls}==False   add result to report   Test Failed : At least one column in ${column_list} contains other values instead of nulls for ${alias}\n
    run keyword if    ${all_nulls}==False   set test variable      ${test_status}  FAIL
    log test to report per step    ${source_list}_${column_list}[0]

Check that column contain hashtag for one source
    [Arguments]        ${column_list}    ${source_list}    ${target}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source_list}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${column_count}   Get Length   ${column_list}
    Run keyword if    ${column_count}>=45      Validate that each column contains hashtag      ${alias}      ${column_list}    ${target}
    Run keyword if    ${column_count}<45       Validate that columns contain hashtag           ${alias}      ${column_list}    ${target}

Check that column contain hashtag or value for one source
    [Arguments]        ${column_list}    ${source_list}    ${target}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source_list}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    Validate that each column contains hashtag or values      ${alias}      ${column_list}    ${target}

Check that column contain nulls or value for one source
    [Arguments]        ${column_list}    ${source_list}    ${target}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source_list}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    Validate that each column contains nulls or values      ${alias}      ${column_list}    ${target}

Check that column contain hashtag or value for list of sources
    [Arguments]       ${column_list}     ${source_list}    ${target}
    FOR  ${source}  IN     @{source_list}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    Validate that each column contains hashtag or values      ${alias}      ${column_list}    ${target}
   END

Check that column contain nulls or value for list of sources
    [Arguments]       ${column_list}     ${source_list}    ${target}
    FOR  ${source}  IN     @{source_list}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    Validate that each column contains nulls or values      ${alias}      ${column_list}    ${target}
    END

Check that column contain hashtag for list of sources
    [Arguments]       ${column_list}     ${source_list}    ${target}
    FOR  ${source}  IN     @{source_list}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${column_count}   Get Length   ${column_list}
    Run keyword if    ${column_count}>=45      Validate that each column contains hashtag      ${alias}      ${column_list}    ${target}
    Run keyword if    ${column_count}<45       Validate that columns contain hashtag           ${alias}      ${column_list}    ${target}
    END

Validate that each column contains hashtag
    [Arguments]     ${alias}    ${column_list}    ${target}
    FOR  ${column}  IN     @{column_list}
    ${hastags}=      return_count_of_table_all_hashtags_per_column    ${system}.${target}       ${alias}      ${column}
    log to console    Alias ${alias} tested for column ${column}
    run keyword if    ${hastags}==True    add result to report   Test Passed : Column: ${column} contains hashtags values for ${alias}\n
    run keyword if    ${hastags}==False   add result to report   Test Failed : Column ${column} contains other values instead of hashtags for  ${alias}\n
    run keyword if    ${hastags}==False   set test variable      ${execution_status}  FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    END

Validate that each column contains hashtag or values
    [Arguments]     ${alias}    ${column_list}    ${target}
    FOR  ${column}  IN     @{column_list}
    ${hastags}=       return_count_of_table_all_hashtags_or_value_per_column    ${system}.${target}       ${alias}      ${column}
    log to console    Alias ${alias} tested for column ${column}
    run keyword if    ${hastags}==True    add result to report   Test Passed : Column: ${column} contains hashtags values or filter values for ${alias}\n
    run keyword if    ${hastags}==False   add result to report   Test Failed : Column ${column} contains other values instead of hashtags or filter values for  ${alias}\n
    run keyword if    ${hastags}==False   set test variable      ${execution_status}  FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    END

Validate that each column contains nulls or values
   [Arguments]     ${alias}    ${column_list}    ${target}
    FOR  ${column}  IN     @{column_list}
    ${hastags}=       return_count_of_table_all_null_or_value_per_column    ${system}.${target}       ${alias}      ${column}
    log to console    Alias ${alias} tested for column ${column}
    run keyword if    ${hastags}==True    add result to report   Test Passed : Column: ${column} contains null values or filter values for ${alias}\n
    run keyword if    ${hastags}==False   add result to report   Test Failed : Column ${column} contains other values instead of null or filter values for  ${alias}\n
    run keyword if    ${hastags}==False   set test variable      ${execution_status}  FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    END

Validate that columns contain hashtag
    [Arguments]     ${alias}    ${column_list}    ${target}
    ${hastags}=      return_count_of_table_all_hashtags    ${system}.${target}       ${alias}     ${column_list}
    log to console    Alias ${alias} tested for columns ${column_list}
    run keyword if    ${hastags}==True    add result to report   Test Passed : Columns: ${column_list} contains hashtags values for ${alias}\n
    run keyword if    ${hastags}==False   add result to report   Test Failed : At least one column in ${column_list} contains other values instead of hashtags for ${alias}\n
    run keyword if    ${hastags}==False   set test variable      ${execution_status}  FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS

Check that column contain nulls for one source
    [Arguments]       ${column_list}    ${source_list}     ${target}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}   ${source_list}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${column_count}   Get Length   ${column_list}
    Run keyword if    ${column_count}>=45     Validate that each column contains nulls      ${alias}      ${column_list}     ${target}
    Run keyword if    ${column_count}<45      Validate that columns contain nulls           ${alias}      ${column_list}     ${target}

Check that column contain null for list of sources
   [Arguments]        ${column_list}    ${source_list}    ${target}
    FOR  ${source}  IN     @{source_list}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${column_count}   Get Length   ${column_list}
    Run keyword if    ${column_count}>=45      Validate that each column contains nulls      ${alias}      ${column_list}    ${target}
    Run keyword if    ${column_count}<45       Validate that columns contain nulls           ${alias}      ${column_list}    ${target}
    END

Validate that columns contain nulls
    [Arguments]      ${alias}       ${column_list}     ${target}
    add system information to report null     ${system}.${target}    ${alias}
    ${all_nulls}=     return_count_of_table_all_nulls    ${system}.${target}      ${alias}       ${column_list}
    log to console    Alias ${alias} tested for columns ${column_list}
    run keyword if    ${all_nulls}==True    add result to report   Test Passed: Columns ${column_list} contains nulls values for ${alias}\n
    run keyword if    ${all_nulls}==False   add result to report   Test Failed: At least one column in ${column_list} contains other values instead of nulls for ${alias}\n
    run keyword if    ${all_nulls}==False   set test variable      ${execution_status}  FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS

Validate that each column contains nulls
    [Arguments]      ${alias}       ${column_list}     ${target}
    FOR  ${column}  IN     @{column_list}
    ${all_nulls}      return_count_of_table_all_nulls_per_column    ${system}.${target}    ${alias}     ${column}
    log to console    Alias ${alias} tested for column ${column}
    run keyword if    ${all_nulls}==True    add result to report   Test Passed: Column ${column} contains nulls values for ${alias}\n
    run keyword if    ${all_nulls}==False   add result to report   Test Failed: Column ${column} contains other values instead of nulls for ${alias}\n
    run keyword if    ${all_nulls}==False   set test variable      ${execution_status}  FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    END

Validate that colums contains predefined values for sources
    [Arguments]   ${source_list}    ${column_list}     ${target}
    ${temp}     run keyword and return status     Should Be String         ${source_list}
    Run keyword if   ${temp}==True      Check that column contain predefined values for one source             ${column_list}   ${source_list}     ${target}
    Run keyword if   ${temp}==False     Check that column contain predefined for list of sources    ${column_list}   ${source_list}     ${target}
    log test to report per step     ${source_list}

Validate that colums contains predefined values for sources or values from original table
    [Arguments]   ${source_list}    ${column_list}     ${target}
    ${temp}     run keyword and return status     Should Be String         ${source_list}
    Run keyword if   ${temp}==True      Check that column contain predefined values for one source or values from original table            ${column_list}   ${source_list}     ${target}
    Run keyword if   ${temp}==False     Check that column contain predefined for list of sources or values from original table   ${column_list}   ${source_list}     ${target}
    log test to report per step     ${source_list}_${column_name}

Check that column contain predefined for list of sources
    [Arguments]      ${column_list}    ${source_list}    ${target}
    FOR  ${source}   IN   @{source_list}
    Check that column contain predefined values for one source    ${column_list}     ${source}    ${target}
    END

Check that column contain predefined for list of sources or values from original table
    [Arguments]      ${column_list}    ${source_list}    ${target}
    FOR  ${source}   IN   @{source_list}
    Check that column contain predefined values for one source or values from original table    ${column_list}     ${source}    ${target}
    END

Check that column contain predefined values for one source
    [Arguments]    ${column_list}   ${source}     ${target}
    ${temp}    Get Length    ${column_list}
    run Keyword If   ${temp} >1     Check list of predifined values for source      ${column_list}   ${source}     ${target}
    run Keyword If   ${temp} ==1    Check one predifined values for source      ${column_list}   ${source}     ${target}

Check one predifined values for source
    [Arguments]    ${column_list}   ${source}     ${target}
    FOR    ${column}  IN  @{column_list}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    log     ${column}
    ${column_list}   Convert to string   ${column_list}
    ${column}=    Fetch From Left    ${column_list}    :
    ${value}=     Fetch From Right   ${column_list}    :
    ${column}=    Remove String    ${column}    	{  '  ${SPACE}
    Set test variable    ${column_name}   ${column}
    ${value}=     Remove String    ${value}    }
    ${value_is_list}=    run keyword and return status   Should Contain      ${value}   [
    run keyword if  ${value_is_list}==True     Get count of table with multiple predefined values    ${alias}    ${column}    ${value}    ${target}
    run keyword if  ${value_is_list}==False    Get count of table with single predefined value       ${alias}   ${column}     ${value}    ${target}
    END

Check list of predifined values for source
    [Arguments]    ${column_list}   ${source}     ${target}
    FOR    ${column}  IN  @{column_list}
    log       ${column}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${columnlIST}   Convert to string   ${column}
    ${columnStr}=    Fetch From Left   ${columnlIST}   :
    ${value}=     Fetch From Right  ${columnlIST}    :
    ${columnStr}=    Remove String    ${columnStr}    	{  '  ${SPACE}
    Set test variable    ${column_name}   ${columnStr}
    ${value}=     Remove String    ${value}    }
    ${value_is_list}=    run keyword and return status   Should Contain      ${value}   [
    run keyword if  ${value_is_list}==True     Get count of table with multiple predefined values    ${alias}    ${columnStr}    ${value}    ${target}
    run keyword if  ${value_is_list}==False    Get count of table with single predefined value       ${alias}   ${columnStr}     ${value}    ${target}
    END

Check that column contain predefined values for one source or values from original table
    [Arguments]    ${column_list}   ${source}     ${target}
    log    ${source}
    log     ${column_list}
    FOR    ${column}  IN  @{column_list}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${column_list}   Convert to string   ${column_list}
    ${column}=    Fetch From Left    ${column_list}    :
    ${value}=     Fetch From Right   ${column_list}    :
    ${column}=    Remove String    ${column}    	{  '  ${SPACE}
    Set test variable    ${column_name}   ${column}
    ${value}=     Remove String    ${value}    }
    ${value_is_list}=    run keyword and return status   Should Contain      ${value}   [
    run keyword if  ${value_is_list}==True     Get count of table with multiple predefined values or values from original table    ${alias}    ${column}    ${value}    ${target}
    run keyword if  ${value_is_list}==False    Get count of table with single predefined value or values from original table       ${alias}   ${column}     ${value}    ${target}
    END

Get count of table with single predefined value
    [Arguments]      ${alias}    ${column}    ${value}     ${target}
    log     ${value}
    ${status}    run keyword and return status   Should Contain      ${value}   '
    run keyword if     ${status}==False     Set test variable    ${value}     '${value}'
    ${result}=      return_count_of_table_all_pred_values     ${system}.${target}    ${alias}     ${column}   	 ${value}
    Run keyword if  ${result}==True      add result to report   \nTest Passed: Values:${value} was as expected in target ${system}.${target} column ${column}    ELSE   add result to report   \nTest Failed: Value ${value} was not as expected in target ${system}.${target} column ${column}.
    Run keyword if  ${result}==False     set test variable   ${execution_status}  FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    log to console   Tested column ${column} in source ${alias}

Get count of table with single predefined value or values from original table
    [Arguments]      ${alias}    ${column}    ${value}     ${target}
    log     ${value}
    ${status}    run keyword and return status   Should Contain      ${value}   '
    run keyword if     ${status}==False     Set test variable    ${value}     '${value}'
    ${result}=      return_count_of_table_all_pred_values_or_value      ${system}.${target}    ${alias}     ${column}   	 ${value}
    Run keyword if  ${result}==True      add result to report   \nTest Passed: Values:${value} was as expected in target ${system}.${target} column ${column}    ELSE   add result to report   \nTest Failed: Value ${value} was not as expected in target ${system}.${target} column ${column}.
    Run keyword if  ${result}==False     set test variable   ${execution_status}  FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    log to console   Tested column ${column} in source ${alias}

Get count of table with multiple predefined values
    [Arguments]      ${alias}    ${column}     ${value}    ${target}
    ${value}=    Remove String    ${value}    [    ]
    ${result}      return_count_of_table_list_pred_values     ${system}.${target}    ${alias}   ${column}      ${value}
    Run keyword if  ${result}==False     set test variable   ${execution_status}  FAIL
    Run keyword if  ${result}==True       Add result to report   \nTest Passed: All values were as expected in target ${system}.${target} column ${column}    ELSE   add result to report   \nTest Failed: At least one values was not as expected in target ${system}.${target} column ${column}.
    log to console   Tested column ${column} in source ${alias}

Get count of table with multiple predefined values or values from original table
    [Arguments]      ${alias}    ${column}     ${value}    ${target}
    ${value}=      Remove String    ${value}    [    ]
    ${result}      return_count_of_table_all_pred_values_or_value     ${system}.${target}    ${alias}   ${column}      ${value}
    Run Keyword If  ${result}==False      set test variable   ${execution_status}  FAIL
    Run keyword if  ${result}==True       Add result to report   \nTest Passed: All values were as expected in target ${system}.${target} column ${column}    ELSE   add result to report   \nTest Failed: At least one values was not as expected in target ${system}.${target} column ${column}.
    log to console   Tested column ${column} in source ${alias}

I expect that the columns contain all predefined values
    [Arguments]   ${source_list}    ${column_predef_val_list}    ${target}
    Validate that colums contains predefined values for sources     ${source_list}    ${column_predef_val_list}    ${target}
    Log     Colums contains predefined values: ${column_predef_val_list} for source/sources: ${source_list}

I expect that the columns contain predefined values or values from original table
   [Arguments]   ${source_list}    ${column_predef_val_list}    ${target}
    Validate that colums contains predefined values for sources or values from original table     ${source_list}    ${column_predef_val_list}    ${target}
    Log     Colums contains predefined values: ${column_predef_val_list} for source/sources: ${source_list}

I expect that data count is not below expected amount
    Validate that data count is not below expected amount    ${DATA_COUNT}
    Log   Expected data count: ${DATA_COUNT}

Validate that data count is not below expected amount
    [Arguments]    ${DATA_COUNT}
    ${target_source_count}=     test data count per source   ${system}    ${table}    ${source_system}
    ${status} =	Evaluate   ${target_source_count}>=${DATA_COUNT}
    ${test_result}=  run keyword and return status      should be true   ${status}
    run keyword if   ${test_result}==False   set test variable   ${execution_status}  FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    run keyword if   ${test_result}==True   add result to report       Expected data count is ${DATA_COUNT}      ELSE   add result to report   Error occurred
    run keyword if   ${test_result}==True   add result to report       Test Passed:The ${system}.${table} data count is ${target_source_count}\n     ELSE   add result to report     Error occurred\n
    run keyword if   '${execution_status}'=='PASS'   add result to report   Test Passed: Target data count is not below expected amount\n.   ELSE   add result to report   Test Failed:Target data count is bellow expected amount:${DATA_COUNT}.
    run keyword and continue on failure   Should Be Equal As Strings   ${execution_status}   PASS
    log test to report per step     ${jira_id_tag}

I expect that view is returning data for all the sources
    [Arguments]    ${source}
    Validate that data count for all the sources for the view    ${source}
    Log    Source: ${source}

Validate that data count for all the sources for the view
    [Arguments]       ${source_system}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    FOR  ${i}  IN  @{source_system}
        Validate the data count for each source    ${i}
    END
    run keyword if   '${data}'!='FAIL' and '${execution_status}'=='FAIL'      add result to report      Test Failed: The view is not returning data as expected  ELSE    add result to report  Test Passed: The view is returning data as expected
    run keyword and continue on failure     should be equal as strings  ${execution_status}  PASS
    log test to report per step     ${jira_id_tag}

Validate the data count for each source
    [Arguments]      ${source}
    ${alias}   get from dictionary     ${TABLE_ALIAS}     ${source}
    set test variable    ${alias}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${count}=    return count of table with conditions  ${system}.${view}       SRC_SYS_CD == '${alias}'
    run keyword if       ${count}==0      Data not available
    run keyword   add result to report   Expected = ${count}
    run Keyword If  '${count}'!='0'    add result to report   Test Passed: The ${system}.${view} view is returning data for the source ${alias} with data count ${count}\n       ELSE    add result to report    Test Failed:The ${system}.${view} view is not returning data for the source ${alias} as expected with data count ${count}\n
    run keyword if   ${count}==0   set test variable   ${execution_status}    FAIL
    Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
    log to console    Checked for source ${alias}

Data not available
     run keyword   add result to report  !Test Failed: Data not available
     set test variable   ${execution_status}  FAIL
     Run Keyword And Continue On Failure     Should Be Equal As Strings   ${execution_status}     PASS
     set test variable   ${data}  FAIL

# Templates to be used for UTC, Whitespaces, Nulls or Hashtags
Validate that the columns marked as Follow UTC contain a date in UTC format
    [Arguments]    ${sources}     ${columns}    ${target}   ${databricks_name}
    Given I have access to Databricks database     ${databricks_name}
    When I check that the requirements are implemented correctly
    Then I expect that columns which are marked as Follow UTC don't contain null     ${columns}     ${sources}      ${target}
    And I expect that these columns contain a date in UTC format                      ${columns}     ${sources}     ${target}

Validate that for all listed columns contains all nulls values
    [Arguments]   ${source_list}    ${column_list}   ${target}     ${databricks_name}
    Given I have access to Databricks database    ${databricks_name}
    When I check that the requirements are implemented correctly
    Then I expect that listed columns contains all nulls values   ${source_list}    ${column_list}    ${target}

Validate that columns Follow UTC Format or contain nulls as per the filter requirement
    [Arguments]    ${sources}     ${columns}     ${target}    ${databricks_name}
    Given I have access to Databricks database    ${databricks_name}
    When I check that the requirements are implemented correctly
    Then I expect that columns Follow UTC Format or contain nulls as per the filter requirement    ${sources}     ${columns}     ${target}

Validate that trim implemented correctly for all listed columns per source
    [Arguments]   ${source_list}    ${column_list}   ${target}    ${databricks_name}
    Given I have access to Databricks database    ${databricks_name}
    When I check that the requirements are implemented correctly
    Then I expect that all whitespaces removed from columns    ${source_list}    ${column_list}   ${target}

Validate that Populate as null for all listed columns implemented correctly per source
    [Arguments]   ${source_list}    ${column_list}   ${target}    ${databricks_name}
    Given I have access to Databricks database    ${databricks_name}
    When I check that the requirements are implemented correctly
    Then I expect that listed columns contains all nulls values   ${source_list}    ${column_list}     ${target}

Validate that the columns marked as Populate as hashtag contain hashtag values
   [Arguments]   ${source_list}    ${column_list}   ${target}    ${databricks_name}
   Given I have access to Databricks database    ${databricks_name}
   When I check that the requirements are implemented correctly
   Then I expect that listed columns contains a hashtag values for source systems    ${source_list}    ${column_list}    ${target}

Validate that the columns contain hashtag values or values as per filter requirement
   [Arguments]   ${source_list}    ${column_list}   ${target}    ${databricks_name}
   Given I have access to Databricks database    ${databricks_name}
   When I check that the requirements are implemented correctly
   Then I expect that listed columns contains a hashtag values or values as per filter requirement   ${source_list}    ${column_list}    ${target}

Validate that the columns contain null values or values as per filter requirement
   [Arguments]   ${source_list}    ${column_list}   ${target}    ${databricks_name}
   Given I have access to Databricks database    ${databricks_name}
   When I check that the requirements are implemented correctly
   Then I expect that listed columns contains a null values or values as per filter requirement   ${source_list}    ${column_list}    ${target}

Validate that columns contain nulls after providing filter conditions of multiple unions
    [Arguments]      ${source_list}     ${column_list}    ${null_filter_condition}    ${target}     ${databricks_name}
    Given I have access to Databricks database    ${databricks_name}
    When I check that the requirements are implemented correctly
    Then I expect that the listed columns contains all nulls values after providing the filter conditions of multiple unions   ${source_list}       ${column_list}    ${null_filter_condition}     ${target}

Validate that columns contain all predefined values
    [Arguments]   ${source_list}    ${column_predef_val_list}   ${target}     ${databrick_name}
    Given I have access to Databricks database     ${databrick_name}
    When I check that the requirements are implemented correctly
    Then I expect that the columns contain all predefined values     ${source_list}    ${column_predef_val_list}     ${target}

Validate that columns contain predefined values or values from original table
   [Arguments]   ${source_list}    ${column_predef_val_list}   ${target}     ${databrick_name}
    Given I have access to Databricks database     ${databrick_name}
    When I check that the requirements are implemented correctly
    Then I expect that the columns contain predefined values or values from original table     ${source_list}    ${column_predef_val_list}     ${target}

# TESTS FOR VIEWS VALIDATION
I expect that the target view has correct number of columns
    Validate that the target view contains required number of columns    ${COLUMN_COUNT}
    Log     Required column number: ${COLUMN_COUNT}

Validate that the target view contains required number of columns
    [Arguments]     ${number_of_columns}
    ${columns}=    get columns from view   ${system}.${table}
    ${column_count}=   get length  ${columns}
    Add result to report   Validate that the target view: ${system}.${view} contains required number of columns
    Add result to report   Expected columns number : ${number_of_columns}
    Add result to report   Actual columns number : ${column_count}
    ${status}       Run Keyword And Return Status    Should be equal as numbers     ${column_count}     ${number_of_columns}
    Run keyword if  ${status}==True       Add result to report    Test PASSED : The ${system}.${view} contains required columns number ${number_of_columns}
    Run keyword if  ${status}==False      Add result to report    !Test FAILED: The ${system}.${view} expected ${number_of_columns} columns but contains ${column_count} columns
    Run keyword if  ${status}==False      Set test variable    ${execution_status}    FAIL
    log test to report per step     ${jira_id_tag}

I expect that the row count of view should match the underlying table
    The rowcounts of view and underlying table are the same

The rowcounts of view and underlying table are the same
    ${table_count}=    return count of table  ${underlying_l1_db_for_vw}.${underlying_table}
    ${view_count}=     return count of table  ${system}.${table}
    run keyword     add result to report    Expected = ${table_count}
    run keyword if  ${table_count}!=${view_count}     set test variable   ${execution_status}  FAIL
    run keyword if  ${table_count}==${view_count}     add result to report  Test Passed : View ${system}.${table} contains the same number of rows as table ${underlying_l1_db_for_vw}.${underlying_table}
    run keyword if  "${execution_status}"=="FAIL"  add result to report      Test Failed: View ${system}.${table} does not contain the same number of rows as table ${underlying_l1_db_for_vw}.${underlying_table}
    run keyword and continue on failure     should be equal as strings  ${execution_status}  PASS
    log test to report per step     ${jira_id_tag}

Validate that columns contain all predefined values for list of sources
    [Arguments]    ${source_list}    ${column_predef_val_list}   ${target}     ${databrick_name}
    Given I have access to Databricks database    ${databrick_name}
    When I check that the requirements are implemented correctly
    Then I check that columns contain all predefined values for list of sources    ${source_list}    ${column_predef_val_list}   ${target}

I check that columns contain all predefined values for list of sources
    [Arguments]    ${source_list}    ${column_predef_val_list}   ${target}
    FOR  ${i}  IN  @{source_system}
        ${alias}   get from dictionary     ${TABLE_ALIAS}     ${i}
        set test variable    ${alias}
        FOR  ${j}  IN  @{column_predef_val_list}
        ${predef_val_list}   Convert to string   ${j}
        ${source}=    Fetch From Left     ${predef_val_list}    [
        ${value}=     Fetch From Right    ${predef_val_list}    [
        ${value}      Remove String    ${value}    	]
        ${result}  run keyword and return status        Should contain   ${source}     ${alias}
        run keyword if    ${result}==True     Check predifined value for specific source    ${alias}   ${value}    ${target}
        Exit For Loop IF    ${result}==True
        END
    END
    log test to report per step     ${jira_id_tag}

Check predifined value for specific source
    [Arguments]   ${alias}    ${ColumnValue}    ${target}
    ${column}=    Fetch From Left    ${ColumnValue}    :
    ${value}=     Fetch From Right   ${ColumnValue}    :
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    Set test variable    ${column_name}   ${column}
    ${value}=     Remove String    ${value}    }
    ${value_is_list}=    run keyword and return status   Should Contain      ${value}   [
    run keyword if  ${value_is_list}==True     Get count of table with multiple predefined values     ${alias}    ${column}    ${value}    ${target}
    run keyword if  ${value_is_list}==False    Get count of table with single predefined value        ${alias}   ${column}     ${value}    ${target}