# Support file for level 1 curration tests
# Should hold keywords for tests that are generic to all Level 1 currations

*** Settings ***
Resource    Tests/Databricks/Support-Databricks.robot
Library       Library/Utils.py
*** Variables ***
@{blacklist}=         ETL_ID  ETL_CREATEDATE  ETL_LASTUPDATEDATE  ETL_USERID
${test_status}        PASS
${query_file_path}    Data/data_level_1_views/
${data}               PASS
${test_result}        PASS
*** Keywords ***
# ---------------------------- First level Keywords  ----------------------------
#-------------------GIVEN Keywords
I check the target table for number of columns
    The target table is checked for number of columns

I check the target view for number of columns
    The target view is checked for number of columns

I check the primary keys for the curated table
    Primary Keys are checked for curated table

I check that the Primary keys column does not contain null values
    The primary keys columns is checked for null values

I check that the columns contain not null values
    The columns are checked for not null values

I check that the curation logic has been applied
    The curation logic has been checked

I check that all specified columns contain a date in UTC
    All specified columns are checked for a UTC date

I check that not all column values are null
    All specified columns are checked to ensure that not all column values are null

I check that the column values are a hashtag for the source system
    All specified columns are checked for hashtag for the source systems

I check that the column values are trimmed for source system
    All specified columns are trimmed for source system

I check that the column values do not contain whitespaces for the source systems
   All specified columns are checked for whitespace for the source systems

I check that the column values contain blanks for the source systems
    All specified columns are checked for blanks for the source systems

I check that the column values contain zeros for the source systems
    All specified columns are checked for zeros for the source systems

I check that the specified columns contain all null values for source system
    All specified columns are checked for null values for source system

I check that the specified columns contain predefined values for source system
   All specified columns are checked for predefined values for source system

I check that the column values contain specified characters or they are nulls source system
    All specified columns are checked for specified characters or nulls for source system

I check the location of the EDM
    The EDM location is checked

I check the row count of view
    The rowcount of view is checked

I check the data count per source on target
   Get data count per source on target

I have access to Databricks database
    Check the user has successful connection to Databricks

I have access to view Databricks database
    Check the user has successful connection to Databricks views

I check that the requirements are implemented correctly
    Check that requirements are implemented correctly

#-------------------WHEN Keywords
The user has access to Databricks database
    Check the user has successful connection to Databricks

I check that test requirements are implemented correctly
     Check that requirements are implemented correctly

I has access to Databricks database
   Check the user has successful connection to Databricks

I check that the Columns for Cell to Cell Data Validation
    Columns are Checked for Cell to Cell Data Validation

I have access to Databricks view database
    Check the user has successful connection to Databricks views
#---------------THEN Keywords
I expect that columns are of the correct datatype
    The columns are of the correct datatype

I expect that the columns do not contain any whitespace
    The columns do not contain any whitespace

I expect that the primary keys column does not contain null values
    The Columns does not contain null values

I expect that the columns should contain not null values
    The Columns contain not null values

I expect there is only unique primary keys
    The Uniqueness Of The Primary Keys Are As Expected For The Curated Table

I expect that the column values do not contain whitespaces for the source systems when columns are same for multiple sources
    Validate that columns values do not contain whitespaces for the source system when columns are same for multiple sources

I expect that the column values do not contain whitespaces for the source systems when columns are different
    Validate that columns in source systems do not contain whitespaces when columns are different

I expect that the column values do not contain whitespaces in view for the source systems when columns are same for multiple sources
    Validate that columns values do not contain whitespaces in view for the source system when columns are same for multiple sources

I expect that the row count of view should match the underlying table
    The rowcounts of view and underlying table are the same

I expect that the UTC Column contains required number of characters
      Validate that UTC Column contains required number of characters

Check that requirements are implemented correctly
    set test variable    ${test_status}        PASS

I expect that the target view has correct number of columns
    The target view contains correct number of columns

I expect that the columns contain a date in UTC
    The columns contains a date in UTC

I expect that the Columns contains date values in UTC format when columns are different for 2 sets of sources
    [Arguments]     ${UTC_SOURCES_1}      ${UTC_COLUMNS_1}      ${UTC_SOURCES_2}      ${UTC_COLUMNS_2}
    Validate for specified source systems that specified columns contains a date in UTC(without non_null validation)    ${UTC_SOURCES_1}      ${UTC_COLUMNS_1}
    Validate for specified source systems that specified columns contains a date in UTC(without non_null validation)    ${UTC_SOURCES_2}      ${UTC_COLUMNS_2}

I expect that the columns contain a date in UTC(without non_null validation)
    The columns contains a date in UTC(without non_null validation)

I expect that the columns contain a date in UTC for specified source
    [Arguments]    ${source}      ${UTC_COLUMNS}
    Validate that specified columns contains a date in UTC   ${source}      ${UTC_COLUMNS}

I expect that for the specified source systems that specified columns contains a date in UTC
    [Arguments]    ${SOURCES_UTC}      ${COLUMNS_UTC}
    Validate for specified source systems that specified columns contains a date in UTC   ${SOURCES_UTC}      ${COLUMNS_UTC}

I expect that the columns contain all null values for source system
    Validate that columns contain all nulls for the source system

I expect that the columns contain all null values when columns are same for multiple sources
    Validate that columns contain all nulls when columns are same for multiple sources

I expect that the columns contain all null values when columns are same for 2 sets of sources
    [Arguments]     ${sourceList}     ${columns}    ${sourceList2}    ${columns2}
    Validate that columns contain all nulls when columns are same for set of sources      ${sourceList}    ${columns}
    Validate that columns contain all nulls when columns are same for set of sources      ${sourceList2}    ${columns2}

I expect that the columns contain all null values when columns are same for 3 sets of sources
    [Arguments]     ${sourceList}     ${columns}    ${sourceList2}    ${columns2}   ${sourceList3}    ${columns3}
    Validate that columns contain all nulls when columns are same for set of sources      ${sourceList}    ${columns}
    Validate that columns contain all nulls when columns are same for set of sources      ${sourceList2}    ${columns2}
    Validate that columns contain all nulls when columns are same for set of sources      ${sourceList3}    ${columns3}

I expect that the columns contain all null values when columns are same for set of sources
    [Arguments]     ${sourceList}     ${columns}
    Validate that columns contain all nulls when columns are same for set of sources      ${sourceList}    ${columns}
q11
I expect that the columns contain all null values with condition when columns are same for a set of sources
    [Arguments]     ${sourceList}     ${columns}
    Validate that columns contain all nulls with condition when columns are same for set of sources      ${sourceList}    ${columns}

I expect that the columns contain all null values with condition when columns are same for 2 set of sources
    [Arguments]     ${sourceList}     ${columns}    ${sourceList2}    ${columns2}
    Validate that columns contain all nulls with condition when columns are same for set of sources      ${sourceList}    ${columns}
    Validate that columns contain all nulls with condition when columns are same for set of sources      ${sourceList2}    ${columns2}

I expect that the columns contain whitespace removed when columns are same for set of sources
    [Arguments]     ${sourceList}     ${columns}
    Validate that columns contain whitespace removed when columns are same for set of sources      ${sourceList}    ${columns}

I expect that the columns contain whitespace removed when columns are same for 2 sets of sources
    [Arguments]     ${sourceList}     ${columns}    ${sourceList2}    ${columns2}
    Validate that columns contain whitespace removed when columns are same for set of sources      ${sourceList}    ${columns}
    Validate that columns contain whitespace removed when columns are same for set of sources      ${sourceList2}    ${columns2}

I expect that the all columns values are a hashtag for source system when columns are same for set of sources
    [Arguments]     ${sourceList}     ${columns}
    Validate that all columns contain hashtag when columns are same for set of sources    ${sourceList}    ${columns}

I expect that the column values are a hashtag for source system when columns are same for set of sources
    [Arguments]     ${sourceList}     ${columns}
    Validate that columns contain hashtag when columns are same for set of sources      ${sourceList}    ${columns}

I expect that the column values are a hashtag for source system when columns are same for 2 sets of sources
    [Arguments]     ${sourceList}     ${columns}     ${sourceList2}     ${columns2}
    Validate that columns contain hashtag when columns are same for set of sources      ${sourceList}    ${columns}
    Validate that columns contain hashtag when columns are same for set of sources      ${sourceList2}    ${columns2}

I expect that the column values are a hashtag for source system when columns are same for 1 sets of sources
    [Arguments]     ${sourceList}     ${columns}
    Validate that columns contain hashtag when columns are same for set of sources      ${sourceList}    ${columns}

Validate that columns contain hashtag when columns are same for set of sources
    [Arguments]     ${sourceList}     ${columns}
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{sourceList}
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        The column values are a hashtag for source system when columns are same for set of sources       ${columns}
    END
    log test to report per step     ${sourceList}_${columns}[0]
    #Teardown Test

Validate that all columns contain hashtag when columns are same for set of sources
    [Arguments]     ${sourceList}     ${columns}
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{sourceList}
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        The columns values are a hashtag for source system when set columns are same for set of sources    ${columns}
    END
	log test to report per step     ${sourceList}_${columns}[0]
    #Teardown Test

I expect that the columns contain all predefined values when columns are same for multiple sources
    Validate that columns contain all predefined values when columns are same for multiple sources

I expect that the columns contain all predefined values for source
    Validate that columns contain all predefined values for source

I expect that the columns contain all predefined values in views
    Validate that columns contain all predefined values in views

I expect that not all column values are null
    The column values are not all null

I expect that columns do not contain null values
    Validate that columns do not contain null values
# ---------------------------- Second level Keywords  ----------------------------
All specified columns are checked for a UTC date
    log to console  PASS

The target table is checked for number of columns
    log to console  PASS

The target view is checked for number of columns
    log to console  PASS

Check the user has successful connection to Databricks
    run keyword if      ${Connection}==True      log to console  CONNECTED TO DATABRICKS SUCCESSFULLY
    set test variable   ${databrick_connection_Type}   Table
    run keyword if      ${Connection}==False     Reconnect to Databrick

Check the user has successful connection to Databricks views
    run keyword if      ${Connection}==True      log to console  CONNECTED TO DATABRICKS VIEWS SUCCESSFULLY
    set test variable   ${databrick_connection_Type}   View
    run keyword if      ${Connection}==False     Reconnect to Databrick views

Columns are Checked for Cell to Cell Data Validation
    Log To Console  PASS

Primary Keys are checked for curated table
    log to console  PASS

I check the column datatypes
    The column datatypes are checked

The column datatypes are checked
    log to console  PASS

The columns are checked for whitespace
    log to console  PASS

The primary keys columns is checked for null values
    Log To Console  PASS

The columns are checked for not null values
    Log To Console  PASS

The curation logic has been checked
    log to console  PASS

The rowcount of view is checked
    log to console  PASS

All specified columns are checked to ensure that not all column values are null
    log to console  PASS

All specified columns are checked for whitespace for the source systems
    Log To Console  PASS

All specified columns are trimmed for source system
    log to console  PASS

All specified columns are checked for null values for source system
    log to console  PASS

All specified columns are checked for predefined values for source system
    log to console  PASS

All specified columns are checked for blanks for the source systems
    log to console  PASS

All specified columns are checked for zeros for the source systems
    log to console  PASS

All specified columns are checked for specified characters or nulls for source system
    log to console  PASS

All specified columns are checked for hashtag for the source systems
    log to console  PASS

The EDM location is checked
    log to console  PASS

# ---------------------------- Third level Keywords  ----------------------------
Reconnect to Databrick
    log to console  CONNECTION TO DATABRICKS FAILED
    run keyword if      ${Connection}==False     Connect To CDL Databricks
    run keyword if      ${Connection}==True      log to console  Reconnected tO DATABRICKS Successfully

Reconnect to Databrick views
    log to console  CONNECTION TO DATABRICKS VIEWS FAILED
    run keyword if      ${Connection}==False    Connect to CDL VIEW Databricks
    run keyword if      ${Connection}==True      log to console  Reconnected to DATABRICKS VIEWS Successfully

Validate that UTC Column contains required number of characters
    UTC Columns should contain ${UTC_COLUMN_CHARACTERS} characters


UTC Columns should contain ${No. of Characters} characters
    FOR    ${column}   IN    @{utc_columns}
        ${count}=       Run Keyword  Check For Count Of Characters In Column  ${system}.${table}        ${column}       ${No. of Characters}
        run keyword and continue on failure  should be equal as numbers  ${count}    0
        run keyword if  ${count}==0     add result to report  Test Passed : Counts is Zero so All Records in ${column} Column contains 28 Characters in ${system}.${table}\n        ELSE        add result to report    Test Failed: ${count} Records in ${column} does not have 28 Characters.
    END
    Teardown Test

I get data from original table with required join condition
    Get primary keys data from original table    ${COLUMNS_FOR_JOIN}    ${JOIN_CONDITION}

Get primary keys data from original table
    [Arguments]     ${COLUMNS_FOR_JOIN}    ${JOIN_CONDITION}
    ${final_table_column_list}        Create List
    ${final_select_column_list}       Create List
    ${original_select_column_list}    Create List
    ${expected_data_list}             Create List
    Set test variable       ${final_table_column_list}
    Set test variable       ${final_select_column_list}
    Set test variable       ${original_select_column_list}
    Set test variable       ${expected_data_list}
    FOR    ${column}        IN       @{COLUMNS_FOR_JOIN}
    ${value}=     get from dictionary  ${COLUMNS_FOR_JOIN}    ${column}
    ${expected_data}     run get data from original table    ${value}     ${JOIN_CONDITION}
    Append To List       ${final_select_column_list}          ${column}
    Append To List       ${original_select_column_list}       ${value}
    ${expected_data}     Convert To String             ${expected_data}
    ${expected_data}     Remove String                 ${expected_data}    (    '    )    ,    [    ]
    ${select_actual_data}       Catenate               And ${column}=${expected_data}
    ${select_original_data}     Catenate               And ${value}=${expected_data}
    Append To List       ${final_table_column_list}       ${select_actual_data}
    Append To List       ${expected_data_list}            ${select_original_data}
    END
    Get expected column data from original table      ${COLUMNS_FOR_JOIN}    ${JOIN_CONDITION}
    log     ${final_select_column_list}
    log      ${original_select_column_list}

I expect that data from original table is presented in final table
    Validate that data is presented in final table

Validate that data is presented in final table
    ${final_table_column_list}      Convert To String   ${final_table_column_list}
    ${final_table_column_list}      Remove String       ${final_table_column_list}   ['And     (    '    )    ,       ]
    ${final_select_column_list}     Convert To String          ${final_select_column_list}
    ${final_select_column_list}     Remove String              ${final_select_column_list}    (    '    )    [    ]
    ${actual_data}    run get data from final table     ${final_select_column_list}  ${table}    ${final_table_column_list}
    ${result}         run keyword and return status    Lists Should Be Equal      ${actual_data}      ${expected_data}
    run keyword if      ${result}==True       set test variable   ${test_status}  PASS
    run keyword if      ${result}==False      set test variable   ${test_status}  FAIL
    run keyword if      ${result}==True       add result to report    Test passed: for l1.${table} the join requirement implemented as required and data is presented in ${table} table
    Teardown Test

The columns contains a date in UTC
    FOR     ${column}   IN  @{utc_columns}
        run keyword if      ${Connection}==False     Connect to CDL Databricks
        ${non_nulls}=   run keyword  count non nulls  ${system}   ${table}  ${column}
        run keyword if      ${non_nulls}==0    Fail test and log error    ${column}
        run keyword if      ${non_nulls}!=0    Check UTC format for column     ${system}   ${table}   ${column}
        END

    run keyword if   '${test_status}'=='PASS'   add result to report   Test Passed: \nAll columns ${utc_columns} are in UTC format.   ELSE   add result to report   Test Failed: At least one column in not in UTC Format.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
	log test to report per step     ${jira_id_tag}
    #Teardown Test

The columns contains a date in UTC(without non_null validation)
    FOR     ${column}   IN  @{utc_columns}
        run keyword if      ${Connection}==False     Connect to CDL Databricks
        Check UTC format for column     ${system}   ${table}   ${column}
        END

    run keyword if   '${test_status}'=='PASS'   add result to report   Test Passed: \nAll columns ${utc_columns} are in UTC format.   ELSE   add result to report   Test Failed: At least one column in not in UTC Format.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
	log test to report per step     ${jira_id_tag}
    #Teardown Test

I expect that specified source systems that specified columns contains a date in UTC(without non_null validation)
    [Arguments]         ${SOURCES_UTC}       ${COLUMNS_UTC}
     Validate for specified source systems that specified columns contains a date in UTC(without non_null validation)    ${SOURCES_UTC}       ${COLUMNS_UTC}

I expect that for a set of source list that specified columns contains a date in UTC(without non_null validation)
    [Arguments]         ${SOURCES_UTC}       ${COLUMNS_UTC}
     Validate for a set of source list that specified columns contains a date in UTC(without non_null validation)    ${SOURCES_UTC}       ${COLUMNS_UTC}

Validate for specified source systems that specified columns contains a date in UTC(without non_null validation)
    [Arguments]         ${SOURCES_UTC}       ${COLUMNS_UTC}
    Set system source for test
    FOR  ${source}  IN  @{SOURCES_UTC}
        run keyword if      ${Connection}==False     Connect to CDL Databricks
        set test variable   ${source}
        log to console     Checking for ${source}
         ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
        FOR     ${column}   IN  @{COLUMNS_UTC}
            run keyword if      ${Connection}==False     Connect to CDL Databricks
            Check UTC format for specific alias      ${system}   ${table}   ${column}  ${alias}
        END
    END
    run keyword if    '${test_status}'=='PASS' and ${ends_with_0000}==True    add result to report    Test Passed:\nAll columns ${COLUMNS_UTC} are in UTC format.    ELSE IF    '${test_status}'=='FAIL'    add result to report    Test Failed: At least one column in not in UTC Format.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
	log test to report per step    ${SOURCES_UTC}_${COLUMNS_UTC}[0]
    #Teardown Test

Validate for a set of source list that specified columns contains a date in UTC(without non_null validation)
    [Arguments]         ${SOURCES_UTC}       ${COLUMNS_UTC}
    Set system source for test
    FOR  ${source}  IN  @{SOURCES_UTC}
        run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
        set test variable   ${source}
        log to console     Checking for ${source}
         ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
        FOR     ${column}   IN  @{COLUMNS_UTC}
            run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
            Check UTC format for specific alias for a set of source list      ${system}   ${table}   ${column}  ${alias}
        END
    END
    run keyword if    '${test_status}'=='PASS' and ${ends_with_0000}==True    add result to report    Test Passed:\n All columns ${COLUMNS_UTC} are in UTC format.    ELSE IF    '${test_status}'=='FAIL'    add result to report    Test Failed: At least one column is not in UTC Format.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
	log test to report per step     ${SOURCES_UTC}_${COLUMNS_UTC}[0]
    #Teardown Test

Validate that specified columns contains a date in UTC
    [Arguments]         ${source}       ${UTC_COLUMNS}
     ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
    FOR     ${column}   IN  @{UTC_COLUMNS}
        ${non_nulls}=   run keyword  count non nulls  ${system}   ${table}  ${column}
        run keyword if      ${non_nulls}==0      Fail test and log error    ${column}
        run keyword if      ${non_nulls}!=0      Check UTC format for specific alias      ${system}   ${table}   ${column}  ${alias}
    END

    run keyword if   '${test_status}'=='PASS'   add result to report   Test Passed: \nAll columns ${utc_columns} are in UTC format.   ELSE   add result to report   Test Failed: At least one column in not in UTC Format.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    Teardown Test

Validate for specified source systems that specified columns contains a date in UTC
    [Arguments]         ${SOURCES_UTC}       ${COLUMNS_UTC}
    Set system source for test
    FOR  ${source}  IN  @{SOURCES_UTC}
        set test variable   ${source}
         ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
        FOR     ${column}   IN  @{COLUMNS_UTC}
            ${non_nulls}=   run keyword  count non nulls  ${system}   ${table}  ${column}
            run keyword if      ${non_nulls}==0      Fail test and log error    ${column}
            run keyword if      ${non_nulls}!=0      Check UTC format for specific alias      ${system}   ${table}   ${column}  ${alias}
        END
    END
    run keyword if   '${test_status}'=='PASS'   add result to report   Test Passed: \nAll columns ${COLUMNS_UTC} are in UTC format.   ELSE   add result to report   Test Failed: At least one column in not in UTC Format.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    log test to report per step      ${SOURCES_UTC}_${COLUMNS_UTC}[0]
   # Teardown Test

Fail test and log error
    [Arguments]            ${var}
    set test variable      ${test_status}  FAIL
    add result to report   Test Failed, ${var} not tested

Check UTC format for specific alias
    [Arguments]    ${system}   ${table}   ${column}  ${alias}
    ${utc_date}=   run keyword and continue on failure      return_date_as_utc_source   ${system}   ${table}   ${column}  ${alias}
    ${is_not_none}=   run keyword and return status         Should Be Equal As Strings   '${utc_date}'   'None'
    ${ends_with_0000}=    run keyword and return status     should end with    ${utc_date}   +0000
    log    ${utc_date}
    run keyword if         ${ends_with_0000}==False and ${is_not_none}==False   set test variable   ${test_result}   False
    run keyword if         ${is_not_none}==True      Validate that column contains null as required by filter condition    ${column}    ${alias}
    run keyword if         ${ends_with_0000}==True    set test variable   ${test_result}   True
    set test variable      ${ends_with_0000}
    log to console  Source ${alias} checked for column ${column}
    run keyword if    ${test_result}==False    set test variable   ${test_status}  FAIL
    run keyword if    ${test_result}==True and ${ends_with_0000}==True     add result to report   The column ${column} ends with +0000 offset   ELSE IF  ${test_result}==False and ${is_not_none}==True   add result to report   The column ${column} does note end with +0000 offset.
    run keyword if    ${test_result}==True and ${ends_with_0000}==True     add result to report   Test Passed: The ${column} column contains a record in UTC \n   ELSE IF  ${test_result}==False and ${is_not_none}==True   add result to report   Test Failed: The ${column} column contains a record that is not in UTC \n

Check UTC format for specific alias for a set of source list
    [Arguments]    ${system}   ${table}   ${column}  ${alias}
    ${utc_date}=   run keyword and continue on failure      return_date_as_utc_source   ${system}   ${table}   ${column}  ${alias}
    ${is_not_none}=   run keyword and return status         Should Be Equal As Strings   '${utc_date}'   'None'
    ${ends_with_0000}=    run keyword and return status     should end with    ${utc_date}   +0000
    log    ${utc_date}
    run keyword if         ${ends_with_0000}==False and ${is_not_none}==False   set test variable   ${test_result}   False
    run keyword if         ${is_not_none}==True      Validate that column contains null as required by filter condition for a set of source list    ${column}    ${alias}
    run keyword if         ${ends_with_0000}==True    set test variable   ${test_result}   True
    set test variable      ${ends_with_0000}
    log to console  Source ${alias} checked for column ${column}
    run keyword if    ${test_result}==False    set test variable   ${test_status}  FAIL
    run keyword if    ${test_result}==True and ${ends_with_0000}==True     add result to report   The column ${column} ends with +0000 offset   ELSE IF  ${test_result}==False and ${is_not_none}==True    add result to report   The column ${column} does note end with +0000 offset.
    run keyword if    ${test_result}==True and ${ends_with_0000}==True     add result to report   Test Passed: The ${column} column contains a record in UTC \n   ELSE IF  ${test_result}==False and ${is_not_none}==True   add result to report   Test Failed: The ${column} column contains a record that is not in UTC \n

Validate that column contains null as required by filter condition
    [Arguments]         ${column}    ${alias}
     FOR  ${i}   IN  @{UTC_COLUMS_WITH_NULL_SOURCE}
     ${status}    run keyword and return status      Should Be Equal As Strings    ${alias}    ${i}
     run keyword if        ${status}== True    Validate that column for source in the list   ${column}    ${UTC_COLUMS_WITH_NULL_1}
     EXIT FOR LOOP IF      ${status}== True
     END
     ${status}     Run keyword and return status    Variable Should Exist  ${UTC_COLUMS_WITH_NULL_2_SOURCE}
     Run Keyword if     ${status} == True   Check second UTC list    ${alias}    ${column}

Check second UTC list
    [Arguments]     ${alias}    ${column}
     FOR  ${i}   IN  @{UTC_COLUMS_WITH_NULL_2_SOURCE}
     ${status}    run keyword and return status      Should Be Equal As Strings    ${alias}    ${i}
     run keyword if        ${status}== True    Validate that column for source in the list   ${column}    ${UTC_COLUMS_WITH_NULL_2}
     EXIT FOR LOOP IF      ${status}== True
     END

Validate that column contains null as required by filter condition for a set of source list
    [Arguments]         ${column}    ${alias}
     FOR  ${i}   IN  @{UTC_COLUMS_WITH_NULL_SOURCE}
     ${status}    run keyword and return status      Should Be Equal As Strings    ${alias}    ${i}
     run keyword if        ${status}== True    Validate that column for source in the list   ${column}    ${UTC_COLUMS_WITH_NULL_1}
     EXIT FOR LOOP IF      ${status}== True
     END

Validate that column for source in the list
    [Arguments]       ${column}    ${list}
    FOR    ${i}   IN   @{list}
    ${temp}    run keyword and return status      Should Be Equal As Strings     ${i}      ${column}
    run keyword if    ${temp}==True     set test variable   ${test_result}   True     ELSE      set test variable   ${test_result}   False
    run keyword if    ${temp}==True     set test variable   ${test_status}  PASS      ELSE      set test variable   ${test_status}   FAIL
    run keyword if    ${temp}==True     add result to report   The column ${column} does note end with +0000 offset.
    run keyword if    ${temp}==True     add result to report   Test Passed: The ${column} contains null values instead of UTC as per requirement \n
    EXIT FOR LOOP IF       ${temp}==True
    END
    run keyword if     ${test_result}==False    add result to report   !Test Failed: The ${column} should follow UTC but it is not. \n

Check UTC format for column
    [Arguments]    ${system}   ${table}   ${column}
    ${utc_date}=   run keyword    return_date_as_utc     ${system}   ${table}   ${column}
    ${is_not_none}=   run keyword and return status       should not be equal as strings   '${utc_date}'   'None'
    ${ends_with_0000}=   run keyword and return status    should end with    ${utc_date}   +0000
    log    ${utc_date}
    log to console     Checked for ${column}
    ${test_result}=  run keyword and return status      should be true   ${is_not_none}==${ends_with_0000}
    run keyword if  ${test_result}==False   set test variable   ${test_status}  FAIL
    run keyword if   ${test_result}==True   add result to report   The column ${column} ends with +0000 offset   ELSE   add result to report   The column ${column} does note end with +0000 offset.
    run keyword if   ${test_result}==True  add result to report   Test Passed: The ${column} column contains a record in UTC\n   ELSE   add result to report   Test Failed: The ${column} column contains a record that is not in UTC\n

The columns contains a date in UTC for ${source}
    run keyword if      '${source}' == 'elims'    set test variable   @{utc_columns}  @{utc_columns_elims}
    run keyword if      '${source}' == 'sap'    set test variable   @{utc_columns}  @{utc_columns_sap}
    FOR     ${column}   IN  @{utc_columns}
        ${non_nulls}=   run keyword  count non nulls  ${system}   ${table}  ${column}
        run keyword if      ${non_nulls}==0   column not tested ${column}
        run keyword if      ${non_nulls}==0   run keyword    CONTINUE FOR LOOP
        ${utc_date}=   run keyword and continue on failure    return_date_as_utc    ${system}   ${table}   ${column}
        ${is_not_none}=   run keyword and ignore error   should not be equal as strings   '${utc_date}'   'None'
        ${ends_with_0000}=   run keyword and ignore error  should end with    ${utc_date}   +0000

        ${test_result}=  run keyword and ignore error  should be true   '${is_not_none}[0]'=='PASS'   '${ends_with_0000}[0]'=='PASS'

        run keyword if   '${test_result}[0]'=='FAIL'   set test variable   ${test_status}  FAIL

        run keyword if   '${test_result}[0]'=='PASS'   add result to report   The column ${column} ends with +0000 offset   ELSE   add result to report   The column ${column} does note end with +0000 offset.
        run keyword if   '${test_result}[0]'=='PASS'   add result to report   Test Passed: The ${column} column contains a record in UTC\n   ELSE   add result to report   Test Failed: The ${column} column contains a record that is not in UTC\n
    END

    run keyword if   '${test_status}'=='PASS'   add result to report   Test Passed: \nAll columns ${utc_columns} are in UTC format.   ELSE   add result to report   Test Failed: At least one column in not in UTC Format.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    Teardown Test

Column not tested ${col}
    run keyword and continue on failure     add result to report   Column ${col} contains all NULLs, Test Skipped.
    log to console  [INFO] Column ${col} contains all NULLs, Test Skipped.

The columns do not contain any whitespace
    FOR     ${column}   IN  @{whitespace_columns}
        run keyword     Check ${column} for whitespace
    END
    Teardown Test

Check ${column} for whitespace
    ${count}=   run count query with conditions     ${table}     ${system}  ${column} like ' %' or ${column} like '% '
    run keyword and continue on failure  should be equal as numbers  ${count}   0
    run keyword and continue on failure  add result to report   Expected: 0\nActual: ${count}

    run keyword if  ${count}==0    add result to report  Test Passed :The field ${column} contains no whitespace\n        ELSE        add result to report    Test Failed: Column ${column} contains whitespace\n
    run keyword if  ${count}!=0    set test variable   ${test_status}  FAIL


The columns are of the correct datatype
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${columns_and_datatypes_from_dbfs}=        Run Keyword  return column descriptions from table        ${table}  ${system}
    FOR     ${result}   IN  @{columns_and_datatypes_from_dbfs}
        ${column}=    set variable  ${result}[name]
        ${datatype}=     get from dictionary     ${COLUMNS_AND_DATATYPES}   ${column}
        ${datatype_from_dbfs}=    set variable  ${result}[type]
        ${test_result}=   run keyword and ignore error   dictionary should contain item   ${COLUMNS_AND_DATATYPES}   ${column}   ${datatype_from_dbfs}

        run keyword if   '${test_result}[0]'=='FAIL'   set test variable   ${test_status}  FAIL
        run keyword if   '${test_result}[0]'=='FAIL'   log to console   '${test_result}[1]'

        run keyword  add result to report  Expected: ${datatype}
        run keyword  add result to report  Actual: ${datatype_from_dbfs}
        run keyword  add result to report   ${test_result}[0]: Column ${column} should be ${datatype}\n
    END
    run keyword if   '${test_status}'=='PASS'   add result to report   \nTest Passed: All Columns are correct datatype.   ELSE   add result to report   \nTest Failed: At least one datatype was incorrect.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    log test to report per step     ${jira_id_tag}
   # Teardown Test

The target table has ${number_of_columns} columns
    ${columns}=   get columns from table  ${system}.${table}
    ${column_count}=   get length  ${columns}
    run keyword and continue on failure  add result to report   Checking number of columns for ${system}.${table}
    run keyword and continue on failure  add result to report   Expected : ${number_of_columns}
    run keyword and continue on failure  add result to report   Actual : ${column_count}
    run keyword and continue on failure  should be equal as numbers     ${column_count}     ${number_of_columns}
    run keyword if  ${column_count}==${number_of_columns}     add result to report  Test Passed : The ${system}.${table} expected ${number_of_columns} columns and returned ${column_count} columns\n        ELSE        add result to report    Test Failed: The ${system}.${table} expected ${number_of_columns} columns and returned ${column_count} columns
    log test to report per step     ${jira_id_tag}
    #Teardown Test

The target view contains correct number of columns
    ${columns}=   get columns from view  ${system}.${table}
    ${act_column_count}=   get length  ${columns}
    run keyword and continue on failure  add result to report   Checking number of columns for ${system}.${table}
    run keyword and continue on failure  add result to report   Expected : ${column_count}
    run keyword and continue on failure  add result to report   Actual : ${act_column_count}
    run keyword and continue on failure  should be equal as numbers     ${act_column_count}     ${column_count}
    run keyword if  ${act_column_count}==${column_count}     add result to report  Test Passed : The ${system}.${table} expected ${column_count} columns and returned ${act_column_count} columns\n        ELSE        add result to report    Test Failed: The ${system}.${table} expected ${column_count} columns and returned ${act_column_count} columns
	log test to report per step     ${jira_id_tag}
    #Teardown Test

The uniqueness of the primary keys are as expected for the curated table
    FOR    ${table}        IN       @{PRIMARY_KEYS}
        ${primary_keys}=    get from dictionary  ${PRIMARY_KEYS}    ${table}
        ${results_from_select_all}=        run keyword and continue on failure  return count of table      ${system}.${table}
         run keyword if       ${results_from_select_all}==0     Fail test and log error    ${table}
         run keyword if       ${results_from_select_all}!=0     Validate that table contains uniqueness of the primary keys      ${system}    ${table}    ${primary_keys}    ${results_from_select_all}
    END
    run keyword if   '${test_status}'=='PASS'   add result to report   Test Passed: \nUniqueness of the primary keys are as expected for the curated table   ELSE   add result to report   Test Failed: At least one primary keys are not as expected.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    log test to report per step     ${jira_id_tag}
    #Teardown Test

Validate that table contains uniqueness of the primary keys
   [Arguments]     ${system}    ${table}    ${primary_keys}    ${results_from_select_all}
   ${results_from_pks}=    run keyword and continue on failure  return count multiple columns from table         ${system}.${table}     ${primary_keys}
        run keyword and continue on failure  should be equal as numbers  ${results_from_select_all}    ${results_from_pks}
        add result to report     Select all result: ${results_from_select_all} \nSelect distinct primary keys result: ${results_from_pks}\n
        run keyword if  ${results_from_select_all}==${results_from_pks}     add result to report  Test Passed : Counts are equal so there are no duplicate primary keys in ${system}.${table}\n        ELSE        add result to report    Test Failed: Duplicates found ${results_from_select_all} is not equal ${results_from_pks}

The Columns does not contain null values
    ${count}=   Run Keyword  Check For Null In Columns   ${system}.${table}  ${PRIMARY_KEYS_LIST}
    run keyword and continue on failure  should be equal as numbers  ${count}    0
    run keyword if  ${count}==0     add result to report  Test Passed : Counts is Zero so there are no null Value primary keys in ${system}.${table}\n        ELSE        add result to report    Test Failed: Duplicates found There are ${Count} Primary Keys Records which contains null values
    Teardown Test

The Columns contain not null values
    ${count}=   Run Keyword  Check For Null In Columns   ${system}.${table}  ${NOT_NULL_COLUMNS}
    run keyword and continue on failure  should be equal as numbers  ${count}    0
    run keyword if  ${count}==0     add result to report  Test Passed : Null values count is zero so ${NOT_NULL_COLUMNS} is a "NOT NULL" column.\n        ELSE        add result to report    Test Failed : "${Count}" - Null values found in ${NOT_NULL_COLUMNS}.
    Teardown Test

Validate that columns contain all nulls for the source system
   ${temp}       Run keyword and return status      set test variable   @{source_system}    @{ALL_NULL_SOURCES}
   Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Columns contain all nulls for the source system
    END
	log test to report per step     ${source_system}
    #Teardown Test

Validate that columns contain all nulls when columns are same for multiple sources
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{ALL_NULL_SOURCES}
    Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Columns contain all nulls for the source system when columns are same for multiple sources
    END
	log test to report per step     ${source_system}
    #Teardown Test

Validate that columns contain all nulls when columns are same for set of sources
    [Arguments]    ${sourceList}     ${columns}
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{sourceList}
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Columns contain all nulls for the source system when columns are same for set of sources     @{columns}
    END
    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non nulls  ELSE    add result to report  Test Passed: Columns contain all nulls
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
	log test to report per step    ${sourceList}_${columns}[0]
    #Teardown Test

Validate that columns contain all nulls with condition when columns are same for set of sources
    [Arguments]    ${sourceList}     ${columns}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{sourceList}
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Columns contain all nulls with condition for the source system when columns are same for set of sources     @{columns}
    END
    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non nulls  ELSE    add result to report  Test Passed: Columns contain all nulls
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
	log test to report per step     ${sourceList}_${columns}[0]
    #Teardown Test

Validate that columns contain all predefined values when columns are same for multiple sources
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{ALL_PREDEFINED_SOURCES}
   Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Columns contain all predefined values for the source system when columns are same for multiple sources
    END
	log test to report per step     ${source_system}
    #Teardown Test

Validate that columns contain all predefined values for source
   ${temp}       Run keyword and return status      set test variable   @{source_system}    @{ALL_PREDEFINED_SOURCES}
   Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        log    ${i}
        Columns contain all predefined values for the source system
    END
	log test to report per step     ${source_system}
    #Teardown Test

Columns contain all nulls for the source system
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${i}
    set test variable   ${alias}
    ${temp2}       Run keyword and return status      Set null value column list for source
    run keyword if    ${temp2}==False      set test variable   @{null_columns}    @{null_columns_sap}
    Add information to all null columns report  ${alias}
    ${column count}   Get Length   ${null_columns}
    Run keyword if   ${column count}>15     Connect to databrick and Check null and add result to report
    Run keyword if   ${column count}<15     Check null and add result to report

Columns contain all nulls for the source system when columns are same for set of sources
    [Arguments]     @{null_columns}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${i}
    set test variable   ${alias}
    set test variable   @{null_columns}  @{null_columns}
    Add information to all null columns report   ${alias}
    ${column count}   Get Length   ${null_columns}
    Run keyword if   ${column count}>=15       Connect to databrick and Check null and add result to report
    Run keyword if   ${column count}<15        Check null and add result to report

Columns contain all nulls with condition for the source system when columns are same for set of sources
    [Arguments]     @{null_columns}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${i}
    set test variable   ${alias}
    set test variable   @{null_columns}  @{null_columns}
    Add information to all null columns report  ${alias}
    ${column count}   Get Length   ${null_columns}
    Run keyword if   ${column count}>=15       Connect to databrick and Check null with condition and add result to report
    Run keyword if   ${column count}<15        Null check with condition and add result to report

Columns contain all nulls for the source system when columns are same for multiple sources
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${i}
    set test variable   ${alias}
    set test variable   @{null_columns}  @{null_columns}
    Add information to all null columns report     ${alias}
    ${column count}   Get Length   ${null_columns}
    Run keyword if   ${column count}>=15       Connect to databrick and Check null and add result to report
    Run keyword if   ${column count}<15       Check null and add result to report

Set null value column list for source
    run keyword if      '${i}' == 'atlas'           set test variable   @{null_columns}  @{null_columns_atlas}
    run keyword if      '${i}' == 'europe2'         set test variable   @{null_columns}  @{null_columns_europe2}
    run keyword if      '${i}' == 'sustain'         set test variable   @{null_columns}  @{null_columns_sustain}
    run keyword if      '${i}' == 'panda'           set test variable   @{null_columns}  @{null_columns_panda}
    run keyword if      '${i}' == 'elims'           set test variable   @{null_columns}  @{null_columns_elims}
    run keyword if      '${i}' == 'maximo'          set test variable   @{null_columns}  @{null_columns_maximo}
    run keyword if      '${i}' == 'p01'             set test variable   @{null_columns}  @{null_columns_p01}
    run keyword if      '${i}' == 'btb_na'          set test variable   @{null_columns}  @{null_columns_btb_na}
    run keyword if      '${i}' == 'btb_latam'       set test variable   @{null_columns}  @{null_columns_btb_latam}
    run keyword if      '${i}' == 'taishan'         set test variable   @{null_columns}  @{null_columns_taishan}
    run keyword if      '${i}' == 'sap'             set test variable   @{null_columns}  @{null_columns_sap}
    run keyword if      '${i}' == 'bw2'             set test variable   @{null_columns}  @{null_columns_bw2}
    run keyword if      '${i}' == 'deu'             set test variable   @{null_columns}  @{null_columns_deu}
    run keyword if      '${i}' == 'etq_instinct'    set test variable   @{null_columns}  @{null_columns_etq_instinct}
    run keyword if      '${i}' == 'gmd'             set test variable   @{null_columns}  @{null_columns_gmd}
    run keyword if      '${i}' == 'geu'             set test variable   @{null_columns}  @{null_columns_geu}
    run keyword if      '${i}' == 'bwi'             set test variable   @{null_columns}  @{null_columns_bwi}
    run keyword if      '${i}' == 'bba'             set test variable   @{null_columns}  @{null_columns_bba}
    run keyword if      '${i}' == 'mrs'             set test variable   @{null_columns}  @{null_columns_mrs}
    run keyword if      '${i}' == 'hcs'             set test variable   @{null_columns}  @{null_columns_hcs}
    run keyword if      '${i}' == 'trackwise'       set test variable   @{null_columns}  @{null_columns_trackwise}
    run keyword if      '${i}' == 'prd_9pq'         set test variable   @{null_columns}  @{null_columns_9pq}
    run keyword if      '${i}' == 'prd_9ec'         set test variable   @{null_columns}  @{null_columns_9ec}
    run keyword if      '${i}' == 'hmd'             set test variable   @{null_columns}  @{null_columns_hmd}

Columns contain all predefined values for the source system when columns are same for multiple sources
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${i}
    set test variable   ${alias}
    FOR    ${column}        IN       @{COLUMNS_WITH_PREDEFINED_VALUES}
    ${value_init}=    get from dictionary  ${COLUMNS_WITH_PREDEFINED_VALUES}    ${column}
    ${value_str}=    Convert To String    ${value_init}
    ${value_is_list}=    run keyword and return status   Should Contain      ${value_str}   [
    run keyword if      ${Connection}==False     Connect to CDL Databricks
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    run keyword if  ${value_is_list}==True    Get count table with predefined multiple values    ${system}.${table}    ${column}    ${value_str}     ${value_init}
    run keyword if  ${value_is_list}==False    Get count table with predefined values    ${system}.${table}    ${column}    ${value_init}
    add system information to predefined report columns   ${system}.${table}   ${alias}  ${column}    ${value}
    run keyword     add result to report    Expected = ${table_count}
    run keyword if  ${val}==${table_count}     add result to report  Test Passed : Column ${column} contains ${value} values\n        ELSE        add result to report    Test Failed: Column ${column} contains non ${value} values\n
    run keyword if  ${val}!=${table_count}     set test variable   ${test_status}  FAIL
    run keyword if  ${val}==0     set test variable   ${test_status}  FAIL
    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non ${value} values  ELSE    add result to report  Test Passed: Columns contain ${value} values
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
    Log to console    Values are checked for ${column}
    END

Columns contain all predefined values for the source system
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${i}
    set test variable   ${alias}
    FOR    ${source}        IN       @{COLUMNS_WITH_PREDEFINED_VALUES_PER_SOURCE}
    ${details}=    get from dictionary  ${COLUMNS_WITH_PREDEFINED_VALUES_PER_SOURCE}    ${source}
    ${status}     Run keyword and return status   Should be equal as strings    ${source}    ${alias}
    Run keyword if   ${status}==True     Check predefined values for specific source  ${details}
    END

Check predefined values for specific source
    [Arguments]    ${details}
    ${column}=   Fetch From Left    ${details}    :
    ${value}=    Fetch From Right   ${details}    :
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    add system information to predefined report columns   ${system}.${table}   ${alias}  ${column}    ${value}
    ${val}=    return count of table with conditions  ${system}.${table}        ${column} in ("${value}") and src_sys_cd == '${alias}'
    run keyword     add result to report    Expected = ${table_count}
    run keyword if  ${val}==${table_count}     add result to report  Test Passed : Column ${column} contains ${value} values\n        ELSE        add result to report    Test Failed: Column ${column} contains non ${value} values\n
    run keyword if  ${val}!=${table_count}     set test variable   ${test_status}  FAIL
    run keyword if  ${val}==0     set test variable   ${test_status}  FAIL
    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non ${value} values  ELSE    add result to report  Test Passed: Columns contain ${value} values
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
    Log to console    Values are checked for  ${details}

Validate that columns contain all predefined values in views
   FOR    ${column}        IN       @{COLUMNS_WITH_PREDEFINED_VALUES}
   ${table_count}=    return count of table with conditions  ${system}.${view}       src_sys_cd == '${source_system}'
   run keyword if      ${Connection}==False     Connect to CDL Databricks
   ${value_init}=    get from dictionary  ${COLUMNS_WITH_PREDEFINED_VALUES}    ${column}
   ${value_str}=    Convert To String    ${value_init}
   ${value_is_list}=    run keyword and return status   Should Contain      ${value_str}   [
   run keyword if  ${value_is_list}==True    Get count table with predefined multiple values    ${system}.${view}    ${column}    ${value_str}
   run keyword if  ${value_is_list}==False    Get count table with predefined values    ${system}.${view}    ${column}    ${value_init}
   add system information to predefined report columns   ${system}.${view}   ${source_system}  ${column}    ${value}
   run keyword     add result to report    Expected = ${table_count}
   run keyword if  ${val}==${table_count}     add result to report  Test Passed : Column ${column} contains ${value} values\n        ELSE        add result to report    Test Failed: Column ${column} contains non ${value} values\n
   run keyword if  ${val}!=${table_count}     set test variable   ${test_status}  FAIL

   run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non ${value} values  ELSE    add result to report  Test Passed: Columns contain ${value} values
   run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
   Log to console    Values are checked for ${column}
   END
   Teardown Test

Get count table with predefined multiple values
    [Arguments]       ${view}    ${column}    ${value_str}     ${value_init}
     ${value}=    Remove String    ${value_str}    "    [    ]
     ${val}=  return count of table with conditions        ${view}       ${column} in (${value})
     Run keyword if  ${val}==0    Check values with whitespaces     ${view}    ${column}    ${value_init}
     Run keyword if  ${val}!=0    set test variable   ${val}      ${val}
     set test variable   ${value}     ${value}

Check values with whitespaces
    [Arguments]       ${view}    ${column}   ${value_init}
     ${templist}    Create List
     FOR  ${i}  IN  @{value_init}
     ${tempString}     Catenate  ${column} like ('% ${i} %') OR
     Append To List  ${templist}    ${tempString}
     END
     ${templist}    Convert To String     ${templist}
     ${templist}    Remove String         ${templist}      [     OR"]     "    ,    ]
     ${val}=  return count of table with conditions        ${view}       ${templist}
     set test variable   ${val}       ${val}
     run keyword if  ${val}==0     set test variable   ${test_status}  FAIL

Get count table with predefined values
    [Arguments]       ${view}    ${column}    ${value_init}
     ${value}=    Catenate    "${value_init}"
     ${val}=  return count of table with conditions        ${view}       ${column} in (${value})
     set test variable   ${val}      ${val}
     set test variable   ${value}     ${value}

I expect that the columns contain predefined rule in views
   Validate that the columns contain predefined rule

Validate that the columns contain predefined rule
    FOR    ${column}        IN       @{COLUMN_FOR_PREDEFINED_RULE}
    ${origin_col}=    get from dictionary  ${COLUMN_FOR_PREDEFINED_RULE}    ${column}
    ${expected_id}=       run sql query to get value from view column    ${column}   ${system}     ${view}    ${sample1_view_condition}
    ${expected_id}        Convert To String    ${expected_id}
    ${expected_result}    Remove String    ${expected_id}       (    '     ,    )    [    ]
    ${expected_result}    Catenate  ${PREDEFINED_VALUES_RULE}${expected_result}
    Check actual data    ${expected_result}     ${origin_col}
    run keyword if   '${test_status}'=='PASS'   add result to report   \nTest Passed: All values were as expected in target ${system}.${view} column ${column}    ELSE   add result to report   \nTest Failed: At least one values was not as expected in target ${system}.${view}column ${column}.
    END
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    Teardown Test

Check actual data
    [Arguments]       ${expected_result}     ${origin_col}
    FOR   ${i}     IN   @{COLUMN_WITH_PREDEFINED_RULE}
    ${actual_result}=     run sql query to get value from view column    ${i}    ${system}     ${view}    ${sample1_view_condition}
    ${actual_result}        Convert To String    ${actual_result}
    ${actual_result}    Remove String    ${actual_result}       (    '     ,    )    [    ]
    log   Expected Result: ${expected_result}
    log    Actual Result: ${actual_result}
    ${result}    run keyword and return status      Should Be Equal As Strings  ${expected_result}    ${actual_result}
    run keyword and continue on failure     should be true  ${result}
    run keyword if  ${result}  add result to report       Expected Result: ${expected_result}
    run keyword if  ${result}  add result to report       Actual Result: ${actual_result}
    run keyword if  ${result}  add result to report       Test Passed for ${system}.${view} column ${i}: Expected Result found in Actual Result\n  ELSE    add result to report  Test Failed: Expected Result not found in Actual Result\n
    run keyword if  ${result}  add result to report       Expected rule ${PREDEFINED_VALUES_RULE}${origin_col} was applied to column.
    run keyword if  not ${result}  set test variable   ${test_status}  FAIL
    END

Connect to databrick and Check null and add result to report
    Check null and add result to report
    Disconnect from Databricks

Connect to databrick and Check null with condition and add result to report
    Null check with condition and add result to report
    Disconnect from Databricks

Check null and add result to report
    run keyword if      ${Connection}==False     Connect to CDL Databricks
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    ${templist}    Create List
    FOR   ${column}   IN   @{null_columns}
    ${tempString}     Catenate  ${column}  IS NULL  AND
    Append To List    ${templist}     ${tempString}
    END
    ${templist}     Convert To String      ${templist}
    log     ${templist}
    ${templist}     Remove String          ${templist}       [     AND']     '   ,
    log     ${templist}
    ${nulls}=    return count of table with conditions  ${system}.${table}        ${templist} and src_sys_cd == '${alias}'
    run keyword if       ${nulls}==0      Data not available
    run keyword       add result to report    Expected = ${table_count}
    ${status}         Run keyword and return status    Should be true    '${nulls}'=='${table_count}' and '${nulls}'!='0'
    run keyword if    ${status} ==True     Compare results and add to report     ${nulls}     ${table_count}    ${templist}
    run keyword if    ${status} ==False    Check each column and add result      ${table_count}    ${alias}      ${null_columns}

Null check with condition and add result to report
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    ${nulls}=    return count of table null column check with conditions  ${system}.${table}       ${alias}       ${null_columns}
    run keyword if       ${nulls}==0      Data not available
    run keyword     add result to report    Expected = ${table_count}
    run keyword if  '${nulls}'=='${table_count}' and '${nulls}'!='0'     add result to report    Test Passed:Column ${null_columns} contains all nulls\n         ELSE IF    '${nulls}'!='0' and '${nulls}'!='${table_count}'    add result to report    Test Failed:Column ${null_columns} contains non null values\n
    run keyword if  ${nulls}!=${table_count}     set test variable   ${test_status}  FAIL
    Log to console    Columns are checked for ${i}

Compare results and add to report
    [Arguments]      ${nulls}    ${table_count}    ${templist}
    run keyword if  '${nulls}'=='${table_count}' and '${nulls}'!='0'     add result to report    Test Passed:Columns ${templist} contains all nulls\n         ELSE IF    '${nulls}'!='0' and '${nulls}'!='${table_count}'    add result to report    Test Failed:Column ${column} contains non null values\n
    run keyword if  ${nulls}!=${table_count}     set test variable   ${test_status}  FAIL
    Log to console    Columns are checked for ${alias}

Check each column and add result
   [Arguments]      ${table_count}    ${alias}    ${null_columns}
    FOR     ${column}   IN  @{null_columns}
    run keyword if      ${Connection}==False     Connect to CDL Databricks
    ${nulls}=    return count of table with conditions  ${system}.${table}        ${column} IS NULL and src_sys_cd == '${alias}'
    run keyword if       ${nulls}==0      Data not available
    run keyword     add result to report    Expected = ${table_count}
    run keyword if  '${nulls}'=='${table_count}' and '${nulls}'!='0'     add result to report    Test Passed:Column ${column} contains all nulls\n         ELSE IF    '${nulls}'!='0' and '${nulls}'!='${table_count}'    add result to report    Test Failed:Column ${column} contains non null values\n
    run keyword if  ${nulls}!=${table_count}     set test variable   ${test_status}  FAIL
    Log to console    Column ${column} is checked for ${alias}
    END

I expect that the curation logic has been applied
    The curation logic has been applied

I expect that the curation logic has been applied for 9pq and 9ec
    The curation logic has been applied for 9pq and 9ec

The curation logic has been applied
    ${expected_filter}=    Get Variable Value    ${expected_system_result}   All
    ${src_sys_sql_dict}=      get source system sql    ${table}   ${expected_filter}

    FOR     ${src_sql}  IN  @{src_sys_sql_dict}
        ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${src_sql}
        ${query}=   get test query  ${table}    ${src_sql}   ${alias}
        set test variable    ${query}       ${query}
        set test variable    ${src_sql}     ${src_sql}
        ${actual_result}=   run sql query full  ${query}
        ${expected}=    get from dictionary     ${EXPECTED_RESULT}    ${src_sql}
        Set test variable     ${actual_result}   ${actual_result}
        Set test variable     ${expected}    ${expected}
        Compare actual and expected result and add to report
    END

    run keyword if   '${test_status}'=='PASS'   add result to report   \nTest Passed: All values were as expected in target after curation.   ELSE   add result to report   \nTest Failed: At least one values was not as expected in target after curation.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    Teardown Test

I expect that the EDM location is correct
    The location of the EDM is correct

The location of the EDM is correct
    Log to console    ${table}
    Log to console    ${table_location}
    ${actual_location}=   return location of table on dbfs   ${table}   l1
    Log to console    ${actual_location}
    run keyword     add result to report   Actual Result: ${actual_location}
    run keyword     add result to report   Expected Result: ${table_location}\n
    run keyword and continue on failure     should be equal   ${actual_location}   ${table_location}
    run keyword if  "${actual_location}"=="${table_location}"   add result to report   Test Passed: Table location was equal to expected location \n  ELSE    add result to report  Test Failed: Table location was not equal to expected location\n
    log test to report per step     ${jira_id_tag}
   #Teardown Test

I expect that the column values are a hashtag for the sap source system
    The column values are a hashtag for source system sap

The column values are a hashtag for source system sap
        FOR     ${source}   IN   @{sap_sources}
            ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}

            ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'

            FOR     ${column}   IN  @{hashtag_columns_sap}
                ${hashtags}=    return count of table with conditions  ${system}.${table}        ${column} = '#' and src_sys_cd == '${alias}'
                run keyword     add result to report    Expected = ${table_count}
                run keyword if  ${hashtags}==${table_count}     add result to report     Test Passed : Column ${column} contains all hashtags for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains non hashtag values\n
                run keyword if  ${hashtags}!=${table_count}     set test variable   ${test_status}  FAIL
            END
        END

            run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non hashtags  ELSE    add result to report  Test Passed: Columns contain all hashtags
            run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
        log test to report per step     ${jira_id_tag}
        #Teardown Test

I expect that the column values are a hashtag for source system when columns are same for multiple sources
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{HASHTAG_SOURCES}
    Set system source for test
    FOR  ${i}  IN  @{source_system}
    set test variable   ${i}
    The column values are a hashtag for source system when columns are same for multiple sources
    END
    log test to report per step     ${jira_id_tag}
   # Teardown Test

Set system source for test
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{source_system}
    run keyword if  ${temp}== False                  set test variable   @{source_system}    @{sap_sources}

The column values are a hashtag for source system when columns are same for multiple sources
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${i}
    set test variable  ${alias}
    ${column count}   Get Length   ${hashtag_columns}
    Run keyword if   ${column count}>=15       Connect to databrick and Check columns for hastags and add result to report
    Run keyword if   ${column count}<15       Check columns for hastags and add result to report

The column values are a hashtag for source system when columns are same for set of sources
    [Arguments]    ${column}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${i}
    set test variable  ${alias}
    ${column count}   Get Length   ${column}
    set test variable     ${hashtag_columns}     ${column}
    Run keyword if   ${column count}>=15       Connect to databrick and Check columns for hastags and add result to report
    Run keyword if   ${column count}<15        Check columns for hastags and add result to report

The columns values are a hashtag for source system when set columns are same for set of sources
    [Arguments]    ${column}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${i}
    set test variable  ${alias}
    ${column count}   Get Length   ${column}
    set test variable     ${hashtag_columns}     ${column}
    Run keyword if   ${column count}>=15      Connect to databrick and Check columns set for hastags and add result to report
    Run keyword if   ${column count}<15       Check columns set for hastags and add result to report

Connect to databrick and Check columns for hastags and add result to report
    Check columns for hastags and add result to report
    Disconnect from Databricks

Connect to databrick and Check columns set for hastags and add result to report
    Check columns set for hastags and add result to report
    Disconnect from Databricks

Check columns for hastags and add result to report
    run keyword if      ${Connection}==False     Connect to CDL Databricks
    Add information to hashtag columns report
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    FOR    ${column}   IN  @{hashtag_columns}
    log    ${column}
    Validate hashtags values per column   ${table_count}    ${column}    ${alias}
    END
    run keyword if   '${data}'!='FAIL' and '${test_status}'=='FAIL'  add result to report      Test Failed: At least 1 column contains non hashtags  ELSE    add result to report  Test Passed: Columns contain all hashtags
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

Validate hashtags values per column
    [Arguments]    ${table_count}     ${column}    ${alias}
    ${hashtags}=    return count of table with conditions  ${system}.${table}        ${column} = '#' and src_sys_cd == '${alias}'
    run keyword if       ${hashtags}==0      Data not available
    run keyword     add result to report    Expected = ${table_count}
    run keyword if  '${hashtags}'=='${table_count}' and '${hashtags}'!='0'      add result to report      Test Passed:Column ${column} contains all hashtags for ${alias}\n        ELSE IF    '${hashtags}'!='0' and '${hashtags}'!='${table_count}'    add result to report    Test Failed: Column ${column} contains non hashtag values\n
    run keyword if  '${hashtags}'=='${table_count}' and '${hashtags}'!='0'      set test variable   ${test_status}  PASS
    run keyword if   ${hashtags}!=${table_count}     set test variable   ${test_status}  FAIL
    Log to console    Column ${column} is checked for ${i}

I expect that the column values are a hashtag for source system or contains filter values
    [Arguments]     ${sources}    ${columns}
    Validate that column values are a hashtag for source system or contains filter values    ${sources}    ${columns}
    Log   ${sources}
    Log   ${columns}

Validate that column values are a hashtag for source system or contains filter values
     [Arguments]     ${sources}    ${columns}
      FOR  ${i}  IN  @{source_system}
      set test variable   ${i}
      run keyword if      ${Connection}==False     Connect to CDL Databricks
      ${alias}=   get from dictionary     ${TABLE_ALIAS}   ${i}
      Check column with filter    ${alias}    ${columns}
      END

Check column with filter
    [Arguments]    ${alias}    ${columns}
    Add information to hashtag columns report
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    Run keyword if     ${table_count}==0    Data not available
    Run keyword if     ${table_count}!=0       Check values per source    ${alias}    ${columns}   ${table_count}

Check values per source
    [Arguments]    ${alias}    ${columns}    ${table_count}
    FOR   ${column}   IN   @{columns}
    ${table_count_filter}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}' and ${column} not in ("#") and ${column} is not null
    ${status}    Run keyword and return status     Should be equal    ${table_count}    ${table_count_filter}
    Run keyword if   ${status}==True    Validate hashtags values per column     ${table_count}   ${column}    ${alias}
    Run keyword if   ${status}==False   Get hashtag count from table    ${table_count}    ${table_count_filter}    ${column}    ${alias}
    END

Get hashtag count from table
    [Arguments]   ${table_count}  ${table_count_filter}   ${column}    ${alias}
    add result to report    Column:${column} in ${table} for ${alias} contains other values as per filter requirement
    ${count}    Evaluate   ${table_count}-${table_count_filter}
    Run keyword if  ${count}!=0      Validate hashtags values per column      ${count}     ${column}    ${alias}
    Run keyword if  ${count}==0      Add result to report   Test Passed:Column ${column} don't contain hashtag values as per filer requirement

Check columns set for hastags and add result to report
    run keyword if      ${Connection}==False     Connect to CDL VIEW Databricks
    Add information to hashtag columns report
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    ${templist}    Create List
    FOR   ${column}   IN   @{hashtag_columns}
    ${tempString}     Catenate  ${column}  = '#'  AND
    Append To List    ${templist}     ${tempString}
    END
    ${templist}    Convert To String       ${templist}
    ${templist}     Remove String          ${templist}       [     AND"]     "    ,    ]
    ${hashtags}=    return count of table with conditions  ${system}.${table}       ${templist} and src_sys_cd == '${alias}'
    run keyword if       ${hashtags}==0      Data not available
    run keyword     add result to report    Expected = ${table_count}
    ${status}       run keyword and return status      should be true      '${hashtags}'=='${table_count}' and '${hashtags}'!='0'
    run keyword if   ${status}==False      add result to report     Test failed '${hashtags}' not equal '${table_count}' at least one column in set contains non hashtags
    run keyword if   ${status}==True       Compare hashtags results and add to report     ${hashtags}    ${table_count}   ${hashtag_columns}

Compare hashtags results and add to report
    [Arguments]      ${hashtags}    ${table_count}    ${hashtag_columns}
    run keyword if  '${hashtags}'=='${table_count}' and '${hashtags}'!='0'      add result to report      Test Passed:Columns ${hashtag_columns} contains all hashtags for ${alias}\n        ELSE IF    '${hashtags}'!='0' and '${hashtags}'!='${table_count}'    add result to report    Test Failed: Columns ${hashtag_columns} contains non hashtag values\n
    run keyword if   ${hashtags}!=${table_count}     set test variable   ${test_status}  FAIL
    run keyword if  '${data}'!='FAIL' and '${test_status}'=='FAIL'  add result to report      Test Failed: At least 1 column contains non hashtags  ELSE    add result to report  Test Passed: Columns contain all hashtags
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
    Log to console    Columns are checked for ${alias}

I expect that the column values are a hashtag for source system when columns are different
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{source_system}
    run keyword if  ${temp}== False                  set test variable   @{source_system}    @{sap_sources}
    FOR  ${i}  IN  @{source_system}
    set test variable   ${i}
    The column values are a hashtag for source system when columns are different
    END
    log test to report per step     ${jira_id_tag}
    #Teardown Test

The column values are a hashtag for source system when columns are different
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${i}
    set test variable    ${alias}
    run keyword if      '${i}' == 'maximo'       set test variable   @{hashtag_columns}  @{hashtag_columns_maximo}
    run keyword if      '${i}' == 'elims'        set test variable   @{hashtag_columns}  @{hashtag_columns_elims}
    run keyword if      '${i}' == 'btb_na'       set test variable   @{hashtag_columns}  @{hashtag_columns_btb_na}
    run keyword if      '${i}' == 'btb_latam'    set test variable   @{hashtag_columns}  @{hashtag_columns_btb_latam}
    run keyword if      '${i}' == 'taishan'      set test variable   @{hashtag_columns}  @{hashtag_columns_taishan}
    run keyword if      '${i}' == 'deu'          set test variable   @{hashtag_columns}  @{hashtag_columns_deu}
    run keyword if      '${i}' == 'p01'          set test variable   @{hashtag_columns}  @{hashtag_columns_p01}
    run keyword if      '${i}' == 'bw2'          set test variable   @{hashtag_columns}  @{hashtag_columns_bw2}
    Add information to hashtag columns report
    Run keyword if   ${column count}>15       Connect to databrick and Check columns for hastags and add result to report
    Run keyword if   ${column count}<15       Check columns for hastags and add result to report

I expect that the column values are trimmed for source system
    ${column count}         Get Length         ${whitespace_columns}
    Run keyword if   ${column count}>15       Connect to databrick and validate requirement for each source
    Run keyword if   ${column count}<15       Validate requirement for each source

Connect to databrick and validate requirement for each source
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{source_system}
    run keyword if  ${temp}== False                  set test variable   @{source_system}    @{sap_sources}
    FOR  ${i}  IN  @{source_system}
    set test variable   ${i}
    Validate that column values are trimmed for source system
    Log to console    Columns are checked for ${i}
    Disconnect from Databricks
    END
    log test to report per step     ${jira_id_tag}
    #Teardown Test

Validate requirement for each source
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{source_system}
    run keyword if  ${temp}== False                  set test variable   @{source_system}    @{sap_sources}
    FOR  ${i}  IN  @{source_system}
    set test variable   ${i}
    Validate that column values are trimmed for source system
    END
    log test to report per step     ${jira_id_tag}
   # Teardown Test

Validate that column values are trimmed for source system
    run keyword if      ${Connection}==False     Connect to CDL Databricks
    ${alias} =   get from dictionary     ${TABLE_ALIAS}  ${i}
    add system information to report whitespaces   ${system}.${table}     ${i}
    FOR     ${column}   IN  @{whitespace_columns}
        add columns names to report  ${column}
    END
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'

    FOR     ${column}   IN  @{whitespace_columns}
            ${whitespace}=    return count of table with conditions  ${system}.${table}        (${column} not like ' %' or ${column} not like '% ' or ${column} is NULL) and src_sys_cd == '${alias}'
            run keyword     add result to report    Expected = ${table_count}
            run keyword if  ${whitespace}==${table_count}     add result to report  Test Passed : Column ${column} contains no values with whitespace for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains values with whitespace for ${alias}\n
            run keyword if  ${whitespace}!=${table_count}     set test variable   ${test_status}  FAIL
    END

    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values with whitespace  ELSE    add result to report  Test Passed: These columns do not contain values with whitespace @{whitespace_columns}
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

Validate that columns values do not contain whitespaces for the source system when columns are same for multiple sources
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{WHITESPACE_SOURCES}
    Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Validate that columns in source systems do not contain whitespaces when columns are same for multiple sources
    END
    log test to report per step     ${jira_id_tag}
   # Teardown Test

Validate that columns values do not contain whitespaces in view for the source system when columns are same for multiple sources
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{WHITESPACE_SOURCES}
    Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Validate that columns in source systems do not contain whitespaces in view when columns are same for multiple sources
    END
    log test to report per step     ${jira_id_tag}
   # Teardown Test

Validate that columns in source systems do not contain whitespaces in view when columns are same for multiple sources
    ${alias}   get from dictionary     ${TABLE_ALIAS}   ${i}
    set test variable    ${alias}
    ${column count}   Get Length   ${whitespace_columns}
    Run keyword if      ${column count}>=15       Connect to databrick and Validate all columns whitespaces for source and add result to report for view
    Run keyword if      ${column count}<15        Validate all columns whitespaces for source and add result to report for view

Validate that columns in source systems do not contain whitespaces when columns are different
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{WHITESPACE_SOURCES}
    Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Validate that columns in source systems do not contain whitespaces when columns are different for multiple sources
    END
    log test to report per step     ${jira_id_tag}
    #Teardown Test

Validate that columns contain whitespace removed when columns are same for set of sources
    [Arguments]    ${sourceList}     ${columns}
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{sourceList}
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Columns contain whitespace removed for the source system when columns are same for set of sources     @{columns}
    END
    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non nulls  ELSE    add result to report  Test Passed: Columns contain all nulls
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
	log test to report per step    ${sourceList}_${columns}[0]
    #Teardown Test

Columns contain whitespace removed for the source system when columns are same for set of sources
    [Arguments]     @{whitespace_columns}
    run keyword if      ${Connection}==False     Connect to CDL Databricks
    ${alias} =   get from dictionary     ${TABLE_ALIAS}  ${i}
    add system information to report whitespaces   ${system}.${table}     ${i}
    FOR     ${column}   IN  @{whitespace_columns}
        add columns names to report  ${column}
    END
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    FOR     ${column}   IN  @{whitespace_columns}
            ${whitespace}=    return count of table with conditions  ${system}.${table}        (${column} not like ' %' or ${column} not like '% ' or ${column} is NULL) and src_sys_cd == '${alias}'
            run keyword     add result to report    Expected = ${table_count}
            run keyword if  ${whitespace}==0     Data not available
            run keyword if  '${whitespace}'=='${table_count}' and '${whitespace}'!='0'    add result to report  Test Passed : Column ${column} contains no values with whitespace for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains values with whitespace for ${alias}\n
            run keyword if  '${whitespace}'=='${table_count}' and '${whitespace}'!='0'     set test variable   ${test_status}    PASS
            run keyword if  '${whitespace}'!='${table_count}'     set test variable   ${test_status}    FAIL
            log to console    Column ${column} is tested for ${alias}
    END
    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values with whitespace  ELSE    add result to report  Test Passed: These columns do not contain values with whitespace @{whitespace_columns}
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

Validate that columns in source systems do not contain whitespaces when columns are different for multiple sources
    ${alias}   get from dictionary     ${TABLE_ALIAS}   ${i}
    set test variable    ${alias}

    run keyword if      '${i}' == 'atlas'        set test variable   @{whitespace_columns}  @{whitespace_columns_atlas}
    run keyword if      '${i}' == 'europe2'      set test variable   @{whitespace_columns}  @{whitespace_columns_europe2}
    run keyword if      '${i}' == 'sustain'      set test variable   @{whitespace_columns}  @{whitespace_columns_sustain}
    run keyword if      '${i}' == 'panda'        set test variable   @{whitespace_columns}  @{whitespace_columns_panda}
    run keyword if      '${i}' == 'elims'        set test variable   @{whitespace_columns}  @{whitespace_columns_elims}
    run keyword if      '${i}' == 'maximo'       set test variable   @{whitespace_columns}  @{whitespace_columns_maximo}
    run keyword if      '${i}' == 'p01'          set test variable   @{whitespace_columns}  @{whitespace_columns_p01}
    run keyword if      '${i}' == 'btb_na'       set test variable   @{whitespace_columns}  @{whitespace_columns_btb_na}
    run keyword if      '${i}' == 'btb_latam'    set test variable   @{whitespace_columns}  @{whitespace_columns_btb_latam}
    run keyword if      '${i}' == 'taishan'      set test variable   @{whitespace_columns}  @{whitespace_columns_tai}
    run keyword if      '${i}' == 'bw2'          set test variable   @{whitespace_columns}  @{whitespace_columns_bw2}
    run keyword if      '${i}' == 'deu'          set test variable   @{whitespace_columns}  @{whitespace_columns_deu}
    run keyword if      '${i}' == 'bba'          set test variable   @{whitespace_columns}  @{whitespace_columns_bba}
    run keyword if      '${i}' == 'gmd'          set test variable   @{whitespace_columns}  @{whitespace_columns_gmd}
    run keyword if      '${i}' == 'geu'          set test variable   @{whitespace_columns}  @{whitespace_columns_geu}
    run keyword if      '${i}' == 'bwi'          set test variable   @{whitespace_columns}  @{whitespace_columns_bwi}
    run keyword if      '${i}' == 'hcs'          set test variable   @{whitespace_columns}  @{whitespace_columns_hcs}
    run keyword if      '${i}' == 'mrs'          set test variable   @{whitespace_columns}  @{whitespace_columns_mrs}
    run keyword if      '${i}' == 'hmd'          set test variable   @{whitespace_columns}  @{whitespace_columns_hmd}
    run keyword if      '${i}' == 'fsn'          set test variable   @{whitespace_columns}  @{whitespace_columns_fsn}
    run keyword if      '${i}' == 'jet'          set test variable   @{whitespace_columns}  @{whitespace_columns_jet}
    run keyword if      '${i}' == 'jsw'          set test variable   @{whitespace_columns}  @{whitespace_columns_jsw}
    run keyword if      '${i}' == 'svs'          set test variable   @{whitespace_columns}  @{whitespace_columns_svs}

    add system information to report whitespaces  ${system}.${table}     ${i}
    FOR     ${column}   IN  @{whitespace_columns}
    add columns names to report  ${column}
    END
    ${column count}   Get Length   ${whitespace_columns}
    Run keyword if   ${column count}>=15       Connect to databrick and Validate whitespaces for source and add result to report
    Run keyword if   ${column count}<15        Validate whitespaces for source and add result to report

Connect to databrick and Validate whitespaces for source and add result to report
    Validate whitespaces for source and add result to report
    Log to console    Columns are checked for ${i}
    Disconnect from Databricks

Validate that columns in source systems do not contain whitespaces when columns are same for multiple sources
    ${alias}   get from dictionary     ${TABLE_ALIAS}   ${i}
    set test variable    ${alias}
    ${column count}   Get Length   ${whitespace_columns}
    Run keyword if      ${column count}>=15       Connect to databrick and Validate whitespaces for source and add result to report
    Run keyword if      ${column count}<15        Validate whitespaces for source and add result to report

Data not available
     run keyword   add result to report  !Test Failed: Data not available
     set test variable   ${test_status}  FAIL
     set test variable   ${data}  FAIL

Validate whitespaces for source and add result to report
    run keyword if      ${Connection}==False     Connect to CDL Databricks
    Add information to whitespace columns report
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    FOR   ${column}   IN   @{whitespace_columns}
    ${whitespace}=   return count of table with conditions   ${system}.${table}   (${column} not like ' %' or ${column} not like '% ' or ${column} is NULL) and src_sys_cd == '${alias}'
    run keyword if       ${whitespace}==0      Data not available
    run keyword   add result to report   Expected = ${table_count}
    run Keyword If  '${whitespace}'=='${table_count}' and '${whitespace}'!='0'    add result to report   Test Passed : Column ${column} contains no values with whitespace for ${alias}\n       ELSE IF    '${whitespace}'!='0' and '${whitespace}'!='${table_count}'        add result to report    Test Failed: Column ${column} contains values with whitespace for ${alias}\n
    run keyword if   ${whitespace}!=${table_count}   set test variable   ${test_status}    FAIL
    log to console    Column ${column} tested for source ${alias}
    END
    run keyword if   '${data}'!='FAIL' and '${test_status}'=='FAIL'      add result to report      Test Failed: At least 1 column contains values with whitespace  ELSE    add result to report  Test Passed: Columns do not contain values with whitespace
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

Add information to whitespace columns report
    add system information to report whitespaces  ${system}.${table}     ${i}
    FOR     ${column}   IN  @{whitespace_columns}
    add columns names to report  ${column}
    END

Add information to all null columns report
    [Arguments]    ${i}
    add system information to report null   ${system}.${table}     ${i}
    FOR     ${column}   IN  @{null_columns}
    add columns names to report  ${column}
    END

Add information to hashtag columns report
    add system information to report hashtag  ${system}.${table}     ${i}
    FOR     ${column}   IN   @{hashtag_columns}
    add columns names to report  ${column}
    END

I expect that the column values contain blanks for the sap source systems
    The column values contain blanks for the sap source systems

The column values contain blanks for the sap source systems
        FOR     ${source}   IN   @{sap_sources}
            ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}

            ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'

            FOR     ${column}   IN  @{blank_columns_sap}
                ${blanks}=    return count of table with conditions  ${system}.${table}        trim(${column}) = '' and src_sys_cd == '${alias}'
                run keyword     add result to report    Expected = ${table_count}
                run keyword if  ${blanks}==${table_count}     add result to report  Test Passed : Column ${column} contains values with blanks for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains values without blanks for ${alias}\n
                run keyword if  ${blanks}!=${table_count}     set test variable   ${test_status}  FAIL
            END
        END
            run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values without blanks  ELSE    add result to report  Test Passed: Columns contain values with blanks
            run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
        log test to report per step     ${jira_id_tag}
       # Teardown Test

I expect that the column values contain zeros for the sap source systems
    The column values contain zeros for the sap source systems

The column values contain zeros for the sap source systems
        FOR     ${source}   IN   @{sap_sources}
            ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}

            ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'

            FOR     ${column}   IN  @{zero_columns_sap}
                ${zeros}=    return count of table with conditions  ${system}.${table}        ${column} = '0' and src_sys_cd == '${alias}'
                run keyword     add result to report    Expected = ${table_count}
                run keyword if  ${zeros}==${table_count}     add result to report  Test Passed : Column ${column} contains values with zeros for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains values without zeros for ${alias}\n
                run keyword if  ${zeros}!=${table_count}     set test variable   ${test_status}  FAIL
            END
        END
            run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values without zeros  ELSE    add result to report  Test Passed: Columns contain values with zeros
            run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
        log test to report per step     ${jira_id_tag}
       # Teardown Test

# ------------------- Used to ensure that date formatting has worked --------------------------------

Validate that columns do not contain null values
        ${table_count}=    return count of table  ${system}.${table}
        FOR     ${column}   IN  @{not_null_columns}
            ${nonulls}=    return count of table not null      ${system}.${table}        ${column}

            run keyword     add result to report    Expected = ${table_count}
            run keyword if  ${nonulls}!=${table_count}     set test variable   ${test_status}  FAIL
            run keyword if  ${nonulls}==${table_count}     add result to report  Test Passed : Column ${column} does not contain null values as per requirement       ELSE        add result to report    Test Failed: Column ${column} contains values which are null\n
        END

        run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains null values   ELSE    add result to report  Test Passed: Columns does not contain null values
        run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
        log test to report per step     ${jira_id_tag}
        #Teardown Test

The column values are not all null
        ${table_count}=    return count of table  ${system}.${table}
        FOR     ${column}   IN  @{not_only_null_columns}
            ${nonulls}=    return count of table with conditions  ${system}.${table}        ${column} IS NULL
            run keyword     add result to report    Expected = ${table_count}
            run keyword if  ${nonulls}==${table_count}     set test variable   ${test_status}  FAIL
            run keyword if  ${nonulls}!=${table_count}     add result to report  Test Passed : Column ${column} contains values which are not all null\n        ELSE        add result to report    Test Failed: Column ${column} contains values which are all null\n
        END

        run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains only null values   ELSE    add result to report  Test Passed: Columns contain some values which are not null
        run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
       log test to report per step     ${jira_id_tag}
       # Teardown Test

# ----------------------------BEGIN: NULL and HASHTAG Test Specifically for MAINT_LOC maximo ----------------------------
I expect that the column values contain nulls and hashtags for maximo with multiple object names
    The column values contain nulls and hashtags for maximo with multiple object names

The column values contain nulls and hashtags for maximo with multiple object names
    FOR     ${object}   IN   @{maximo_objects}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  maximo
    run keyword if      '${object}' == 'asset'    set test variable   @{null_columns}  @{null_columns_maximo_asset}
    run keyword if      '${object}' == 'asset'    set test variable   ${hashtag_column}  MAINT_LOC_CD_2
    run keyword if      '${object}' == 'locations'    set test variable   @{null_columns}  @{null_columns_maximo_locations}
    run keyword if      '${object}' == 'locations'    set test variable   ${hashtag_column}  MAINT_LOC_CD
    ${table_count}=    return count of table with conditions  ${system}.${table}       ${hashtag_column} == '#' and src_sys_cd == '${alias}'

         FOR     ${column}   IN  @{null_columns}
         ${nulls}=    return count of table with conditions  ${system}.${table}        ${column} IS NULL and ${hashtag_column} == '#' and src_sys_cd == '${alias}'
         run keyword     add result to report    Expected = ${table_count}
         run keyword if  ${nulls}==${table_count}     add result to report  Test Passed : Column ${column} contains values with nulls and column ${hashtag_column} contains hashtags for ${alias} where object name is ${object}\n        ELSE        add result to report    Test Failed: Column ${column} contains values without nulls and hashtags for ${alias} where object name is ${object}\n
         run keyword if  ${nulls}!=${table_count}     set test variable   ${test_status}  FAIL
         END
    END

    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values without nulls and hashtags  ELSE    add result to report  Test Passed: Columns specified for NULL values contain only NULL and Columns specified for HASHTAG values contain only HASHTAGS for Maximo.
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
    log test to report per step     ${jira_id_tag}
   # Teardown Test

# ----------------------------BEGIN: NULL and Character Test Specifically for CAPA_EXTN_RQST eti ----------------------
I expect that the column values contain specified characters or they are nulls for ${source}
    The column values contain specified characters or they are nulls for ${source}

The column values contain specified characters or they are nulls for ${source}
    ${dict_keys}=       get dictionary keys     ${CHARS_AND_COLUMNS_ETI}
    ${alias}=           get from dictionary     ${TABLE_ALIAS}  etq_instinct

    FOR    ${chars}        IN       @{CHARS_AND_COLUMNS_ETI}
        ${column}=    get from dictionary  ${CHARS_AND_COLUMNS_ETI}    ${chars}
        ${actual_result}=    return count of table with conditions  ${system}.${table}        SRC_SYS_CD == '${alias}' AND ${column} IS NOT NULL AND ${column} NOT LIKE '${chars}%'
        run keyword     add result to report    Expected = 0
        run keyword if  ${actual_result}==0     add result to report  Test Passed : Column ${column} only contains values with nulls or the first two characters are ${chars} where object name is ${table} for ${alias} source system\n        ELSE        add result to report    Test Failed: Column ${column} contains values without nulls or where the first two characters are ${chars}\n
        run keyword if  ${actual_result}!=0     set test variable   ${test_status}  FAIL
    END
    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column does not contain values with nulls or the first two characters are as specified  ELSE    add result to report  Test Passed: Columns contain values with nulls or the first two specified characters ${dict_keys}
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
    log test to report per step     ${jira_id_tag}
   # Teardown Test

#----------------------------Rowcount test for l1 views--------------------------------------------------------------
The rowcounts of view and underlying table are the same
    ${table_count}=    return count of table  ${underlying_l1_db_for_vw}.${underlying_table}
    ${view_count}=     return count of table  ${system}.${table}
    run keyword     add result to report    Expected = ${table_count}
    run keyword if  ${table_count}!=${view_count}     set test variable   ${test_status}  FAIL
    run keyword if  ${table_count}==${view_count}     add result to report  Test Passed : View ${system}.${table} contains the same number of rows as table ${underlying_l1_db_for_vw}.${underlying_table}

    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: View ${system}.${table} does not contain the same number of rows as table ${underlying_l1_db_for_vw}.${underlying_table}
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
    log test to report per step     ${jira_id_tag}
   # Teardown Test

I expect that the target table contains required number of columns
    Validate that the target table contains required number of columns

Validate that the target table contains required number of columns
    log  Required number of columns is ${COLUMN_COUNT}
    The target table has ${COLUMN_COUNT} columns

I expect that Active column has been updated
    The Active column has been updated

The Active column has been updated
    SET JIRA TAG
    ${flag_is_set}=   check src sys active flag  ${system}  src_sys
    run keyword and continue on failure  should be true  ${flag_is_set}==True
    run keyword and continue on failure  add result to report   Expected: flag is set = True
    run keyword and continue on failure  add result to report   Actual: flag is set = ${flag_is_set}

    run keyword and continue on failure  add result to report   \nTest Passed: The Active flag has been set where SRC_SYS_SHRT_NM is in Sustain, E2, Atlas, Panda or eLIMS
    log test to report per step     ${jira_id_tag}
   # Teardown Test

I expect that curation logic has been applied on specified table
   Validate that curation logic has been applied on specified table

I expect that for curation logic with environment details has been applied on specified table
   Validate that curation logic with environment details has been applied on specified table

I expect that for curation logic with environment details has been applied on specified table for multiple sources
   Validate that curation logic with environment details has been applied on specified table for multiple sources

Validate that curation logic has been applied on specified table
    ${expected_filter}=    Get Variable Value    ${expected_system_result}   All
    ${src_sys_sql_dict}=      get source system sql    ${table}   ${expected_filter}
    FOR     ${src_sql}  IN  @{src_sys_sql_dict}
        ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${src_sql}
        ${query}=   get test query db     ${table}    ${src_sql}   ${alias}
        ${actual_result}=   run sql query full with cond    ${query}    ${condition}
        ${expected}=    get from dictionary     ${EXPECTED_RESULT}    ${src_sql}
        Set test variable     ${actual_result}   ${actual_result}
        Set test variable     ${expected}    ${expected}
        Compare actual and expected result and add to report
    END

    run keyword if   '${test_status}'=='PASS'   add result to report   \nTest Passed: All values were as expected in target after curation.   ELSE   add result to report   \nTest Failed: At least one values was not as expected in target after curation.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    log test to report per step     ${jira_id_tag}
    #Teardown Test

I expect that curation logic has been applied on specified table for multiple sources
    Validate that curation logic has been applied on specified table for multiple sources

Validate that curation logic has been applied on specified table for multiple sources
    ${expected_filter}=    Get Variable Value    ${expected_system_result}   All
    ${src_sys_sql_dict}=      get source system sql    ${table}   ${expected_filter}
    FOR     ${src_sql}  IN  @{src_sys_sql_dict}
    ${status}      run keyword and return status    Should Be Equal As Strings  ${src_sql}    hmd
    run keyword if    ${status}==True        Check that curation logic with environment details has been applied on specified table      ${src_sql}
    run keyword if    ${status}==False       Check that curation logic has been applied on specified table      ${src_sql}
    END
    run keyword if   '${test_status}'=='PASS'   add result to report   \nTest Passed: All values were as expected in target after curation.   ELSE   add result to report   \nTest Failed: At least one values was not as expected in target after curation.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    log test to report per step     ${jira_id_tag}
    #Teardown Test

Check that curation logic has been applied on specified table
    [Arguments]   ${src_sql}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${src_sql}
    ${query}=   get test query db     ${table}    ${src_sql}   ${alias}
    ${temp}  get from dictionary     ${CONDITIONS}   ${src_sql}
    FOR     ${i}  IN  @{temp}
    Set test variable  ${condition}     ${i}
    END
    ${actual_result}=   run sql query full with cond    ${query}    ${condition}
    ${expected}=    get from dictionary     ${EXPECTED_RESULT}    ${src_sql}
    Set test variable     ${actual_result}   ${actual_result}
    Set test variable     ${expected}    ${expected}
    Compare actual and expected result and add to report
    log to console     ${src_sql} is tested

Check that curation logic with environment details has been applied on specified table
    [Arguments]       ${src_sql}
    ${expected_filter}=    Get Variable Value    ${expected_system_result}   All
    ${src_sys_sql_dict}=      get source system sql    ${table}   ${expected_filter}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${src_sql}
    ${query}=   get test env query db     ${table}    ${src_sql}   ${alias}   ${env}
    ${temp}  get from dictionary     ${CONDITIONS}   ${src_sql}
    FOR     ${i}  IN  @{temp}
    Set test variable  ${condition}     ${i}
    END
    ${actual_result}=   run sql query full with cond    ${query}    ${condition}
    ${expected}=    get from dictionary     ${EXPECTED_RESULT}    ${src_sql}
    Set test variable     ${actual_result}   ${actual_result}
    Set test variable     ${expected}    ${expected}
    Compare actual and expected result and add to report
    log to console     ${src_sql} is tested

Compare actual and expected result and add to report
    ${actual_result}    Convert to string   ${actual_result}
    ${expected}         Convert to string   ${expected}
    ${actual_result}    Remove String      ${actual_result}    [    ]
    ${expected}         Remove String      ${expected}   [    ]
    Log  ${actual_result}
    ${result}    run keyword and return status      Should Be Equal As Strings  ${expected}    ${actual_result}
    run keyword if  ${result}==False   Check expected in actual   ${expected}    ${actual_result}
    Log  ${expected}
    Log  ${actual_result}
    run keyword and continue on failure     should be true  ${result}
    run keyword if  ${result}  add result to report   Expected Result: ${expected}
    run keyword if  ${result}  add result to report      Test Passed: Expected Result found in Actual Result\n  ELSE    add result to report  Test Failed: Expected Result not found in Actual Result\n
    run keyword if  not ${result}  set test variable   ${test_status}  FAIL
    run keyword if  ${result}  set test variable   ${result}  False

Check expected in actual
    [Arguments]     ${expected}    ${actual_result}
    ${result}    run keyword and return status   check expected in actual  ${expected}   ${actual_result}
    Set test variable      ${result}     ${result}

I expect that specified rule aplied to column
     Validate that first specified rule aplied to column
     Validate that second rule aplied to column
     Add execution result to report

Validate that first specified rule aplied to column
    Validate that specified rule aplied to column   ${selectCondition1}    ${COLUMNS_WITH_RULES1}    ${rule_pattern1}

Validate that second rule aplied to column
    Validate that specified rule aplied to column   ${selectCondition2}     ${COLUMNS_WITH_RULES2}    ${rule_pattern2}

Validate that specified rule aplied to column
    [Arguments]    ${selectCondition}    ${listOfColumns}    ${rule_pattern}
    ${expected_filter}=    Get Variable Value    ${expected_system_result}   All
    ${src_sys_sql_dict}=      get source system sql    ${table}   ${expected_filter}
    FOR     ${src_sql}  IN  @{src_sys_sql_dict}
       ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${src_sql}
       FOR    ${column}  IN  @{listOfColumns}
       ${value}=    get from dictionary  ${listOfColumns}    ${column}
       ${expected_result}=  run rule query full     ${selectCondition}    ${ruleSQL}     ${value}
       ${temp}  run keyword and return status    Should Be Empty  ${expected_result}
       run keyword if  ${temp}==True       Validate that final column contain nulls    ${column}    ${table}    ${rule_pattern}
       run keyword if  ${temp}==False      Validate that rule applied to column        ${column}    ${table}    ${expected_result}    ${rule_pattern}
       END
    END

Validate that rule applied to column
    [Arguments]       ${column}    ${table}    ${expected_result}   ${rule_pattern}
    ${expected_result}   Convert to string     ${expected_result}
    ${expected_result}   Remove String    ${expected_result}    ',    '
    ${actual_result}=    run sql query to get value from column     ${column}    ${table}    ${whereConditionFT}
    ${actual_result}     Convert to string       ${actual_result}
    ${actual_result}     Remove String    ${actual_result}    '     ,
    ${result}   run keyword and return status    Should Be Equal As Strings    ${expected_result}    ${actual_result}
    log to console       Column ${column} was tested successfully
    Add result to rules report after Compare actual and expected values  ${expected_result}    ${actual_result}   ${column}    ${rule_pattern}    ${result}

Validate that final column contain nulls
    [Arguments]       ${column}    ${table}   ${rule_pattern}
    ${actual_result}=    run sql query to get value from column     ${column}    ${table}    ${whereConditionFT}
    ${actual_result}     Convert to string       ${actual_result}
    set test variable   ${expected_result}      None
    set test variable   ${rule_pattern}         None
    ${result}   run keyword and return status   Should Contain      ${actual_result}   None
    Add result to rules report after Compare actual and expected values  ${expected_result}    ${actual_result}   ${column}    ${rule_pattern}    ${result}

Add result to rules report after Compare actual and expected values
    [Arguments]   ${expected_result}    ${actual_result}   ${column}    ${rule_pattern}    ${result}
    run keyword and continue on failure     should be true  ${result}
    run keyword if  ${result}  add result to report   Expected Result: ${expected_result}
    run keyword if  ${result}  add result to report   Actual Result: ${actual_result}
    run keyword if  ${result}  add result to report      Test Passed: Column ${column} contains required values ${rule_pattern}\n  ELSE    add result to report  Test Failed: Column ${column} doesn't contains required pattern ${rule_pattern}\n
    run keyword if  not ${result}  set test variable   ${test_status}  FAIL
    run keyword if  ${result}  set test variable   ${result}  False

Add execution result to report
    run keyword if   '${test_status}'=='PASS'   add result to report   \nTest Passed: All values were as expected in columns for final table.   ELSE   add result to report   \nTest Failed: At least one values was not as expected in target after curation.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    log test to report per step     ${jira_id_tag}
    #Teardown Test

Get data count per source on target
     ${target_source_count}=     test data count per source   ${system}    ${table}    ${source_system}
     set test variable    ${target_source_count}     ${target_source_count}

I expect that data count is not below expected amount
    Validate that data count is not below expected amount

Validate that data count is not below expected amount
    ${status} =	Evaluate   ${target_source_count}>=${DATA_COUNT}
    ${test_result}=  run keyword and return status      should be true   ${status}
    run keyword if   ${test_result}==False   set test variable   ${test_status}  FAIL
    run keyword if   ${test_result}==True   add result to report       Expected data count is ${DATA_COUNT}      ELSE   add result to report   Error occurred
    run keyword if   ${test_result}==True   add result to report       Test Passed:The ${system}.${table} data count is ${target_source_count}\n     ELSE   add result to report     Error occurred\n
    run keyword if   '${test_status}'=='PASS'   add result to report   Test Passed: Target data count is not below expected amount\n.   ELSE   add result to report   Test Failed:Target data count is bellow expected amount:${DATA_COUNT}.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    log test to report per step     ${jira_id_tag}
    #Teardown Test

I check that first sample data is presented on view
   Get data from target view     ${system}     ${view}    ${sample1_view_condition}
   log    Sample data presented on view:${Actual_result}

I check that second sample data is presented on view
    Get data from target view     ${system}     ${view}    ${sample2_view_condition}
    log    Sample data presented on view:${Actual_result}

Get data from target view
  [Arguments]        ${system}     ${view}    ${view_condition}
   ${Actual_result}      run sql query full with cond for views   ${system}     ${view}    ${view_condition}
   Set test variable     ${Actual_result}    ${Actual_result}

I expect that first sample data is presented on view for 1 union condition
    Validate that sample data is presented on view    ${sample1_condition}
    log  Expected Sample data for view:${expected_result}
    Add execution result to report

I expect that first sample data is presented on view for 2 unions
    Validate that sample data is presented on view for 2 union    ${sample1_condition}
    log  Expected Sample data for view: ${expected_result}

I expect that first sample data is presented on view
    Validate that sample data is presented on view    ${sample1_condition}
    log  Expected Sample data for view:${expected_result}
    Add execution result to report

I expect that second sample data is presented on view
    Validate that sample data is presented on view   ${sample2_condition}
    log  Expected Sample data for view: ${expected_result}
    Add execution result to report

I expect that truncated first sample data is presented on view
    Validate that the truncated sample data is presented on view    ${sample1_condition}
    log  Expected Sample data for view:${expected_result}

I expect that truncated second sample data is presented on view
    Validate that the truncated sample data is presented on view   ${sample2_condition}
    log  Expected Sample data for view: ${expected_result}
    Add execution result to report

I expect that first sample data is presented on view for 3 unions
    Validate that sample data is presented on view for 3 union    ${sample1_condition}
    log  Expected Sample data for view: ${expected_result}

I expect that first sample data is presented on view for 4 unions
    Validate that sample data is presented on view for 4 union    ${sample1_condition}    ${sample1_condition_Un}
    log  Expected Sample data for view: ${expected_result}

Validate that sample data is presented on view for 2 union
    [Arguments]      ${condition}
    set test variable      ${expected_result}      ${condition}
    ${expected_result}=   run sql query full for 2 unions   ${SELECT_DISTINCT}    ${Union_Conditions_1}    ${Union_From_Cond_1}    ${Union_Where_Conditions_1}    ${Union_Conditions_2}    ${Union_From_Cond_2}    ${Union_Where_Conditions_2}       ${condition}
    Compare actual and expected result for unions    ${expected_result}

I expect that second sample data is presented on view for 4 unions
    Validate that sample data is presented on view for 4 union    ${sample2_condition}    ${sample2_condition_Un}
    log  Expected Sample data for view: ${expected_result}
    Add execution result to report

Then I expect that second sample data is presented on view for 3 unions
    Validate that sample data is presented on view for 3 union    ${sample2_condition}
    log  Expected Sample data for view: ${expected_result}
    Add execution result to report

Validate that sample data is presented on view
    [Arguments]      ${condition}
    ${query}=    Get File       ${query_file_path}${view}_robot.sql
    ${expected_result}=   run sql query full with cond    ${query}    ${condition}
    set test variable      ${expected_result}     ${expected_result}
    ${result}=        run keyword and return status     Lists Should Be Equal     @{expected_result}   @{actual_result}
    IF   ${result}==False
        Convert to string   ${actual_result}
        Convert to string   ${expected_result}
        ${result}=    run keyword and return status      Should Be Equal As Strings  ${expected_result}    ${actual_result}
    END
    run keyword and continue on failure     should be true  ${result}
    run keyword if  ${result}==True  add result to report   |Expected Result: ${expected_result}
    run keyword if  ${result}==True  add result to report      Test Passed: Expected Result found in Actual Result\n  ELSE    add result to report  Test Failed: Expected Result not found in Actual Result\n
    run keyword if  ${result}==False  set test variable   ${test_status}  FAIL

Validate that the truncated sample data is presented on view
    [Arguments]      ${condition}
    ${query}=    Get File       ${query_file_path}${view}_robot.sql
    ${expected_result}=   run sql query full with cond    ${query}    ${condition}
    set test variable      ${expected_result}     ${expected_result}
        Convert to string   ${actual_result}
        Convert to string   ${expected_result}
        ${result}=    run keyword and return status      Should Be Equal As Strings  ${expected_result}    ${actual_result}
    run keyword and continue on failure     should be true  ${result}
    run keyword if  ${result}==True  add result to report   |Expected Result: ${expected_result}
    run keyword if  ${result}==True  add result to report      Test Passed: Expected Result found in Actual Result\n  ELSE    add result to report  Test Failed: Expected Result not found in Actual Result\n
    run keyword if  ${result}==False  set test variable   ${test_status}  FAIL

Validate that sample data is presented on view for 3 union
    [Arguments]      ${condition}
    set test variable      ${expected_result}      ${condition}
    ${expected_result}=   run sql query full for 3 unions   ${SELECT_DISTINCT}    ${Union_Conditions_1}    ${Union_From_Cond_1}    ${Union_Where_Conditions_1}    ${Union_Conditions_2}    ${Union_From_Cond_2}    ${Union_Where_Conditions_2}    ${Union_Conditions_3}     ${Union_From_Cond_3}    ${Union_Where_Conditions_3}    ${condition}
    Compare actual and expected result for unions     ${expected_result}

Validate that sample data is presented on view for 4 union
    [Arguments]      ${condition}    ${condition_Un}
    set test variable      ${expected_result}      ${condition}
    ${expected_result}=   run sql query full for 4 unions   ${SELECT_DISTINCT}    ${Union_Conditions_1}    ${Union_From_Cond_1}    ${Union_Where_Conditions_1}    ${Union_Conditions_2}    ${Union_From_Cond_2}    ${Union_Where_Conditions_2}    ${Union_Conditions_3}     ${Union_From_Cond_3}    ${Union_Where_Conditions_3}    ${Union_Conditions_4}     ${Union_From_Cond_4}    ${Union_Where_Conditions_4}    ${condition}     ${condition_Un}
    Compare actual and expected result for unions    ${expected_result}

Compare actual and expected result for unions
    [Arguments]    ${expected_result}
    set test variable      ${expected_result}       ${expected_result}
    ${result}=        run keyword and return status     Lists Should Be Equal     @{expected_result}   @{actual_result}
    run keyword and continue on failure     should be true  ${result}
    run keyword if  ${result}==True  add result to report   |Expected Result: ${expected_result}
    run keyword if  ${result}==True  add result to report      Test Passed: Expected Result found in Actual Result\n  ELSE    add result to report  Test Failed: Expected Result not found in Actual Result\n
    run keyword if  ${result}==False  set test variable   ${test_status}  FAIL

I check that required rule applied for columns
   Check that Rule applied for columns

Check that Rule applied for columns
    FOR   ${column}    IN    @{NULL_COLUMNS}
    ${column1}    run sql query for unions with rule     ${NULL_Target_Col1}    ${system}     ${view}      ${column}
    Set column name       ${column1}   ${NULL_Target_Col1}   ${NULL_SELECT_CONDITION1}
    Set test variable     ${column1}      ${select_column}
    Set test variable     ${target_column1}    ${target_column}
    ${column2}   run sql query for unions with rule cond    ${NULL_Target_Col2}    ${system}     ${view}     ${target_column1}
    Set column name        ${column2}     ${NULL_Target_Col2}     ${NULL_SELECT_CONDITION2}
    Set test variable      ${column2}      ${select_column}
    Set test variable      ${target_column2}    ${target_column}
    Check that target view columns contains nulls   ${column}
    END

Set column name
    [Arguments]    ${column}     ${target}    ${condition}
    log     ${target}
    ${column}    Convert To String    ${column}
    ${column}    Remove String      ${column}      (    '     ,    )    [    ]
    ${target_column}     Catenate  ${target}='${column}'
    ${select_column}     Catenate  ${condition}='${column}'
    Set test variable     ${target_column}      ${target_column}
    Set test variable     ${select_column}   ${select_column}

I expect that columns contain null values
    Validate that target view columns contains nulls

Check that target view columns contains nulls
    [Arguments]      ${column}
    ${rule}      Get From Dictionary	${NULL_COLUMNS}	    ${column}
    ${expected_result}=   run sql query cond for unions     ${rule}     ${Union_From_Cond_1}    ${Union_Where_Conditions_1}    ${column1}     ${column2}
    ${expected_result}    Convert To String    ${expected_result}
    ${expected_result}    Remove String      ${expected_result}      (    '     ,    )    [    ]
    ${result}    run keyword and return status      Should Be Equal As Strings    ${expected_result}      None
    Run keyword if     ${result}== False   Check that column contains void        ${expected_result}
    run keyword and continue on failure     should be true  ${result}
    Set test variable     ${result}    ${result}
    log to console      Column ${column} tested
    run keyword if  ${result}==True  add result to report   |Expected Result: ${expected_result} after Trans/Val Rule: ${NULL_RULE}
    run keyword if  ${result}==True  add result to report    Test Passed: Column:${column} contains nulls as per requirement\n  ELSE    add result to report  Test Failed:Column contain value\n
    run keyword if  ${result}==False  set test variable   ${test_status}  FAIL

Check that column contains void
    [Arguments]        ${expected_result}
    ${result}    run keyword and return status      Should Be Equal As Strings    ${expected_result}      Void
    Set test variable     ${result}    ${result}

Validate that target view columns contains nulls
     Should be true  ${result}
     ${NULL_COLUMNS}     Get Dictionary Keys   ${NULL_COLUMNS}
     Log   ${NULL_COLUMNS} columns checked and presented on target view ${view} as expected
     run keyword if  ${result}==True    add result to report    ${NULL_COLUMNS} columns checked and presented on target view ${view} as expected
     Add execution result to report

I check that the specified columns contain all null in final view
    Validate that the specified columns contain all null in final view

Validate that the specified columns contain all null in final view
    FOR   ${cond}    IN    @{NULL_COLUMNS_WITH_COND}
    run keyword if      ${Connection}==False     Connect to CDL Databricks
    ${columns}=    get from dictionary  ${NULL_COLUMNS_WITH_COND}   ${cond}
    FOR   ${i}    IN    @{columns}
    ${data}        run sql query for null check views    ${i}   ${system}     ${view}    ${cond}
    ${count}       run sql query for count null check views    ${i}   ${system}     ${view}    ${cond}
    log to console     ${i} is tested
    Validate that target view columns data populated as null      ${data}    ${i}
    ${countcolumn}       run sql query for count null check views column   ${i}   ${system}   ${view}  ${cond}
    ${result}    run keyword and return status    Lists Should Be Equal      ${count}      ${countcolumn}
    Set test variable     ${result}    ${result}
    run keyword if  ${result}==True  add result to report    Test Passed: Expected count: ${count}\n         ELSE    add result to report  Test Failed:Error appear\n
    run keyword if  ${result}==True  add result to report    Test Passed: Actual count: ${countcolumn} \n    ELSE    add result to report  Test Failed:Error appear\n
    run keyword if  ${result}==True  add result to report    Test Passed: Column:${i} contains nulls as per requirement\n  ELSE    add result to report  Test Failed:Column contain value\n
    run keyword if  ${result}==False  set test variable   ${test_status}  FAIL
    END
    END

Validate that target view columns data populated as null
    [Arguments]      ${data}    ${column}
    ${expected_result}    Convert To String    ${data}
    ${expected_result}    Remove String      ${expected_result}      (    '     ,    )    [    ]
    ${result}    run keyword and return status      Should Be Equal As Strings    ${expected_result}      None
    run keyword and continue on failure     should be true  ${result}
    Set test variable     ${result}    ${result}
    run keyword if  ${result}==True   set test variable   ${test_status}  PASS
    run keyword if  ${result}==False  set test variable   ${test_status}  FAIL

I expect that the columns contain all null values for view
    Validate that all columns contain all null values

Validate that all columns contain all null values
    Should be true  ${result}
    ${NULL_COLUMNS_WITH_COND}     Get Dictionary Values   ${NULL_COLUMNS_WITH_COND}
    Log   ${NULL_COLUMNS_WITH_COND} columns checked and presented on target view ${system}.${view} as expected
    run keyword if  ${result}==True    add result to report    ${NULL_COLUMNS_WITH_COND} columns checked and presented on target view ${view} as expected
    Add execution result to report

I expect that the column values do not contain whitespaces in view for the source systems when columns are different
    Validate that columns in view for source systems do not contain whitespaces when columns are different

I expect that the column values do not contain any whitespaces for the source systems when columns are different
    Validate that columns for sources do not contain any whitespaces when columns are different

I expect that the all column values do not contain whitespaces in view for the source systems
     Validate that all columns in view for source systems do not contain whitespaces for the source systems

Validate that columns in view for source systems do not contain whitespaces when columns are different
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{WHITESPACE_SOURCES}
    Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Validate that columns in view for source systems do not contain whitespaces when columns are different for multiple sources
    END
    log test to report per step     ${jira_id_tag}
   # Teardown Test

Validate that columns for sources do not contain any whitespaces when columns are different
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{WHITESPACE_SOURCES}
    Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Validate that columns for sources do not contain any whitespaces when columns are different for multiple sources
    END
    log test to report per step     ${jira_id_tag}
   # Teardown Test

Validate that all columns in view for source systems do not contain whitespaces for the source systems
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{WHITESPACE_SOURCES}
    Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Validate that all columns in view for source systems do not contain whitespaces
    END
    log test to report per step     ${jira_id_tag}
    #Teardown Test

Validate that all columns in view for source systems do not contain whitespaces
    ${alias}   get from dictionary     ${TABLE_ALIAS}   ${i}
    set test variable    ${alias}
    run keyword if      '${i}' == 'atlas'        set test variable   @{whitespace_columns}  @{whitespace_columns_atlas}
    run keyword if      '${i}' == 'europe2'      set test variable   @{whitespace_columns}  @{whitespace_columns_europe2}
    run keyword if      '${i}' == 'sustain'      set test variable   @{whitespace_columns}  @{whitespace_columns_sustain}
    run keyword if      '${i}' == 'panda'        set test variable   @{whitespace_columns}  @{whitespace_columns_panda}
    run keyword if      '${i}' == 'elims'        set test variable   @{whitespace_columns}  @{whitespace_columns_elims}
    run keyword if      '${i}' == 'maximo'       set test variable   @{whitespace_columns}  @{whitespace_columns_maximo}
    run keyword if      '${i}' == 'p01'          set test variable   @{whitespace_columns}  @{whitespace_columns_p01}
    run keyword if      '${i}' == 'btb_na'       set test variable   @{whitespace_columns}  @{whitespace_columns_btb_na}
    run keyword if      '${i}' == 'btb_latam'    set test variable   @{whitespace_columns}  @{whitespace_columns_btb_latam}
    run keyword if      '${i}' == 'taishan'      set test variable   @{whitespace_columns}  @{whitespace_columns_tai}
    run keyword if      '${i}' == 'bw2'          set test variable   @{whitespace_columns}  @{whitespace_columns_bw2}
    run keyword if      '${i}' == 'deu'          set test variable   @{whitespace_columns}  @{whitespace_columns_deu}
    run keyword if      '${i}' == 'bba'          set test variable   @{whitespace_columns}  @{whitespace_columns_bba}
    run keyword if      '${i}' == 'gmd'          set test variable   @{whitespace_columns}  @{whitespace_columns_gmd}
    run keyword if      '${i}' == 'geu'          set test variable   @{whitespace_columns}  @{whitespace_columns_geu}
    run keyword if      '${i}' == 'bwi'          set test variable   @{whitespace_columns}  @{whitespace_columns_bwi}
    run keyword if      '${i}' == 'hcs'          set test variable   @{whitespace_columns}  @{whitespace_columns_hcs}
    run keyword if      '${i}' == 'mrs'          set test variable   @{whitespace_columns}  @{whitespace_columns_mrs}
    run keyword if      '${i}' == 'hmd'          set test variable   @{whitespace_columns}  @{whitespace_columns_hmd}
    run keyword if      '${i}' == 'fsn'          set test variable   @{whitespace_columns}  @{whitespace_columns_fsn}
    run keyword if      '${i}' == 'jet'          set test variable   @{whitespace_columns}  @{whitespace_columns_jet}
    run keyword if      '${i}' == 'jsw'          set test variable   @{whitespace_columns}  @{whitespace_columns_jsw}
    run keyword if      '${i}' == 'svs'          set test variable   @{whitespace_columns}  @{whitespace_columns_svs}
    ${column count}   Get Length   ${whitespace_columns}
    Run keyword if   ${column count}>=15      Connect to databrick and Validate all columns whitespaces for source and add result to report for view
    Run keyword if   ${column count}<15       Validate all columns whitespaces for source and add result to report for view

Validate that columns in view for source systems do not contain whitespaces when columns are different for multiple sources
    ${alias}   get from dictionary     ${TABLE_ALIAS}   ${i}
    set test variable    ${alias}

    run keyword if      '${i}' == 'atlas'        set test variable   @{whitespace_columns}  @{whitespace_columns_atlas}
    run keyword if      '${i}' == 'europe2'      set test variable   @{whitespace_columns}  @{whitespace_columns_europe2}
    run keyword if      '${i}' == 'sustain'      set test variable   @{whitespace_columns}  @{whitespace_columns_sustain}
    run keyword if      '${i}' == 'panda'        set test variable   @{whitespace_columns}  @{whitespace_columns_panda}
    run keyword if      '${i}' == 'elims'        set test variable   @{whitespace_columns}  @{whitespace_columns_elims}
    run keyword if      '${i}' == 'maximo'       set test variable   @{whitespace_columns}  @{whitespace_columns_maximo}
    run keyword if      '${i}' == 'p01'          set test variable   @{whitespace_columns}  @{whitespace_columns_p01}
    run keyword if      '${i}' == 'btb_na'       set test variable   @{whitespace_columns}  @{whitespace_columns_btb_na}
    run keyword if      '${i}' == 'btb_latam'    set test variable   @{whitespace_columns}  @{whitespace_columns_btb_latam}
    run keyword if      '${i}' == 'taishan'      set test variable   @{whitespace_columns}  @{whitespace_columns_tai}
    run keyword if      '${i}' == 'bw2'          set test variable   @{whitespace_columns}  @{whitespace_columns_bw2}
    run keyword if      '${i}' == 'deu'          set test variable   @{whitespace_columns}  @{whitespace_columns_deu}
    run keyword if      '${i}' == 'bba'          set test variable   @{whitespace_columns}  @{whitespace_columns_bba}
    run keyword if      '${i}' == 'gmd'          set test variable   @{whitespace_columns}  @{whitespace_columns_gmd}
    run keyword if      '${i}' == 'geu'          set test variable   @{whitespace_columns}  @{whitespace_columns_geu}
    run keyword if      '${i}' == 'bwi'          set test variable   @{whitespace_columns}  @{whitespace_columns_bwi}
    run keyword if      '${i}' == 'hcs'          set test variable   @{whitespace_columns}  @{whitespace_columns_hcs}
    run keyword if      '${i}' == 'mrs'          set test variable   @{whitespace_columns}  @{whitespace_columns_mrs}
    run keyword if      '${i}' == 'hmd'          set test variable   @{whitespace_columns}  @{whitespace_columns_hmd}
    run keyword if      '${i}' == 'fsn'          set test variable   @{whitespace_columns}  @{whitespace_columns_fsn}
    run keyword if      '${i}' == 'jet'          set test variable   @{whitespace_columns}  @{whitespace_columns_jet}
    run keyword if      '${i}' == 'jsw'          set test variable   @{whitespace_columns}  @{whitespace_columns_jsw}
    run keyword if      '${i}' == 'svs'          set test variable   @{whitespace_columns}  @{whitespace_columns_svs}

    add system information to report whitespaces  ${system}.${table}     ${i}
    FOR     ${column}   IN  @{whitespace_columns}
    add columns names to report  ${column}
    END
    ${column count}   Get Length   ${whitespace_columns}
    Run keyword if   ${column count}>=15       Connect to databrick and Validate whitespaces for source and add result to report for view
    Run keyword if   ${column count}<15       Validate whitespaces for source and add result to report for view

Validate that columns for sources do not contain any whitespaces when columns are different for multiple sources
    ${alias}   get from dictionary     ${TABLE_ALIAS}   ${i}
    set test variable    ${alias}

    run keyword if      '${i}' == 'atlas'        set test variable   @{whitespace_columns}  @{whitespace_columns_atlas}
    run keyword if      '${i}' == 'europe2'      set test variable   @{whitespace_columns}  @{whitespace_columns_europe2}
    run keyword if      '${i}' == 'sustain'      set test variable   @{whitespace_columns}  @{whitespace_columns_sustain}
    run keyword if      '${i}' == 'panda'        set test variable   @{whitespace_columns}  @{whitespace_columns_panda}
    run keyword if      '${i}' == 'elims'        set test variable   @{whitespace_columns}  @{whitespace_columns_elims}
    run keyword if      '${i}' == 'maximo'       set test variable   @{whitespace_columns}  @{whitespace_columns_maximo}
    run keyword if      '${i}' == 'p01'          set test variable   @{whitespace_columns}  @{whitespace_columns_p01}
    run keyword if      '${i}' == 'btb_na'       set test variable   @{whitespace_columns}  @{whitespace_columns_btb_na}
    run keyword if      '${i}' == 'btb_latam'    set test variable   @{whitespace_columns}  @{whitespace_columns_btb_latam}
    run keyword if      '${i}' == 'taishan'      set test variable   @{whitespace_columns}  @{whitespace_columns_tai}
    run keyword if      '${i}' == 'bw2'          set test variable   @{whitespace_columns}  @{whitespace_columns_bw2}
    run keyword if      '${i}' == 'deu'          set test variable   @{whitespace_columns}  @{whitespace_columns_deu}
    run keyword if      '${i}' == 'bba'          set test variable   @{whitespace_columns}  @{whitespace_columns_bba}
    run keyword if      '${i}' == 'gmd'          set test variable   @{whitespace_columns}  @{whitespace_columns_gmd}
    run keyword if      '${i}' == 'geu'          set test variable   @{whitespace_columns}  @{whitespace_columns_geu}
    run keyword if      '${i}' == 'bwi'          set test variable   @{whitespace_columns}  @{whitespace_columns_bwi}
    run keyword if      '${i}' == 'hcs'          set test variable   @{whitespace_columns}  @{whitespace_columns_hcs}
    run keyword if      '${i}' == 'mrs'          set test variable   @{whitespace_columns}  @{whitespace_columns_mrs}
    run keyword if      '${i}' == 'hmd'          set test variable   @{whitespace_columns}  @{whitespace_columns_hmd}
    run keyword if      '${i}' == 'fsn'          set test variable   @{whitespace_columns}  @{whitespace_columns_fsn}
    run keyword if      '${i}' == 'jet'          set test variable   @{whitespace_columns}  @{whitespace_columns_jet}
    run keyword if      '${i}' == 'jsw'          set test variable   @{whitespace_columns}  @{whitespace_columns_jsw}
    run keyword if      '${i}' == 'svs'          set test variable   @{whitespace_columns}  @{whitespace_columns_svs}

    add system information to report whitespaces  ${system}.${table}     ${i}
    FOR     ${column}   IN  @{whitespace_columns}
    add columns names to report  ${column}
    END
    ${column count}   Get Length   ${whitespace_columns}
    Run keyword if   ${column count}>=15       Connect to databrick and Validate whitespaces for the source and add result to report
    Run keyword if   ${column count}<15       Validate whitespaces for the source and add result to report

Connect to databrick and Validate whitespaces for source and add result to report for view
    run keyword if      ${Connection}==False     Connect to CDL VIEW Databricks
    Validate whitespaces for source and add result to report for view
    Log to console    Columns are checked for ${i}
    Disconnect from Databricks

Connect to databrick and Validate whitespaces for the source and add result to report
    Validate whitespaces for the source and add result to report
    Log to console    Columns are checked for ${i}
    Disconnect from Databricks

Connect to databrick and Validate all columns whitespaces for source and add result to report for view
    run keyword if      ${Connection}==False     Connect to CDL VIEW Databricks
    Validate all columns whitespaces for source and add result to report for view
    Log to console    Columns are checked for ${i}
    Disconnect from Databricks

Validate all columns whitespaces for source and add result to report for view
     run keyword if      ${Connection}==False     Connect to CDL VIEW Databricks
     Add information to whitespace columns report
     ${templist}    Create List
     ${templist2}    Create List
     FOR   ${column}   IN   @{whitespace_columns}
     ${tempString}     Catenate  ${column}  like ' %' OR
     Append To List  ${templist}    ${tempString}
     END
     ${templist}    Convert To String     ${templist}
     ${templist}    Remove String         ${templist}      [     OR"]     "    ,    ]

     FOR   ${column}   IN   @{whitespace_columns}
     ${tempString}    Catenate  ${column}  like '% ' OR
     Append To List    ${templist2}   ${tempString}
     END
      ${templist2}    Convert To String       ${templist2}
      ${templist2}    Remove String           ${templist2}     [     OR"]     "    ,    ]

     ${whitespace}   return_count_of_table_with_conditions_list        ${system}.${view}    ${alias}     ${templist}     ${templist2}
     ${status}   Run keyword and return status    Should be true      '${whitespace}'=='0'
     run Keyword If    ${status}==False       Test Failed: At least 1 column in set contains values with whitespace
     run Keyword If    ${status}==False       !Check and record which column contains values with whitespace:
     run Keyword If    ${status}==False       Validate whitespaces for source and add result to report
     run Keyword If    ${status}==True        Check result and add to report    ${whitespace}     ${whitespace_columns}     ${alias}

Validate whitespaces for the source and add result to report
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    Add information to whitespace columns report
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${whitespace}=   return count of table with dynamic conditions  ${system}.${table}       ${alias}       ${whitespace_columns}
    run keyword if       ${whitespace}==0      Data not available
    run keyword   add result to report   Expected = ${table_count}
    run Keyword If  '${whitespace}'=='${table_count}' and '${whitespace}'!='0'    add result to report   Test Passed : Columns ${whitespace_columns} contains no values with whitespace for ${alias}\n       ELSE IF    '${whitespace}'!='0' and '${whitespace}'!='${table_count}'        add result to report    Test Failed: Columns ${whitespace_columns} contains values with whitespace for ${alias}\n
    run keyword if   ${whitespace}!=${table_count}   set test variable   ${test_status}    FAIL
    log to console    Columns ${whitespace_columns} tested for source ${alias}
    run keyword if   '${data}'!='FAIL' and '${test_status}'=='FAIL'      add result to report      Test Failed: At least 1 column contains values with whitespace  ELSE    add result to report  Test Passed: Columns do not contain values with whitespace
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

Check result and add to report
     [Arguments]       ${whitespace}     ${whitespace_columns}     ${alias}
     run Keyword If   '${whitespace}'=='0'    add result to report   Test Passed : Columns ${whitespace_columns} contains no values with whitespace for ${alias}\n       ELSE    add result to report    Test Failed: Columns ${whitespace_columns} contains values with whitespace for ${alias}\n
     run keyword if   '${whitespace}'!='0'   set test variable   ${test_status}    FAIL
     run keyword if   '${data}'!='FAIL' and '${test_status}'=='FAIL'      add result to report      Test Failed: At least 1 column contains values with whitespace  ELSE    add result to report  Test Passed: Columns do not contain values with whitespace

Validate whitespaces for source and add result to report for view
    run keyword if      ${Connection}==False     Connect to CDL VIEW Databricks
    Add information to whitespace columns report
    ${table_count}=    return count of table with conditions  ${system}.${view}       src_sys_cd == '${alias}'
    run keyword if      ${Connection}==False     Connect to CDL VIEW Databricks
    FOR   ${column}   IN   @{whitespace_columns}
        ${whitespace}=   return count of table with conditions   ${system}.${view}   (${column} not like ' %' or ${column} not like '% ' or ${column} is NULL) and src_sys_cd == '${alias}'
        run keyword if       ${whitespace}==0      Data not available
        run keyword   add result to report   Expected = ${table_count}
        run Keyword If  '${whitespace}'=='${table_count}' and '${whitespace}'!='0'    add result to report   Test Passed : Column ${column} contains no values with whitespace for ${alias}\n       ELSE IF    '${whitespace}'!='0' and '${whitespace}'!='${table_count}'        add result to report    Test Failed: Column ${column} contains values with whitespace for ${alias}\n
        run keyword if   ${whitespace}!=${table_count}   set test variable   ${test_status}    FAIL
        log to console    Column ${column} tested for source ${alias}
    END
    run keyword if   '${data}'!='FAIL' and '${test_status}'=='FAIL'      add result to report      Test Failed: At least 1 column contains values with whitespace  ELSE    add result to report  Test Passed: Columns do not contain values with whitespace
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

Validate that curation logic with environment details has been applied on specified table
    ${expected_filter}=    Get Variable Value    ${expected_system_result}   All
    ${src_sys_sql_dict}=      get source system sql    ${table}   ${expected_filter}
    FOR     ${src_sql}  IN  @{src_sys_sql_dict}
        ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${src_sql}
        ${query}=   get test env query db     ${table}    ${src_sql}   ${alias}   ${env}
        ${actual_result}=   run sql query full with cond    ${query}    ${condition}
        ${expected}=    get from dictionary     ${EXPECTED_RESULT}    ${src_sql}
        Set test variable     ${actual_result}   ${actual_result}
        Set test variable     ${expected}    ${expected}
        Compare actual and expected result and add to report
    END

    run keyword if   '${test_status}'=='PASS'   add result to report   \nTest Passed: All values were as expected in target after curation.   ELSE   add result to report   \nTest Failed: At least one values was not as expected in target after curation.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    log test to report per step     ${jira_id_tag}
    #Teardown Test

I expect that the column values do not contain whitespaces for the sap source systems
    The column values do not contain whitespaces for the sap source systems

The column values do not contain whitespaces for the sap source systems
    set test variable   ${test_status}  PASS
    FOR     ${source}   IN   @{sap_sources}
    ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
         FOR     ${column}   IN  @{whitespace_columns_sap}
         ${whitespace}=    return count of table with conditions  ${system}.${table}        (${column} not like ' %' or ${column} not like '% ' or ${column} is NULL) and src_sys_cd == '${alias}'
         run keyword     add result to report    Expected = ${table_count}
         run keyword if  ${whitespace}==${table_count}     add result to report  Test Passed : Column ${column} contains no values with whitespace for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains values with whitespace for ${alias}\n
         run keyword if  ${whitespace}!=${table_count}     set test variable   ${test_status}  FAIL
         END
    END
    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values with whitespace  ELSE    add result to report  Test Passed: Columns do not contain values with whitespace
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
    log test to report per step     ${jira_id_tag}
   # Teardown Test

Validate that curation logic with environment details has been applied on specified table for multiple sources
    ${expected_filter}=    Get Variable Value    ${expected_system_result}   All
    ${src_sys_sql_dict}=      get source system sql    ${table}   ${expected_filter}
    FOR     ${src_sql}  IN  @{src_sys_sql_dict}
        ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${src_sql}
        ${query}=   get test env query db     ${table}    ${src_sql}   ${alias}   ${env}
        ${temp}  get from dictionary     ${CONDITIONS}   ${src_sql}
        FOR     ${i}  IN  @{temp}
        Set test variable  ${condition}     ${i}
        END
        ${actual_result}=   run sql query full with cond    ${query}    ${condition}
        ${expected}=    get from dictionary     ${EXPECTED_RESULT}    ${src_sql}
        Set test variable     ${actual_result}   ${actual_result}
        Set test variable     ${expected}    ${expected}
        Compare actual and expected result and add to report
    END
    run keyword if   '${test_status}'=='PASS'   add result to report   \nTest Passed: All values were as expected in target after curation.   ELSE   add result to report   \nTest Failed: At least one values was not as expected in target after curation.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    log test to report per step     ${jira_id_tag}
    #Teardown Test

I expect that the column values contain null values in view for the source systems when columns are different
    Validate that columns in view for source systems contain null values when columns are different

Validate that columns in view for source systems contain null values when columns are different
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{NULL_SOURCES}
    Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Validate that columns in view for source systems contain null values when columns are different for multiple sources
    END
    log test to report per step     ${jira_id_tag}
    #Teardown Test

Validate that columns in view for source systems contain null values when columns are different for multiple sources
    ${alias}   get from dictionary     ${TABLE_ALIAS}   ${i}
    set test variable    ${alias}
    run keyword if      '${i}' == 'hmd'          set test variable   @{null_columns}  @{null_columns_hmd}
    add system information to report null  ${system}.${table}     ${i}
    FOR     ${column}   IN  @{null_columns}
    add columns names to report  ${column}
    END
    ${column count}   Get Length   ${null_columns}
    Run keyword if   ${column count}>=15       Connect to databrick and Validate null values for source and add result to report for view
    Run keyword if   ${column count}<15       Validate null values for source and add result to report for view

Connect to databrick and Validate null values for source and add result to report for view
    Validate null values for source and add result to report for view
    Log to console    Columns are checked for ${i}
    Disconnect from Databricks

Validate null values for source and add result to report for view
    run keyword if      ${Connection}==False     Connect to CDL VIEW Databricks
    Add information to all null columns report    ${i}
    ${table_count}=    return count of table with conditions  ${system}.${view}       src_sys_cd == '${alias}'
    FOR   ${column}   IN   @{null_columns}
    ${nulls}=   return count of table with conditions   ${system}.${view}   (${column} not like ' %' or ${column} not like '% ' or ${column} is NULL) and src_sys_cd == '${alias}'
    run keyword   add result to report   Expected = ${table_count}
    run keyword if   ${nulls}==${table_count}   add result to report   Test Passed : Column ${column} contains no values with null for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains null values for ${alias}\n
    run keyword if   ${nulls}!=${table_count}   set test variable   ${test_status}  FAIL
    END
    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values with null  ELSE    add result to report  Test Passed: Columns do not contain null values
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

Get expected column data from original table
     [Arguments]     ${COLUMNS_FOR_JOIN}    ${JOIN_CONDITION}
     ${expected_data_list}     Convert To String  ${expected_data_list}
     ${expected_data_list}     Remove String      ${expected_data_list}    ['And     (    '    )    ,       ]
     ${original_select_column_list}     Convert To String  ${original_select_column_list}
     ${original_select_column_list}     Remove String      ${original_select_column_list}   [    ]    '
     ${expected_data}     run get data from original table with cond     ${original_select_column_list}    ${JOIN_CONDITION}    ${expected_data_list}
     ${temp}     run keyword and return status     Should Be Empty       ${expected_data}
     run keyword if   ${temp}==True      add result to report   \n|No data available for columns ${original_select_column_list} for condition ${expected_data_list}\n
     run keyword if   ${temp}==True      Get primary keys data from original table    ${COLUMNS_FOR_JOIN}    ${JOIN_CONDITION}
     set test variable    ${expected_data}      ${expected_data}
     add result to report   \n|Expected data presented in original table ${expected_data} for columns ${original_select_column_list}\n

Validate that for all listed columns all whitespaces are removed per source
    [Arguments]   ${source_list}    ${column_list}
    Given I have access to Databricks database
    When I check that test requirements are implemented correctly
    Then I expect that all whitespaces removed    ${source_list}    ${column_list}

I expect that view is returning data for all the sources
    Validate that data count for all the sources for the view

Validate that data count for all the sources for the view
    Set system source for test
    run keyword if      ${Connection}==False     Connect to CDL VIEW Databricks
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Validate the data count for each source
    END
    run keyword if   '${data}'!='FAIL' and '${test_status}'=='FAIL'      add result to report      Test Failed: The view is not returning data as expected  ELSE    add result to report  Test Passed: The view is returning data as expected
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
    log test to report per step     ${jira_id_tag}
    #Teardown Test

Validate the data count for each source
    ${alias}   get from dictionary     ${TABLE_ALIAS}   ${i}
    set test variable    ${alias}
    run keyword if      ${Connection}==False     Connect to CDL VIEW Databricks
    ${count}=    return count of table with conditions  ${system}.${view}       src_sys_cd == '${alias}'
    run keyword if       ${count}==0      Data not available
    run keyword   add result to report   Expected = ${count}
    run Keyword If  '${count}'!='0'    add result to report   Test Passed: The ${system}.${view} view is returning data for the source ${alias} with data count ${count}\n       ELSE    add result to report    Test Failed:The ${system}.${view} view is not returning data for the source ${alias} as expected with data count ${count}\n
    run keyword if   ${count}==0   set test variable   ${test_status}    FAIL
    log to console    checked for source ${alias}

I expect that all whitespaces removed
   [Arguments]     ${source_list}    ${column_list}
   Validate that all whitespaces removed for source    ${source_list}    ${column_list}
   log     ${source_list}
   log    ${column_list}

Validate that all whitespaces removed for source
  [Arguments]     ${source_list}    ${column_list}
   FOR  ${source}  IN     @{source_list}
   log   ${source}
   ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
   run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${column_count}   Get Length   ${column_list}
    Run keyword if   ${column_count}>=45      Validate that whitespaces is removed for each column per source       ${alias}   ${column_list}
    Run keyword if   ${column_count}<45       Check list of columns    ${alias}      ${column_list}
   END
   log test to report per step    ${source_list}_${column_list}[0]

Check list of columns
    [Arguments]     ${alias}      ${column_list}
    add system information to report whitespaces   ${system}.${table}     ${alias}
    ${whitespace}=  return_count_of_whitespaces_for_table     ${system}.${table}       ${alias}       ${column_list}
    log to console    Alias ${alias} tested for columns ${column_list}
    run keyword if    ${whitespace}==True    add result to report   Test Passed : Columns: ${column_list} contains no whitespaces for ${alias}\n
    run keyword if    ${whitespace}==False   add result to report   Test Failed : At least one column in ${column_list}contains whitespaces for ${alias}\n
    run keyword if    ${whitespace}==False   set test variable      ${test_status}  FAIL
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

Validate that whitespaces is removed for each column per source
     [Arguments]     ${alias}      ${column_list}
     FOR  ${column}  IN     @{column_list}
     ${whitespace}    return_count_of_table_whitespaces_per_column     ${system}.${table}    ${alias}     ${column}
     log to console    Alias ${alias} tested for columns ${column}
     run keyword if    ${whitespace}==True    add result to report   Test Passed : Column ${column} contain no whitespaces for ${alias}\n
     run keyword if    ${whitespace}==False   add result to report   Test Failed : Column ${column} contain whitespaces for ${alias}\n
     run keyword if    ${whitespace}==False   set test variable      ${test_status}  FAIL
     run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
     END

Validate that for all listed columns contains all nulls values
    [Arguments]   ${source_list}    ${column_list}
    Given I have access to Databricks database
    When I check that test requirements are implemented correctly
    Then I expect that listed columns contains all nulls values   ${source_list}    ${column_list}

I expect that listed columns contains all nulls values
   [Arguments]     ${source_list}    ${column_list}
   Validate that colums contains nulls for sources    ${source_list}    ${column_list}
   log     ${source_list}
   log    ${column_list}

Validate that colums contains nulls for sources
    [Arguments]     ${source_list}    ${column_list}
    FOR  ${source}  IN     @{source_list}
    ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
    log    ${Connection}
    run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
    ${column_count}   Get Length   ${column_list}
    Run keyword if    ${column_count}>=45      Validate that each column contains nulls      ${alias}   ${column_list}
    Run keyword if    ${column_count}<45       Validate that columns contain nulls      ${alias}      ${column_list}
    END
    log test to report per step    ${source_list}_${column_list}[0]

Validate that columns contain nulls
    [Arguments]      ${alias}       ${column_list}
    add system information to report null     ${system}.${table}     ${alias}
    ${all_nulls}=     return_count_of_table_all_nulls    ${system}.${table}       ${alias}       ${column_list}
    log to console    Alias ${alias} tested for columns ${column_list}
    run keyword if    ${all_nulls}==True    add result to report   Test Passed : Columns: ${column_list} contains nulls values for ${alias}\n
    run keyword if    ${all_nulls}==False   add result to report   Test Failed : At least one column in ${column_list} contains other values instead of nulls for ${alias}\n
    run keyword if    ${all_nulls}==False   set test variable      ${test_status}  FAIL

Validate that each column contains nulls
    [Arguments]      ${alias}       ${column_list}
    FOR  ${column}  IN     @{column_list}
    ${all_nulls}    return_count_of_table_all_nulls_per_column    ${system}.${table}    ${alias}     ${column}
    log to console    Alias ${alias} tested for column ${column}
    run keyword if    ${all_nulls}==True    add result to report   Test Passed : Column: ${column} contains nulls values for ${alias}\n
    run keyword if    ${all_nulls}==False   add result to report   Test Failed : Column ${column} contains other values instead of nulls for  ${alias}\n
    run keyword if    ${all_nulls}==False   set test variable      ${test_status}  FAIL
    END

I expect that for a set of source list that specified columns contains a date in UTC with filter conditions
    [Arguments]         ${SOURCES_UTC}       ${COLUMNS_UTC}       ${COLUMNS_NULL_FILTER}
    Validate for a set of source list that specified columns contains a date in UTC with filter conditions    ${SOURCES_UTC}       ${COLUMNS_UTC}       ${COLUMNS_NULL_FILTER}

Validate for a set of source list that specified columns contains a date in UTC with filter conditions
    [Arguments]         ${SOURCES_UTC}       ${COLUMNS_UTC}       ${COLUMNS_NULL_FILTER}
    set test variable   @{columns_null_filter}    @{COLUMNS_NULL_FILTER}
    Set system source for test
    FOR  ${source}  IN  @{SOURCES_UTC}
        run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
        set test variable   ${source}
        log to console     Checking for ${source}
         ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
        FOR     ${column}   IN  @{COLUMNS_UTC}
            run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
            Check UTC format for specific alias for a set of source list with filter conditions      ${system}   ${table}   ${column}  ${alias}  ${columns_null_filter}
        END
    END
    run keyword if    '${test_status}'=='PASS' and ${ends_with_0000}==True    add result to report    Test Passed:\n All columns ${COLUMNS_UTC} are in UTC format.    ELSE IF    '${test_status}'=='FAIL'    add result to report    Test Failed: At least one column is not in UTC Format.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    log test to report per step     ${SOURCES_UTC}_${COLUMNS_UTC}[0]
   # Teardown Test

Validate that column contains null value for a set of source list as required with filter conditions
    [Arguments]         ${column}    ${alias}  ${columns_null_filter}
     FOR  ${i}   IN  @{columns_null_filter}
         ${status}    run keyword and return status      Should Be Equal As Strings    ${column}    ${i}
         run keyword if        ${status}== True    Validate that column for source in the list   ${column}    ${columns_null_filter}
         EXIT FOR LOOP IF      ${status}== True
     END

Validate that for all listed columns contains contains a date in UTC(with filter conditions)
    [Arguments]   ${source_list}    ${column_list}    ${column_list_with_filter}
    Given I have access to Databricks database
    When I check that test requirements are implemented correctly
    Then I expect that listed columns contains a date in UTC(with filter conditions)   ${source_list}    ${column_list}    ${column_list_with_filter}

I expect that listed columns contains a date in UTC(with filter conditions)
   [Arguments]     ${source_list}    ${column_list}    ${column_list_with_filter}
   Validate that colums contains a date in UTC(with filter conditions)    ${source_list}    ${column_list}    ${column_list_with_filter}
   log     ${source_list}
   log    ${column_list}

Validate that colums contains a date in UTC(with filter conditions)
    [Arguments]     ${source_list}    ${column_list}    ${column_list_with_filter}
    FOR  ${source}  IN     @{source_list}
        ${alias} =   get from dictionary     ${TABLE_ALIAS}    ${source}
        log    ${Connection}
        run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
        FOR     ${column}   IN  @{column_list}
            run Keyword If    ${Connection}==False and '${databrick_connection_Type}'=='Table'    Connect to CDL Databricks    ELSE IF    ${Connection}==False and '${databrick_connection_Type}'=='View'    Connect to CDL VIEW Databricks
            Check UTC format for specific alias for a set of source list with filter conditions      ${system}   ${table}   ${column}  ${alias}  ${column_list_with_filter}
        END
    END
    log test to report per step    ${source_list}_${column_list}[0]

Check UTC format for specific alias for a set of source list with filter conditions
    [Arguments]    ${system}   ${table}   ${column}  ${alias}  ${column_list_with_filter}
    ${utc_date}=   run keyword and continue on failure      return_date_as_utc_source   ${system}   ${table}   ${column}  ${alias}
    ${is_not_none}=   run keyword and return status         should not be equal as strings   '${utc_date}'   'None'
    ${ends_with_0000}=    run keyword and return status     should end with    ${utc_date}   +0000
    log    ${utc_date}
    run keyword if         ${is_not_none}==False      Validate that column contains null value for a set of source list as required with filter conditions    ${column}    ${alias}  ${column_list_with_filter}
    run keyword if         ${ends_with_0000}==True    set test variable   ${test_result}   True
    set test variable      ${ends_with_0000}
    log to console  Source ${alias} checked for column ${column}
    run keyword if    ${test_result}==False    set test variable   ${test_status}  FAIL
    run keyword if    ${test_result}==True and ${ends_with_0000}==True     add result to report   The column ${column} ends with +0000 offset   ELSE IF  ${test_result}==False and ${is_not_none}==True    add result to report   The column ${column} does note end with +0000 offset.
    run keyword if    ${test_result}==True and ${ends_with_0000}==True     add result to report   Test Passed: The ${column} column contains a record in UTC \n   ELSE IF  ${test_result}==False and ${is_not_none}==True   add result to report   Test Failed: The ${column} column contains a record that is not in UTC \n

Validate that for all listed columns contain hashtag(with filter conditions)
    [Arguments]   ${source_list}    ${column_list}
    Given I have access to Databricks database
    When I check that test requirements are implemented correctly
    Then I expect that the column values are a hashtag for source system or contains non hashtag filter values    ${source_list}    ${column_list}

I expect that the column values are a hashtag for source system or contains non hashtag filter values
    [Arguments]     ${source_list}    ${column_list}
    Validate that column values are a hashtag for source system or contains non hashtag filter values    ${source_list}    ${column_list}
    Log   ${source_list}
    Log   ${column_list}

Validate that column values are a hashtag for source system or contains non hashtag filter values
     [Arguments]     ${source_list}    ${columns}
      FOR  ${i}  IN  @{source_list}
          set test variable   ${i}
          run keyword if      ${Connection}==False     Connect to CDL Databricks
          ${alias}=   get from dictionary     ${TABLE_ALIAS}   ${i}
          Check column with filter condition    ${alias}    ${columns}
      END
	log test to report per step     ${source_list}_${columns}[0]
    #Teardown Test

Check column with filter condition
    [Arguments]    ${alias}    ${columns}
    Add information to hashtag columns report
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    Run keyword if     ${table_count}==0    Data not available
    Run keyword if     ${table_count}!=0       Check values per source for hashtag filter    ${alias}    ${columns}   ${table_count}

Check values per source for hashtag filter
    [Arguments]    ${alias}    ${columns}    ${table_count}
    FOR   ${column}   IN   @{columns}
    ${table_count_filter}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}' and ${column} not in ("#") And ${column} is not null And ${column} != ('')
    ${status}    Run keyword and return status     Should be equal    ${table_count}    ${table_count_filter}
    Run keyword if   ${status}==True    add result to report      Test Passed:Column ${column} contains all non hashtag values for ${alias}\n
    Run keyword if   ${status}==False   Validate hashtags values per column also persists    ${table_count}  ${table_count_filter}   ${column}    ${alias}
    END

Validate hashtags values per column also persists
    [Arguments]    ${table_count}  ${table_count_filter}   ${column}    ${alias}
    run keyword     add result to report    Expected = ${table_count}
    run keyword if  ${table_count_filter}==0    Add result to report      Test Passed:Column ${column} contains all hashtag values
    run keyword if  ${table_count_filter}!=0	add result to report      Test Passed:Column ${column} contains ${table_count_filter} non hashtag values for ${alias}\n
    ${hashtags}=    return count of table with conditions  ${system}.${table}        ${column} = '#' and src_sys_cd == '${alias}'
    run keyword if       ${hashtags}==0      Add result to report   Test Failed:Data not available
    run keyword if   ${hashtags}!=${table_count}    Get hashtag count from the table    ${table_count}  ${table_count_filter}   ${column}    ${alias}

Get hashtag count from the table
    [Arguments]   ${table_count}  ${table_count_filter}   ${column}    ${alias}
    ${count}    Evaluate   ${table_count}-${table_count_filter}
    Run keyword if  ${count}!=0      Add result to report   Test Passed:Column ${column} contain hashtag ${count} values as per filer requirement
