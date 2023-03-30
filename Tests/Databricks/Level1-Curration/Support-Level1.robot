# Support file for level 1 curration tests
# Should hold keywords for tests that are generic to all Level 1 currations

*** Settings ***
Resource    Tests/Databricks/Support-Databricks.robot
Library       Library/Utils.py

*** Variables ***
@{blacklist}=        ETL_ID  ETL_CREATEDATE  ETL_LASTUPDATEDATE  ETL_USERID
${test_status}       PASS

*** Keywords ***
# ---------------------------- First and Second level Keywords  ----------------------------
I check the target table for number of columns
    The target table is checked for number of columns

The target table is checked for number of columns
    log to console  PASS

I check the target view for number of columns
    The target view is checked for number of columns

The target view is checked for number of columns
    log to console  PASS

The user has access to Databricks database
    check the user has successful connection to Databricks

check the user has successful connection to Databricks
    log to console  CONNECTED TO DATABRICKS SUCCESSFULLY

I check that the Columns for Cell to Cell Data Validation
    Columns are Checked for Cell to Cell Data Validation

Columns are Checked for Cell to Cell Data Validation
    Log To Console  PASS

I check the primary keys for the curated table
    Primary Keys are checked for curated table

Primary Keys are checked for curated table
    log to console  PASS

I check the column datatypes
    The column datatypes are checked

The column datatypes are checked
    log to console  PASS

I expect that columns are of the correct datatype
    The columns are of the correct datatype

I check all specified columns for whitespace
     The columns are checked for whitespace

The columns are checked for whitespace
    log to console  PASS

I expect that the column does not contain any whitespace
    The column does not contain any whitespace

I expect that the columns do not contain any whitespace
    The columns do not contain any whitespace

I check that the column contains the correct value for SRC_SYS_CD
    The column is checked for the correct value for SRC_SYS_CD

I check that the Primary keys column does not contain null values
    The primary keys columns is checked for null values

I check that the columns contain not null values
    The columns are checked for not null values

The primary keys columns is checked for null values
    Log To Console  PASS

The columns are checked for not null values
    Log To Console  PASS

The column is checked for the correct value for SRC_SYS_CD
    log to console  PASS

I expect that the column contains the correct value for SRC_SYS_CD
    The column contains the correct value for SRC_SYS_CD

I expect that the primary keys column does not contain null values
    The Columns does not contain null values

I expect that the columns should contain not null values
    The Columns contain not null values

I check the DAI_UPDT_DTTM contains a date in UTC
    The DAI_UPDT_DTTM column is checked for a UTC date

I check the DAI_CRT_DTTM contains a date in UTC
    The DAI_CRT_DTTM column is checked for a UTC date

The DAI_UPDT_DTTM column is checked for a UTC date
    log to console  PASS

The DAI_CRT_DTTM column is checked for a UTC date
    log to console  PASS

I expect the DAI_UPDT_DTTM contains a date in UTC
    The DAI_UPDT_DTTM column contains a date in UTC format

The DAI_UPDT_DTTM column contains a date in UTC format
    set test variable   ${date_column}   DAI_UPDT_DTTM
    The column contains a date in UTC

I expect the DAI_CRT_DTTM contains a date in UTC
    The DAI_CRT_DTTM column contains a date in UTC

The DAI_CRT_DTTM column contains a date in UTC
    set test variable   ${date_column}   DAI_CRT_DTTM
    The column contains a date in UTC

I expect there is only unique primary keys
    The Uniqueness Of The Primary Keys Are As Expected For The Curated Table

I check that the curation logic has been applied
    The curation logic has been checked

The curation logic has been checked
    log to console  PASS

I expect that the target view has correct number of columns
    The target view contains correct number of columns

I check the row count of view
    The rowcount of view is checked

The rowcount of view is checked
    log to console  PASS

I expect that the row count of view should match the underlying table
    The rowcounts of view and underlying table are the same

I expect that the UTC Column contains required number of characters
      Validate that UTC Column contains required number of characters

I expect that the column values do not contain whitespaces for the source systems when columns are different
    Validate that columns in source systems do not contain whitespaces when columns are different

# ---------------------------- Third level Keywords  ----------------------------

UTC Columns should contain ${No. of Characters} characters
    FOR    ${column}   IN    @{utc_columns}
        ${count}=       Run Keyword  Check For Count Of Characters In Column  ${system}.${table}        ${column}       ${No. of Characters}
        run keyword and continue on failure  should be equal as numbers  ${count}    0
        run keyword if  ${count}==0     add result to report  Test Passed : Counts is Zero so All Records in ${column} Column contains 28 Characters in ${system}.${table}\n        ELSE        add result to report    Test Failed: ${count} Records in ${column} does not have 28 Characters.
    END
    Teardown Test

The columns contains a date in UTC
   set test variable   ${test_status}  PASS
    FOR     ${column}   IN  @{utc_columns}
        ${non_nulls}=   run keyword  count non nulls  ${system}   ${table}  ${column}
        run keyword if      ${non_nulls}==0  Fail test and log error    ${column}
        run keyword if      ${non_nulls}!=0  Check UTC format for column     ${system}   ${table}   ${column}
        END

    run keyword if   '${test_status}'=='PASS'   add result to report   Test Passed: \nAll columns ${utc_columns} are in UTC format.   ELSE   add result to report   Test Failed: At least one column in not in UTC Format.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    Teardown Test

Validate that specified columns contains a date in UTC
    [Arguments]         ${source}       ${UTC_COLUMNS}
    set test variable   ${test_status}  PASS
     ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
    FOR     ${column}   IN  @{UTC_COLUMNS}
        ${non_nulls}=   run keyword  count non nulls  ${system}   ${table}  ${column}
        run keyword if      ${non_nulls}==0      Fail test and log error    ${column}
        run keyword if      ${non_nulls}!=0      Check UTC format for specific alias      ${system}   ${table}   ${column}  ${alias}
    END

    run keyword if   '${test_status}'=='PASS'   add result to report   Test Passed: \nAll columns ${utc_columns} are in UTC format.   ELSE   add result to report   Test Failed: At least one column in not in UTC Format.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    Teardown Test

Fail test and log error
    [Arguments]            ${var}
    set test variable      ${test_status}  FAIL
    add result to report   Test Failed, ${var} not tested

Check UTC format for specific alias
    [Arguments]    ${system}   ${table}   ${column}  ${alias}
    ${utc_date}=   run keyword and continue on failure    return_date_as_utc_source   ${system}   ${table}   ${column}  ${alias}
    ${is_not_none}=   run keyword and ignore error   should not be equal as strings   '${utc_date}'   'None'
    ${ends_with_0000}=   run keyword and ignore error  should end with    ${utc_date}   +0000
    log    ${utc_date}
    ${test_result}=  run keyword and ignore error  should be true   '${is_not_none}[0]'=='PASS'   '${ends_with_0000}[0]'=='PASS'
    run keyword if   '${test_result}[0]'=='FAIL'   set test variable   ${test_status}  FAIL
    run keyword if   '${test_result}[0]'=='PASS'   add result to report   The column ${column} ends with +0000 offset   ELSE   add result to report   The column ${column} does note end with +0000 offset.
    run keyword if   '${test_result}[0]'=='PASS'   add result to report   Test Passed: The ${column} column contains a record in UTC\n   ELSE   add result to report   Test Failed: The ${column} column contains a record that is not in UTC\n

Check UTC format for column
    [Arguments]    ${system}   ${table}   ${column}
    ${utc_date}=   run keyword and continue on failure    return_date_as_utc    ${system}   ${table}   ${column}
    ${is_not_none}=   run keyword and ignore error   should not be equal as strings   '${utc_date}'   'None'
    ${ends_with_0000}=   run keyword and ignore error  should end with    ${utc_date}   +0000
    log    ${utc_date}
    ${test_result}=  run keyword and ignore error  should be true   '${is_not_none}[0]'=='PASS'   '${ends_with_0000}[0]'=='PASS'
    run keyword if   '${test_result}[0]'=='FAIL'   set test variable   ${test_status}  FAIL
    run keyword if   '${test_result}[0]'=='PASS'   add result to report   The column ${column} ends with +0000 offset   ELSE   add result to report   The column ${column} does note end with +0000 offset.
    run keyword if   '${test_result}[0]'=='PASS'   add result to report   Test Passed: The ${column} column contains a record in UTC\n   ELSE   add result to report   Test Failed: The ${column} column contains a record that is not in UTC\n

The columns contains a date in UTC for ${source}
    set test variable   ${test_status}  PASS
    run keyword if      '${source}' == 'elims'    set test variable   @{utc_columns}  @{utc_columns_elims}
    run keyword if      '${source}' == 'sap'    set test variable   @{utc_columns}  @{utc_columns_sap}
    FOR     ${column}   IN  @{utc_columns}
        ${non_nulls}=   run keyword  count non nulls  ${system}   ${table}  ${column}
        run keyword if      ${non_nulls}==0   column not tested ${column}
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
    run keyword    CONTINUE FOR LOOP

The columns do not contain any whitespace
    FOR     ${column}   IN  @{whitespace_columns}
        run keyword     Check ${column} for whitespace
    END
    Teardown Test

Check ${column} for whitespace
    set test variable   ${test_status}  PASS
    ${count}=   run count query with conditions     ${table}     ${system}  ${column} like ' %' or ${column} like '% '
    run keyword and continue on failure  should be equal as numbers  ${count}   0
    run keyword and continue on failure  add result to report   Expected: 0\nActual: ${count}

    run keyword if  ${count}==0    add result to report  Test Passed :The field ${column} contains no whitespace\n        ELSE        add result to report    Test Failed: Column ${column} contains whitespace\n
    run keyword if  ${count}!=0    set test variable   ${test_status}  FAIL


The columns are of the correct datatype

    set test variable   ${test_status}  PASS

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
    Teardown Test

The target table has ${number_of_columns} columns
    ${columns}=   get columns from table  ${system}.${table}
    ${column_count}=   get length  ${columns}
    run keyword and continue on failure  add result to report   Checking number of columns for ${system}.${table}
    run keyword and continue on failure  add result to report   Expected : ${number_of_columns}
    run keyword and continue on failure  add result to report   Actual : ${column_count}
    run keyword and continue on failure  should be equal as numbers     ${column_count}     ${number_of_columns}
    run keyword if  ${column_count}==${number_of_columns}     add result to report  Test Passed : The ${system}.${table} expected ${number_of_columns} columns and returned ${column_count} columns\n        ELSE        add result to report    Test Failed: The ${system}.${table} expected ${number_of_columns} columns and returned ${column_count} columns
    Teardown Test

The target view contains correct number of columns
    ${columns}=   get columns from view  ${system}.${table}
    ${act_column_count}=   get length  ${columns}
    run keyword and continue on failure  add result to report   Checking number of columns for ${system}.${table}
    run keyword and continue on failure  add result to report   Expected : ${column_count}
    run keyword and continue on failure  add result to report   Actual : ${act_column_count}
    run keyword and continue on failure  should be equal as numbers     ${act_column_count}     ${column_count}
    run keyword if  ${act_column_count}==${column_count}     add result to report  Test Passed : The ${system}.${table} expected ${column_count} columns and returned ${act_column_count} columns\n        ELSE        add result to report    Test Failed: The ${system}.${table} expected ${column_count} columns and returned ${act_column_count} columns
    Teardown Test

The uniqueness of the primary keys are as expected for the curated table
    set test variable   ${test_status}  PASS
    FOR    ${table}        IN       @{PRIMARY_KEYS}
        ${primary_keys}=    get from dictionary  ${PRIMARY_KEYS}    ${table}
        ${results_from_select_all}=        run keyword and continue on failure  return count of table      ${system}.${table}
        run keyword if       ${results_from_select_all}==0     Fail test and log error    ${table}
        run keyword if       ${results_from_select_all}!=0      Validate that table contains uniqueness of the primary keys      ${system}    ${table}    ${primary_keys}    ${results_from_select_all}
    END
    run keyword if   '${test_status}'=='PASS'   add result to report   Test Passed: \nUniqueness of the primary keys are as expected for the curated table   ELSE   add result to report   Test Failed: At least one primary keys are not as expected.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS
    Teardown Test

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

Validate that UTC Column contains required number of characters
    UTC Columns should contain ${UTC_COLUMN_CHARACTERS} characters

I check that all specified columns contain a date in UTC
    All specified columns are checked for a UTC date

I check that all specified columns contain a date in UTC for sap
    All specified columns are checked for a UTC date

I check that all specified columns contain a date in UTC for elims
    All specified columns are checked for a UTC date

All specified columns are checked for a UTC date
    log to console  PASS

I check that not all column values are null
    All specified columns are checked to ensure that not all column values are null

I check that the column values are a hashtag for the sap source systems
    All specified columns are checked for hashtag for the sap source systems

I check that the column values are a hashtag for maximo source system
    All specified columns are checked for hashtag for maximo source system

I check that the column values are a hashtag for elims source system
    All specified columns are checked for hashtag for elims source system

I check that the column values are a hashtag for btb_na source system
    All specified columns are checked for hashtag for btb_na source system

I check that the column values are a hashtag for p01 source system
    All specified columns are checked for hashtag for p01 source system

I check that the column values are a hashtag for btb_latam source system
    All specified columns are checked for hashtag for btb_latam source system

I check that the column values are a hashtag for taishan source system
    All specified columns are checked for hashtag for taishan source system

I check that the column values are a hashtag for deu source system
    All specified columns are checked for hashtag for deu source system

I check that the column values are a hashtag for bw2 source system
    All specified columns are checked for hashtag for bw2 source system

I check that the column values are a hashtag for gmd source system
    All specified columns are checked for hashtag for gmd source system

I check that the column values are a hashtag for mrs source system
    All specified columns are checked for hashtag for mrs source system

I check that the column values are a hashtag for hcs source system
    All specified columns are checked for hashtag for hcs source system

I check that the column values are a hashtag for geu source system
    All specified columns are checked for hashtag for geu source system

I check that the column values are a hashtag for bbn source system
    All specified columns are checked for hashtag for bbn source system

I check that the column values are a hashtag for bbl source system
    All specified columns are checked for hashtag for bbl source system

I check that the column values are a hashtag for bba source system
    All specified columns are checked for hashtag for bba source system

I check that the column values are trimmed for ${source}
    All specified columns are trimmed for ${source}

I check that the column values do not contain whitespaces for the sap source systems
    All specified columns are checked for whitespace for the sap source systems

I check that the column values do not contain whitespaces for the bba source systems
    All specified columns are checked for whitespace for the bba source systems

I check that the column values do not contain whitespaces for the gmd source systems
    All specified columns are checked for whitespace for the gmd source systems

I check that the column values do not contain whitespaces for the geu source systems
    All specified columns are checked for whitespace for the geu source systems

I check that the column values do not contain whitespaces for the deu source systems
    All specified columns are checked for whitespace for the deu source systems

I check that the column values do not contain whitespaces for the bw2 source systems
    All specified columns are checked for whitespace for the bw2 source systems

I check that the column values do not contain whitespaces for the bwi source systems
    All specified columns are checked for whitespace for the bwi source systems

I check that the column values do not contain whitespaces for the mrs source systems
    All specified columns are checked for whitespace for the mrs source systems

I check that the column values do not contain whitespaces for the hcs source systems
    All specified columns are checked for whitespace for the hcs source systems

I check that the column values do not contain whitespaces for the p01 source systems
    All specified columns are checked for whitespace for the p01 source systems

I check that the column values do not contain whitespaces for the btb_na source systems
    All specified columns are checked for whitespace for the btb_na source systems

I check that the column values do not contain whitespaces for the taishan source systems
    All specified columns are checked for whitespace for the taishan source systems

I check that the column values do not contain whitespaces for the btb_latam source systems
    All specified columns are checked for whitespace for the btb_latam source systems

I check that the column values do not contain whitespaces for the europe2 source systems
    All specified columns are checked for whitespace for the europe2 source systems

I expect that the column values do not contain whitespaces for the p01 source systems
    The columns values do not contain whitespaces for the p01 source system

I expect that the column values do not contain whitespaces for the bba source systems
    The columns values do not contain whitespaces for the bba source system

I expect that the column values do not contain whitespaces for the btb_na source systems
    The columns values do not contain whitespaces for the btb_na source system

I expect that the column values do not contain whitespaces for the btb_latam source systems
    The columns values do not contain whitespaces for the btb_latam source system

I expect that the column values do not contain whitespaces for the europe2 source systems
    The columns values do not contain whitespaces for the europe2 source system

I expect that the column values do not contain whitespaces for the taishan source systems
    The columns values do not contain whitespaces for the taishan source system

I expect that the column values do not contain whitespaces for the deu source systems
    The columns values do not contain whitespaces for the deu source system

I expect that the column values do not contain whitespaces for the bw2 source systems
    The columns values do not contain whitespaces for the bw2 source system

I expect that the column values do not contain whitespaces for the gmd source systems
    The columns values do not contain whitespaces for the gmd source system

I expect that the column values do not contain whitespaces for the geu source systems
    The columns values do not contain whitespaces for the geu source system

I expect that the column values do not contain whitespaces for the bwi source systems
    The columns values do not contain whitespaces for the bwi source system

I expect that the column values do not contain whitespaces for the mrs source systems
    The columns values do not contain whitespaces for the mrs source system

I expect that the column values do not contain whitespaces for the hcs source systems
    The columns values do not contain whitespaces for the hcs source system

I check that the column values do not contain whitespaces for the source systems
   All specified columns are checked for whitespace for the source systems

I check that the column values contain nulls for the sap source systems
    All specified columns are checked for null for the sap source systems

I check that the column values contain blanks for the sap source systems
    All specified columns are checked for blanks for the sap source systems

I check that the column values contain zeros for the sap source systems
    All specified columns are checked for zeros for the sap source systems

I check that the specified columns contain all null values
    All specified columns are checked for null values

#I check that the column values do not contain whitespaces for the ${source} source systems
#    All Specified Columns Are Checked For Whitespace For The ${source} Source Systems

I check that the specified columns contain all null values for elims source system
    All specified columns are checked for null values for elims source system

I check that the specified columns contain all null values for atlas source system
    All specified columns are checked for null values for atlas source system

I check that the specified columns contain all null values for hmd source system
    All specified columns are checked for null values for hmd source system

I check that the specified columns contain all null values for jet source system
    All specified columns are checked for null values for jet source system

I check that the specified columns contain all null values for jsw source system
    All specified columns are checked for null values for jsw source system

I check that the specified columns contain all null values for maximo source system
    All specified columns are checked for null values for maximo source system

I check that the specified columns contain all null values for sustain source system
    All specified columns are checked for null values for sustain source system

I check that the specified columns contain all null values for europe2 source system
    All specified columns are checked for null values for europe2 source system

I check that the specified columns contain all null values for panda source system
    All specified columns are checked for null values for panda source system

I check that the specified columns contain all null values for trackwise source system
    All specified columns are checked for null values for trackwise source system

I check that the specified columns contain all null values for p01 source system
    All specified columns are checked for null values for p01 source system

I check that the specified columns contain all null values for bba source system
    All specified columns are checked for null values for bba source system

I check that the specified columns contain all null values for btb_na source system
    All specified columns are checked for null values for btb_na source system

I check that the specified columns contain all null values for hcs source system
    All specified columns are checked for null values for hcs source system

I check that the specified columns contain all null values for mrs source system
    All specified columns are checked for null values for mrs source system

I check that the specified columns contain all null values for btb_latam source system
    All specified columns are checked for null values for btb_latam source system

I check that the specified columns contain all null values for taishan source system
    All specified columns are checked for null values for taishan source system

I check that the specified columns contain all null values for bw2 source system
    All specified columns are checked for null values for bw2 source system

I check that the specified columns contain all null values for deu source system
    All specified columns are checked for null values for deu source system

I check that the specified columns contain all null values for etq_instinct source system
    All specified columns are checked for null values for etq_instinct source system

I check that the specified columns contain all null values for gmd source system
    All specified columns are checked for null values for gmd source system

I check that the specified columns contain all null values for geu source system
    All specified columns are checked for null values for geu source system

I check that the specified columns contain all null values for bwi source system
    All specified columns are checked for null values for bwi source system

I check that the column values contain specified characters or they are nulls for ${source}
    All specified columns are checked for specified characters or nulls for ${source}

I check the location of the EDM
    The EDM location is checked

All specified columns are checked to ensure that not all column values are null
    log to console  PASS

All specified columns are checked for hashtag for the sap source systems
    log to console  PASS

All specified columns are checked for hashtag for maximo source system
    log to console  PASS

All specified columns are checked for hashtag for elims source system
    log to console  PASS

All specified columns are checked for hashtag for btb_na source system
    log to console  PASS

All specified columns are checked for hashtag for btb_latam source system
    log to console  PASS

All specified columns are checked for hashtag for p01 source system
    log to console  PASS

All specified columns are checked for hashtag for taishan source system
    log to console  PASS

All specified columns are checked for hashtag for deu source system
    log to console  PASS

All specified columns are checked for hashtag for bw2 source system
    log to console  PASS

All specified columns are checked for hashtag for bwi source system
    log to console  PASS

All specified columns are checked for hashtag for hcs source system
    log to console  PASS

All specified columns are checked for hashtag for mrs source system
    log to console  PASS

All specified columns are checked for hashtag for geu source system
    log to console  PASS

All specified columns are checked for hashtag for bbn source system
    log to console  PASS

All specified columns are checked for hashtag for gmd source system
    log to console  PASS

All specified columns are checked for hashtag for bba source system
    log to console  PASS

#All Specified Columns Are Checked For Whitespace For The ${source} Source Systems
#    Log To Console  PASS

All specified columns are trimmed for ${source}
    log to console  PASS

All specified columns are checked for whitespace for the sap source systems
    log to console  PASS

All specified columns are checked for whitespace for the bba source systems
    log to console  PASS

All specified columns are checked for whitespace for the gmd source systems
    log to console  PASS

All specified columns are checked for whitespace for the geu source systems
    log to console  PASS

All specified columns are checked for whitespace for the p01 source systems
    log to console  PASS

All specified columns are checked for whitespace for the taishan source systems
    log to console  PASS

All specified columns are checked for whitespace for the btb_latam source systems
    log to console  PASS

All specified columns are checked for whitespace for the europe2 source systems
    log to console  PASS

All specified columns are checked for whitespace for the btb_na source systems
    log to console  PASS

All specified columns are checked for whitespace for the deu source systems
    log to console  PASS

All specified columns are checked for whitespace for the bw2 source systems
    log to console  PASS

All specified columns are checked for whitespace for the bwi source systems
    log to console  PASS

All specified columns are checked for whitespace for the mrs source systems
    log to console  PASS

All specified columns are checked for whitespace for the hcs source systems
    log to console  PASS

All specified columns are checked for null for the sap source systems
    log to console  PASS

All specified columns are checked for whitespace for the source systems
    Log To Console  PASS

All specified columns are checked for blanks for the sap source systems
    log to console  PASS

All specified columns are checked for zeros for the sap source systems
    log to console  PASS

All specified columns are checked for null values
    log to console  PASS

All Specified Columns Are Checked For Whitespace For The bbn Source Systems
    Log To Console  PASS

All specified columns are checked for null values for elims source system
    log to console  PASS

All specified columns are checked for null values for maximo source system
    log to console  PASS

All specified columns are checked for null values for atlas source system
    log to console  PASS

All specified columns are checked for null values for hmd source system
    log to console  PASS

All specified columns are checked for null values for jet source system
    log to console  PASS

All specified columns are checked for null values for jsw source system
    log to console  PASS

All specified columns are checked for null values for sustain source system
    log to console  PASS

All specified columns are checked for null values for europe2 source system
    log to console  PASS

All specified columns are checked for null values for panda source system
    log to console  PASS

All specified columns are checked for null values for trackwise source system
    log to console  PASS

All specified columns are checked for null values for p01 source system
    log to console  PASS

All specified columns are checked for null values for bba source system
    log to console  PASS

All specified columns are checked for null values for btb_na source system
    log to console  PASS

All specified columns are checked for null values for hcs source system
    log to console  PASS

All specified columns are checked for null values for mrs source system
    log to console  PASS

All specified columns are checked for null values for btb_latam source system
    log to console  PASS

All specified columns are checked for null values for taishan source system
    log to console  PASS

All specified columns are checked for null values for bw2 source system
    log to console  PASS

All specified columns are checked for null values for deu source system
    log to console  PASS

All specified columns are checked for null values for etq_instinct source system
    log to console  PASS

All specified columns are checked for null values for gmd source system
    log to console  PASS

All specified columns are checked for null values for geu source system
    log to console  PASS

All specified columns are checked for null values for bwi source system
    log to console  PASS

All specified columns are checked for specified characters or nulls for ${source}
    log to console  PASS

The EDM location is checked
    log to console  PASS

I expect that the columns contain a date in UTC
    The columns contains a date in UTC

I expect that the columns contain a date in UTC for sap
    The columns contains a date in UTC for sap

I expect that the columns contain a date in UTC for elims
    The columns contains a date in UTC for elims

I expect that the columns contain all null values
    The columns contain all nulls

I expect that the columns contain all null values for elims source system
    The columns contain all nulls for elims source system

I expect that the columns contain all null values for atlas source system
    The columns contain all nulls for atlas source system

I expect that the columns contain all null values for hmd source system
   The columns contain all nulls for the hmd source system

The columns contain all nulls for atlas source system
    The columns contain all nulls for the atlas source system

I expect that the columns contain all null values for jet source system
    The columns contain all nulls for jet source system

I expect that the columns contain all null values for jsw source system
    The columns contain all nulls for jsw source system

The columns contain all nulls for jet source system
    The columns contain all nulls for the jet source system

The columns contain all nulls for jsw source system
    The columns contain all nulls for the jsw source system

I expect that the columns contain all null values for europe2 source system
    The columns contain all nulls for europe2 source system

The columns contain all nulls for europe2 source system
    The columns contain all nulls for the europe2 source system

I expect that the columns contain all null values for sustain source system
    The columns contain all nulls for sustain source system

The columns contain all nulls for sustain source system
    The columns contain all nulls for the sustain source system

I expect that the columns contain all null values for panda source system
    The columns contain all nulls for panda source system

I expect that the columns contain all null values for trackwise source system
    The columns contain all nulls for trackwise source system

The columns contain all nulls for panda source system
    The columns contain all nulls for the panda source system

The columns contain all nulls for trackwise source system
    The columns contain all nulls for the trackwise source system

The columns contain all nulls for elims source system
    The columns contain all nulls for the elims source system

I expect that the columns contain all null values for maximo source system
    The columns contain all nulls for maximo source system

The columns contain all nulls for maximo source system
    The columns contain all nulls for the maximo source system

I expect that the columns contain all null values for p01 source system
    The columns contain all nulls for the p01 source system

I expect that the columns contain all null values for btb_na source system
    The columns contain all nulls for the btb_na source system

I expect that the columns contain all null values for hcs source system
    The columns contain all nulls for the hcs source system

I expect that the columns contain all null values for mrs source system
    The columns contain all nulls for the mrs source system

I expect that the columns contain all null values for btb_latam source system
    The columns contain all nulls for the btb_latam source system

I expect that the columns contain all null values for bba source system
    The columns contain all nulls for the bba source system

I expect that the columns contain all null values for gmd source system
    The columns contain all nulls for the gmd source system

I expect that the columns contain all null values for geu source system
    The columns contain all nulls for the geu source system

I expect that the columns contain all null values for bwi source system
    The columns contain all nulls for the bwi source system

I expect that the columns contain all null values for taishan source system
    The columns contain all nulls for the taishan source system

I expect that the columns contain all null values for sap source system
    The columns contain all nulls for sap source system

I expect that the columns contain all null values for bw2 source system
    The columns contain all nulls for the bw2 source system

I expect that the columns contain all null values for deu source system
    The columns contain all nulls for the deu source system

I expect that the columns contain all null values for etq_instinct source system
    The columns contain all nulls for the etq_instinct source system

The columns contain all nulls
    set test variable   ${test_status}  PASS
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd <> 'elm'
    FOR     ${column}   IN  @{null_columns}
        ${nulls}=    return count of table with conditions  ${system}.${table}        ${column} IS NULL and src_sys_cd <> 'elm'
        run keyword     add result to report    Expected = ${table_count}
        run keyword if  ${nulls}==${table_count}     add result to report  Test Passed : Column ${column} contains all nulls\n        ELSE        add result to report    Test Failed: Column ${column} contains non null values\n
        run keyword if  ${nulls}!=${table_count}     set test variable   ${test_status}  FAIL
    END

    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non nulls  ELSE    add result to report  Test Passed: Columns ${null_columns} contain all nulls
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
    Teardown Test

The columns contain all nulls for the ${source} source system
        ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
        set test variable   ${test_status}  PASS
        ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
        run keyword if      '${source}' == 'atlas'    set test variable   @{null_columns}  @{null_columns_atlas}
        run keyword if      '${source}' == 'europe2'    set test variable   @{null_columns}  @{null_columns_europe2}
        run keyword if      '${source}' == 'sustain'    set test variable   @{null_columns}  @{null_columns_sustain}
        run keyword if      '${source}' == 'panda'    set test variable   @{null_columns}  @{null_columns_panda}
        run keyword if      '${source}' == 'elims'    set test variable   @{null_columns}  @{null_columns_elims}
        run keyword if      '${source}' == 'maximo'    set test variable   @{null_columns}  @{null_columns_maximo}
        run keyword if      '${source}' == 'p01'    set test variable   @{null_columns}  @{null_columns_p01}
        run keyword if      '${source}' == 'btb_na'    set test variable   @{null_columns}  @{null_columns_btb_na}
        run keyword if      '${source}' == 'btb_latam'    set test variable   @{null_columns}  @{null_columns_btb_latam}
        run keyword if      '${source}' == 'taishan'    set test variable   @{null_columns}  @{null_columns_taishan}
        run keyword if      '${source}' == 'sap'    set test variable   @{null_columns}  @{null_columns_sap}
        run keyword if      '${source}' == 'bw2'    set test variable   @{null_columns}  @{null_columns_bw2}
        run keyword if      '${source}' == 'deu'    set test variable   @{null_columns}  @{null_columns_deu}
        run keyword if      '${source}' == 'etq_instinct'    set test variable   @{null_columns}  @{null_columns_etq_instinct}
        run keyword if      '${source}' == 'gmd'    set test variable   @{null_columns}  @{null_columns_gmd}
        run keyword if      '${source}' == 'geu'    set test variable   @{null_columns}  @{null_columns_geu}
        run keyword if      '${source}' == 'bwi'    set test variable   @{null_columns}  @{null_columns_bwi}
        run keyword if      '${source}' == 'bba'    set test variable   @{null_columns}  @{null_columns_bba}
        run keyword if      '${source}' == 'mrs'    set test variable   @{null_columns}  @{null_columns_mrs}
        run keyword if      '${source}' == 'hcs'    set test variable   @{null_columns}  @{null_columns_hcs}
        run keyword if      '${source}' == 'trackwise'    set test variable   @{null_columns}  @{null_columns_trackwise}
        run keyword if      '${source}' == 'jet'    set test variable   @{null_columns}  @{null_columns_jet}
        run keyword if      '${source}' == 'jsw'    set test variable   @{null_columns}  @{null_columns_jsw}
        run keyword if      '${source}' == 'hmd'    set test variable   @{null_columns}  @{null_columns_hmd}

        FOR     ${column}   IN  @{null_columns}
            ${nulls}=    return count of table with conditions  ${system}.${table}        ${column} IS NULL and src_sys_cd == '${alias}'
            run keyword     add result to report    Expected = ${table_count}
            run keyword if  ${nulls}==${table_count}     add result to report  Test Passed : Column ${column} contains all nulls\n        ELSE        add result to report    Test Failed: Column ${column} contains non null values\n
            run keyword if  ${nulls}!=${table_count}     set test variable   ${test_status}  FAIL
        END

        run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non nulls  ELSE    add result to report  Test Passed: Columns contain all nulls
        run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
        Teardown Test

I expect that the curation logic has been applied
    The curation logic has been applied

The curation logic has been applied

    set test variable   ${test_status}  PASS

#     Set Filter to all if it isn't set.
    ${expected_filter}=    Get Variable Value    ${expected_system_result}   All
    ${src_sys_sql_dict}=      get source system sql    ${table}   ${expected_filter}

    FOR     ${src_sql}  IN  @{src_sys_sql_dict}
        ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${src_sql}
        ${query}=   get test query  ${table}    ${src_sql}   ${alias}
        ${actual_result}=   run sql query full  ${query}
        ${expected}=    get from dictionary     ${EXPECTED_RESULT}    ${src_sql}
        ${result}=  check expected in actual  ${expected}   ${actual_result}
        Log  ${expected}
        Log  ${actual_result}
        run keyword and continue on failure     should be true  ${result}
        run keyword if  ${result}  add result to report   Expected Result: ${expected}
        run keyword if  ${result}  add result to report      Test Passed: Expected Result found in Actual Result\n  ELSE    add result to report  Test Failed: Expected Result not found in Actual Result\n
        run keyword if  not ${result}  set test variable   ${test_status}  FAIL

        run keyword if  ${result}  set test variable   ${result}  False
    END

    run keyword if   '${test_status}'=='PASS'   add result to report   \nTest Passed: All values were as expected in target after curation.   ELSE   add result to report   \nTest Failed: At least one values was not as expected in target after curation.
    run keyword and continue on failure   Should Be Equal As Strings   ${test_status}   PASS

    Teardown Test

I expect that the EDM location is correct
    The location of the EDM is correct

The location of the EDM is correct
    ${actual_location}=   return location of table on dbfs   ${table}   l1
    log     ${actual_location}
    log     ${table_location}
    run keyword     add result to report   Actual Result: ${actual_location}
    run keyword     add result to report   Expected Result: ${table_location}\n
    run keyword and continue on failure     should be equal   ${actual_location}   ${table_location}
    run keyword if  "${actual_location}"=="${table_location}"   add result to report   Test Passed: Table location was equal to expected location \n  ELSE    add result to report  Test Failed: Table location was not equal to expected location\n
    Teardown Test

I expect that the column values are a hashtag for the sap source systems
    The column values are a hashtag for the sap source systems

The column values are a hashtag for the sap source systems
        set test variable   ${test_status}  PASS
        FOR     ${source}   IN   @{sap_sources}
            ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}

            ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'

            FOR     ${column}   IN  @{hashtag_columns_sap}
                ${hashtags}=    return count of table with conditions  ${system}.${table}        ${column} = '#' and src_sys_cd == '${alias}'
                run keyword     add result to report    Expected = ${table_count}
                run keyword if  ${hashtags}==${table_count}     add result to report  Test Passed : Column ${column} contains all hashtags for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains non hashtag values\n
                run keyword if  ${hashtags}!=${table_count}     set test variable   ${test_status}  FAIL
            END
        END

            run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non hashtags  ELSE    add result to report  Test Passed: Columns contain all hashtags
            run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

        Teardown Test

I expect that the column values are a hashtag for maximo source system
    The column values are a hashtag for maximo source system

I expect that the column values are a hashtag for elims source system
    The column values are a hashtag for elims source system

I expect that the column values are a hashtag for btb_na source system
    The column values are a hashtag for btb_na source system

I expect that the column values are a hashtag for btb_latam source system
    The column values are a hashtag for btb_latam source system

I expect that the column values are a hashtag for bba source system
    The column values are a hashtag for bba source system

I expect that the column values are a hashtag for p01 source system
    The column values are a hashtag for p01 source system

I expect that the column values are a hashtag for bw2 source system
    The column values are a hashtag for bw2 source system

I expect that the column values are a hashtag for taishan source system
    The column values are a hashtag for taishan source system

I expect that the column values are a hashtag for deu source system
    The column values are a hashtag for deu source system

I expect that the column values are a hashtag for mrs source system
    The column values are a hashtag for mrs source system

I expect that the column values are a hashtag for hcs source system
    The column values are a hashtag for hcs source system

I expect that the column values are a hashtag for geu source system
    The column values are a hashtag for geu source system

I expect that the column values are a hashtag for gmd source system
    The column values are a hashtag for gmd source system

The column values are a hashtag for ${source} source system
        set test variable   ${test_status}  PASS

        ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}

        ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
        run keyword if      '${source}' == 'maximo'    set test variable   @{hashtag_columns}  @{hashtag_columns_maximo}
        run keyword if      '${source}' == 'elims'    set test variable   @{hashtag_columns}  @{hashtag_columns_elims}
        run keyword if      '${source}' == 'btb_na'    set test variable   @{hashtag_columns}  @{hashtag_columns_btb_na}
        run keyword if      '${source}' == 'btb_latam'    set test variable   @{hashtag_columns}  @{hashtag_columns_btb_latam}
        run keyword if      '${source}' == 'taishan'    set test variable   @{hashtag_columns}  @{hashtag_columns_taishan}
        run keyword if      '${source}' == 'deu'    set test variable   @{hashtag_columns}  @{hashtag_columns_deu}
        run keyword if      '${source}' == 'p01'    set test variable   @{hashtag_columns}  @{hashtag_columns_p01}
        run keyword if      '${source}' == 'bw2'    set test variable   @{hashtag_columns}  @{hashtag_columns_bw2}
        run keyword if      '${source}' == 'gmd'    set test variable   @{hashtag_columns}  @{hashtag_columns_gmd}
        run keyword if      '${source}' == 'bba'    set test variable   @{hashtag_columns}  @{hashtag_columns_bba}
        run keyword if      '${source}' == 'mrs'    set test variable   @{hashtag_columns}  @{hashtag_columns_mrs}
        run keyword if      '${source}' == 'hcs'    set test variable   @{hashtag_columns}  @{hashtag_columns_hcs}
        run keyword if      '${source}' == 'geu'    set test variable   @{hashtag_columns}  @{hashtag_columns_geu}

        FOR     ${column}   IN  @{hashtag_columns}
            ${hashtags}=    return count of table with conditions  ${system}.${table}        ${column} = '#' and src_sys_cd == '${alias}'
            run keyword     add result to report    Expected = ${table_count}
            run keyword if  ${hashtags}==${table_count}     add result to report  Test Passed : Column ${column} contains all hashtags for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains non hashtag values\n
            run keyword if  ${hashtags}!=${table_count}     set test variable   ${test_status}  FAIL
        END

        run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains non hashtags  ELSE    add result to report  Test Passed: Columns contain all hashtags
        run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

        Teardown Test


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

        Teardown Test


I expect that the column values are trimmed for ${source}
    The column values are trimmed for ${source}

The column values are trimmed for ${source}
        set test variable   ${test_status}  PASS

        ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}

        ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'

        FOR     ${column}   IN  @{whitespace_columns}
            ${whitespace}=    return count of table with conditions  ${system}.${table}        (${column} not like ' %' or ${column} not like '% ' or ${column} is NULL) and src_sys_cd == '${alias}'
            run keyword     add result to report    Expected = ${table_count}
            run keyword if  ${whitespace}==${table_count}     add result to report  Test Passed : Column ${column} contains no values with whitespace for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains values with whitespace for ${alias}\n
            run keyword if  ${whitespace}!=${table_count}     set test variable   ${test_status}  FAIL
        END

        run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values with whitespace  ELSE    add result to report  Test Passed: These columns do not contain values with whitespace @{whitespace_columns}
        run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

        Teardown Test


#I expect that the column values do not contain whitespaces for the ${source} source systems
#    The columns values do not contain whitespaces for the ${source} source system

The columns values do not contain whitespaces for the ${source} source system
        ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}
        set test variable   ${test_status}  PASS
        ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
        run keyword if      '${source}' == 'atlas'    set test variable   @{whitespace_columns}  @{whitespace_columns_atlas}
        run keyword if      '${source}' == 'europe2'    set test variable   @{whitespace_columns}  @{whitespace_columns_europe2}
        run keyword if      '${source}' == 'sustain'    set test variable   @{whitespace_columns}  @{whitespace_columns_sustain}
        run keyword if      '${source}' == 'panda'    set test variable   @{whitespace_columns}  @{whitespace_columns_panda}
        run keyword if      '${source}' == 'elims'    set test variable   @{whitespace_columns}  @{whitespace_columns_elims}
        run keyword if      '${source}' == 'maximo'    set test variable   @{whitespace_columns}  @{whitespace_columns_maximo}
        run keyword if      '${source}' == 'p01'    set test variable   @{whitespace_columns}  @{whitespace_columns_p01}
        run keyword if      '${source}' == 'btb_na'    set test variable   @{whitespace_columns}  @{whitespace_columns_btb_na}
        run keyword if      '${source}' == 'btb_latam'    set test variable   @{whitespace_columns}  @{whitespace_columns_btb_latam}
        run keyword if      '${source}' == 'taishan'    set test variable   @{whitespace_columns}  @{whitespace_columns_tai}
        run keyword if      '${source}' == 'bw2'    set test variable   @{whitespace_columns}  @{whitespace_columns_bw2}
        run keyword if      '${source}' == 'deu'    set test variable   @{whitespace_columns}  @{whitespace_columns_deu}
        run keyword if      '${source}' == 'bba'    set test variable   @{whitespace_columns}  @{whitespace_columns_bba}
        run keyword if      '${source}' == 'gmd'    set test variable   @{whitespace_columns}  @{whitespace_columns_gmd}
        run keyword if      '${source}' == 'geu'    set test variable   @{whitespace_columns}  @{whitespace_columns_geu}
        run keyword if      '${source}' == 'bwi'    set test variable   @{whitespace_columns}  @{whitespace_columns_bwi}
        run keyword if      '${source}' == 'hcs'    set test variable   @{whitespace_columns}  @{whitespace_columns_hcs}
        run keyword if      '${source}' == 'mrs'    set test variable   @{whitespace_columns}  @{whitespace_columns_mrs}

        FOR   ${column}   IN   @{whitespace_columns}
           ${whitespace}=   return count of table with conditions   ${system}.${table}   (${column} not like ' %' or ${column} not like '% ' or ${column} is NULL) and src_sys_cd == '${alias}'
           run keyword   add result to report   Expected = ${table_count}
           run keyword if   ${whitespace}==${table_count}   add result to report   Test Passed : Column ${column} contains no values with whitespace for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains values with whitespace for ${alias}\n
           run keyword if   ${whitespace}!=${table_count}   set test variable   ${test_status}  FAIL
        END

        run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values with whitespace  ELSE    add result to report  Test Passed: Columns do not contain values with whitespace
        run keyword and continue on failure     should be equal as strings  ${test_status}  PASS
        Teardown Test

I expect that the column values contain nulls for the sap source systems
    The column values contain nulls for the sap source systems

The column values contain nulls for the sap source systems
        set test variable   ${test_status}  PASS
        FOR     ${source}   IN   @{sap_sources}
            ${alias}=   get from dictionary     ${TABLE_ALIAS}  ${source}

            ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'

            FOR     ${column}   IN  @{null_columns_sap}
                ${nulls}=    return count of table with conditions  ${system}.${table}        ${column} IS NULL and src_sys_cd == '${alias}'
                run keyword     add result to report    Expected = ${table_count}
                run keyword if  ${nulls}==${table_count}     add result to report  Test Passed : Column ${column} contains values with nulls for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains values without nulls for ${alias}\n
                run keyword if  ${nulls}!=${table_count}     set test variable   ${test_status}  FAIL
            END
        END
            run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values without nulls  ELSE    add result to report  Test Passed: Columns contain values with nulls
            run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

        Teardown Test

I expect that the column values contain blanks for the sap source systems
    The column values contain blanks for the sap source systems

The column values contain blanks for the sap source systems
        set test variable   ${test_status}  PASS
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

        Teardown Test

I expect that the column values contain zeros for the sap source systems
    The column values contain zeros for the sap source systems

The column values contain zeros for the sap source systems
        set test variable   ${test_status}  PASS
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

        Teardown Test

# ------------------- Used to ensure that date formatting has worked --------------------------------
I expect that not all column values are null
    The column values are not all null

The column values are not all null
        set test variable   ${test_status}  PASS

        ${table_count}=    return count of table  ${system}.${table}

        FOR     ${column}   IN  @{not_only_null_columns}
            ${nonulls}=    return count of table with conditions  ${system}.${table}        ${column} IS NULL
            run keyword     add result to report    Expected = ${table_count}
            run keyword if  ${nonulls}==${table_count}     set test variable   ${test_status}  FAIL
            run keyword if  ${nonulls}!=${table_count}     add result to report  Test Passed : Column ${column} contains values which are not all null\n        ELSE        add result to report    Test Failed: Column ${column} contains values which are all null\n
        END

        run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains only null values   ELSE    add result to report  Test Passed: Columns contain some values which are not null
        run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

        Teardown Test


# ----------------------------BEGIN: NULL and HASHTAG Test Specifically for MAINT_LOC maximo ----------------------------
I expect that the column values contain nulls and hashtags for maximo with multiple object names
    The column values contain nulls and hashtags for maximo with multiple object names

The column values contain nulls and hashtags for maximo with multiple object names
        set test variable   ${test_status}  PASS

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

        Teardown Test

# ----------------------------BEGIN: NULL and Character Test Specifically for CAPA_EXTN_RQST eti ----------------------
I expect that the column values contain specified characters or they are nulls for ${source}
    The column values contain specified characters or they are nulls for ${source}

The column values contain specified characters or they are nulls for ${source}
    set test variable   ${test_status}  PASS

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
    Teardown Test

#----------------------------Rowcount test for l1 views--------------------------------------------------------------
The rowcounts of view and underlying table are the same
        set test variable   ${test_status}  PASS

        ${table_count}=    return count of table  ${underlying_l1_db_for_vw}.${underlying_table}
        ${view_count}=    return count of table  ${system}.${table}
        run keyword     add result to report    Expected = ${table_count}
        run keyword if  ${table_count}!=${view_count}     set test variable   ${test_status}  FAIL
        run keyword if  ${table_count}==${view_count}     add result to report  Test Passed : View ${system}.${table} contains the same number of rows as table ${underlying_l1_db_for_vw}.${underlying_table}

        run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: View ${system}.${table} does not contain the same number of rows as table ${underlying_l1_db_for_vw}.${underlying_table}
        run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

        Teardown Test

I expect that the target table contains required number of columns
    Validate that the target table contains required number of columns

Validate that the target table contains required number of columns
    log  Required number of columns is ${COLUMN_COUNT}
    The target table has ${COLUMN_COUNT} columns

UTC Columns should contain ${No. of Characters} characters
    FOR    ${column}   IN    @{utc_columns}
        ${count}=       Run Keyword  Check For Count Of Characters In Column  ${system}.${table}        ${column}       ${No. of Characters}
        run keyword and continue on failure  should be equal as numbers  ${count}    0
        run keyword if  ${count}==0     add result to report  Test Passed : Counts is Zero so All Records in ${column} Column contains 28 Characters in ${system}.${table}\n        ELSE        add result to report    Test Failed: ${count} Records in ${column} does not have 28 Characters.
    END
    Teardown Test

Validate that columns in source systems do not contain whitespaces when columns are different
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{WHITESPACE_SOURCES}
    Set system source for test
    FOR  ${i}  IN  @{source_system}
        set test variable   ${i}
        Validate that columns in source systems do not contain whitespaces when columns are different for multiple sources
    END
    Teardown Test

Set system source for test
    ${temp}       Run keyword and return status      set test variable   @{source_system}    @{source_system}
    run keyword if  ${temp}== False                  set test variable   @{source_system}    @{sap_sources}

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
    run keyword if      '${i}' == 'jsw'          set test variable   @{whitespace_columns}  @{whitespace_columns_jsw}
    run keyword if      '${i}' == 'jet'          set test variable   @{whitespace_columns}  @{whitespace_columns_jet}
    run keyword if      '${i}' == 'hmd'          set test variable   @{whitespace_columns}  @{whitespace_columns_hmd}

    add system information to report whitespaces  ${system}.${table}     ${i}
    FOR     ${column}   IN  @{whitespace_columns}
    add columns names to report  ${column}
    END
    ${column count}   Get Length   ${whitespace_columns}
    Run keyword if   ${column count}>15       Connect to databrick and Validate whitespaces for source and add result to report
    Run keyword if   ${column count}<15       Validate whitespaces for source and add result to report

Connect to databrick and Validate whitespaces for source and add result to report

    Validate whitespaces for source and add result to report
    Log to console    Columns are checked for ${i}
    Disconnect from Databricks

Validate whitespaces for source and add result to report
    run keyword if      ${Connection}==False     Connect to CDL Databricks
    Add information to whitespace columns report
    ${table_count}=    return count of table with conditions  ${system}.${table}       src_sys_cd == '${alias}'
    FOR   ${column}   IN   @{whitespace_columns}
    ${whitespace}=   return count of table with conditions   ${system}.${table}   (${column} not like ' %' or ${column} not like '% ' or ${column} is NULL) and src_sys_cd == '${alias}'
    run keyword   add result to report   Expected = ${table_count}
    run keyword if   ${whitespace}==${table_count}   add result to report   Test Passed : Column ${column} contains no values with whitespace for ${alias}\n        ELSE        add result to report    Test Failed: Column ${column} contains values with whitespace for ${alias}\n
    run keyword if   ${whitespace}!=${table_count}   set test variable   ${test_status}  FAIL
    END
    run keyword if  "${test_status}"=="FAIL"  add result to report      Test Failed: At least 1 column contains values with whitespace  ELSE    add result to report  Test Passed: Columns do not contain values with whitespace
    run keyword and continue on failure     should be equal as strings  ${test_status}  PASS

Add information to whitespace columns report
    add system information to report whitespaces  ${system}.${table}     ${i}
    FOR     ${column}   IN  @{whitespace_columns}
    add columns names to report  ${column}
    END

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
    Teardown Test

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
